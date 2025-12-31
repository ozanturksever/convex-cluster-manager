package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"github.com/ozanturksever/convex-cluster-manager/internal/natsutil"
)

// StatusProvider is an interface for providing cluster status information.
// The daemon implements this to report its current state.
type StatusProvider interface {
	// GetStatus returns the current cluster node status.
	GetStatus() NodeStatus
}

// NodeStatus represents the status of a cluster node.
type NodeStatus struct {
	NodeID         string `json:"nodeId"`
	ClusterID      string `json:"clusterId"`
	Role           string `json:"role"`           // "PRIMARY" or "PASSIVE"
	Leader         string `json:"leader"`         // Current leader node ID
	BackendStatus  string `json:"backendStatus"`  // "running", "stopped", "failed"
	ReplicationLag int64  `json:"replicationLag"` // Milliseconds behind leader
	WALSequence    uint64 `json:"walSequence"`    // Current WAL position
	UptimeMs       int64  `json:"uptimeMs"`       // Service uptime
	Timestamp      int64  `json:"timestamp"`      // Unix timestamp in milliseconds
}

// ServiceConfig contains configuration for the cluster service.
type ServiceConfig struct {
	ClusterID       string
	NodeID          string
	NATSURLs        []string
	NATSCredentials string
	Version         string // Service version (optional, defaults to "1.0.0")
}

// SubjectBase returns the auth-compatible subject base for this service.
// Format: convex.<cluster_id>.svc.<node_id>
// This allows NATS auth rules like: convex.mycluster.svc.>
func (c ServiceConfig) SubjectBase() string {
	return fmt.Sprintf("convex.%s.svc.%s", c.ClusterID, c.NodeID)
}

// ServiceName returns the service name for registration.
// Format: convex-<cluster_id>-<node_id>
func (c ServiceConfig) ServiceName() string {
	return fmt.Sprintf("convex-%s-%s", c.ClusterID, c.NodeID)
}

// Service wraps a nats.micro service for cluster health and discovery.
// It provides:
// - Built-in PING, INFO, STATS endpoints (automatic from nats.micro)
// - Custom STATUS endpoint for cluster-specific health data
// - Service discovery via nats.micro service registry
type Service struct {
	cfg    ServiceConfig
	logger *slog.Logger

	nc  *nats.Conn
	svc micro.Service

	mu             sync.RWMutex
	statusProvider StatusProvider
	startedAt      time.Time
}

// NewService creates a new cluster service.
func NewService(cfg ServiceConfig) (*Service, error) {
	if cfg.ClusterID == "" {
		return nil, fmt.Errorf("clusterID is required")
	}
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("nodeID is required")
	}
	if len(cfg.NATSURLs) == 0 {
		return nil, fmt.Errorf("at least one NATS URL is required")
	}
	if cfg.Version == "" {
		cfg.Version = "1.0.0"
	}

	return &Service{
		cfg:    cfg,
		logger: slog.Default().With("component", "service", "node", cfg.NodeID, "cluster", cfg.ClusterID),
	}, nil
}

// SetStatusProvider sets the status provider for the service.
// This should be called before Start().
func (s *Service) SetStatusProvider(provider StatusProvider) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statusProvider = provider
}

// Start connects to NATS and registers the micro service.
func (s *Service) Start(ctx context.Context) error {
	// Connect to NATS with automatic failover and cluster discovery
	nc, err := natsutil.Connect(natsutil.ConnectOptions{
		URLs:        s.cfg.NATSURLs,
		Credentials: s.cfg.NATSCredentials,
		Logger:      s.logger,
	})
	if err != nil {
		return fmt.Errorf("connect to NATS: %w", err)
	}
	s.nc = nc

	// Create micro service configuration
	// Subject pattern: convex.<cluster_id>.svc.<node_id>
	svcConfig := micro.Config{
		Name:        s.cfg.ServiceName(),
		Version:     s.cfg.Version,
		Description: fmt.Sprintf("Convex cluster node %s in cluster %s", s.cfg.NodeID, s.cfg.ClusterID),
		Metadata: map[string]string{
			"cluster_id": s.cfg.ClusterID,
			"node_id":    s.cfg.NodeID,
		},
	}

	// Create the micro service
	svc, err := micro.AddService(nc, svcConfig)
	if err != nil {
		nc.Close()
		return fmt.Errorf("create micro service: %w", err)
	}
	s.svc = svc

	// Add custom status endpoint
	// Subject: convex.<cluster_id>.svc.<node_id>.status
	statusSubject := s.cfg.SubjectBase() + ".status"
	err = svc.AddEndpoint("status", micro.HandlerFunc(s.handleStatus), micro.WithEndpointSubject(statusSubject))
	if err != nil {
		_ = svc.Stop()
		nc.Close()
		return fmt.Errorf("add status endpoint: %w", err)
	}

	s.mu.Lock()
	s.startedAt = time.Now()
	s.mu.Unlock()

	s.logger.Info("service started",
		"name", svcConfig.Name,
		"subject_base", s.cfg.SubjectBase(),
		"status_subject", statusSubject,
	)

	return nil
}

// Stop stops the micro service and closes the NATS connection.
func (s *Service) Stop() error {
	var errs []error

	if s.svc != nil {
		if err := s.svc.Stop(); err != nil {
			errs = append(errs, fmt.Errorf("stop micro service: %w", err))
		}
		s.svc = nil
	}

	if s.nc != nil {
		s.nc.Close()
		s.nc = nil
	}

	s.logger.Info("service stopped")

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// handleStatus handles requests to the status endpoint.
func (s *Service) handleStatus(req micro.Request) {
	s.mu.RLock()
	provider := s.statusProvider
	startedAt := s.startedAt
	s.mu.RUnlock()

	var status NodeStatus
	if provider != nil {
		status = provider.GetStatus()
	} else {
		// Default status when no provider is set
		status = NodeStatus{
			NodeID:    s.cfg.NodeID,
			ClusterID: s.cfg.ClusterID,
			Role:      "UNKNOWN",
		}
	}

	// Always update timestamp and uptime
	now := time.Now()
	status.Timestamp = now.UnixMilli()
	if !startedAt.IsZero() {
		status.UptimeMs = now.Sub(startedAt).Milliseconds()
	}

	data, err := json.Marshal(status)
	if err != nil {
		s.logger.Error("failed to marshal status", "error", err)
		_ = req.Error("500", "internal error", nil)
		return
	}

	_ = req.Respond(data)
}

// Info returns the service info.
func (s *Service) Info() *micro.Info {
	if s.svc == nil {
		return nil
	}
	info := s.svc.Info()
	return &info
}

// Stats returns the service statistics.
func (s *Service) Stats() *micro.Stats {
	if s.svc == nil {
		return nil
	}
	stats := s.svc.Stats()
	return &stats
}

// QueryNodeStatus queries the status of another node in the cluster.
func (s *Service) QueryNodeStatus(ctx context.Context, nodeID string, timeout time.Duration) (*NodeStatus, error) {
	if s.nc == nil {
		return nil, fmt.Errorf("service not started")
	}

	// Build subject for target node
	subject := fmt.Sprintf("convex.%s.svc.%s.status", s.cfg.ClusterID, nodeID)

	// Send request with timeout
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	msg, err := s.nc.RequestWithContext(reqCtx, subject, nil)
	if err != nil {
		return nil, fmt.Errorf("query node %s: %w", nodeID, err)
	}

	var status NodeStatus
	if err := json.Unmarshal(msg.Data, &status); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	return &status, nil
}

// DiscoverNodes discovers all nodes in the cluster using micro service discovery.
// Returns a map of node ID to service info.
func (s *Service) DiscoverNodes(ctx context.Context) (map[string]*micro.Info, error) {
	if s.nc == nil {
		return nil, fmt.Errorf("service not started")
	}

	// Use micro.ControlSubject to query for services
	// Service name pattern: convex-<cluster_id>-*
	servicePrefix := fmt.Sprintf("convex-%s-", s.cfg.ClusterID)

	// Query for all services matching our cluster pattern
	// This uses the $SRV.INFO subject pattern
	infoSubject := fmt.Sprintf("$SRV.INFO.%s", servicePrefix)

	nodes := make(map[string]*micro.Info)

	// Subscribe to collect responses
	inbox := s.nc.NewRespInbox()
	sub, err := s.nc.SubscribeSync(inbox)
	if err != nil {
		return nil, fmt.Errorf("subscribe to inbox: %w", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	// Publish discovery request
	if err := s.nc.PublishRequest(infoSubject, inbox, nil); err != nil {
		return nil, fmt.Errorf("publish discovery request: %w", err)
	}

	// Collect responses with timeout
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		msg, err := sub.NextMsg(500 * time.Millisecond)
		if err != nil {
			if err == nats.ErrTimeout {
				continue
			}
			break
		}

		var info micro.Info
		if err := json.Unmarshal(msg.Data, &info); err != nil {
			s.logger.Warn("failed to unmarshal service info", "error", err)
			continue
		}

		// Extract node ID from metadata
		if nodeID, ok := info.Metadata["node_id"]; ok {
			nodes[nodeID] = &info
		}
	}

	return nodes, nil
}

// Running returns true if the service is running.
func (s *Service) Running() bool {
	return s.svc != nil && s.nc != nil && s.nc.IsConnected()
}
