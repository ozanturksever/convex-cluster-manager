package health

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/ozanturksever/convex-cluster-manager/internal/natsutil"
)

// Config contains configuration for the health checker.
type Config struct {
	ClusterID string
	NodeID    string
	NATSURLs  []string
}

// Validate validates the health checker configuration.
func (c Config) Validate() error {
	if c.ClusterID == "" {
		return fmt.Errorf("cluster ID is required")
	}
	if c.NodeID == "" {
		return fmt.Errorf("node ID is required")
	}
	if len(c.NATSURLs) == 0 {
		return fmt.Errorf("at least one NATS URL is required")
	}
	return nil
}

// HealthResponse is the payload returned by health checks.
type HealthResponse struct {
	NodeID         string `json:"nodeId"`
	Role           string `json:"role"`
	Leader         string `json:"leader"`
	BackendStatus  string `json:"backendStatus"`
	ReplicationLag int64  `json:"replicationLag"`
	WALSequence    uint64 `json:"walSequence"`
	UptimeMs       int64  `json:"uptimeMs"`
	Timestamp      int64  `json:"timestamp"`
}

// Checker exposes node health information over NATS.
type Checker struct {
	cfg     Config
	logger  *slog.Logger
	subject string

	mu             sync.RWMutex
	role           string
	leader         string
	backendStatus  string
	replicationLag int64
	walSequence    uint64
	startedAt      time.Time

	nc     *nats.Conn
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewChecker creates a new health checker with the given configuration.
func NewChecker(cfg Config) (*Checker, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	c := &Checker{
		cfg:     cfg,
		logger:  slog.Default().With("component", "health", "cluster", cfg.ClusterID, "node", cfg.NodeID),
		subject: fmt.Sprintf("convex.health.%s", cfg.NodeID),
	}

	return c, nil
}

// Start connects to NATS and begins serving health check requests.
func (c *Checker) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.nc != nil {
		// Already started.
		return nil
	}

	if len(c.cfg.NATSURLs) == 0 {
		return fmt.Errorf("no NATS URLs configured")
	}

	// Connect to NATS with automatic failover and cluster discovery
	nc, err := natsutil.Connect(natsutil.ConnectOptions{
		URLs:   c.cfg.NATSURLs,
		Logger: c.logger,
	})
	if err != nil {
		return fmt.Errorf("connect NATS: %w", err)
	}

	// Subscribe for request-reply health checks.
	_, err = nc.Subscribe(c.subject, c.handleRequest)
	if err != nil {
		nc.Close()
		return fmt.Errorf("subscribe health subject: %w", err)
	}

	c.nc = nc
	c.startedAt = time.Now()
	c.ctx, c.cancel = context.WithCancel(context.Background())

	// Background ticker to keep the checker alive and allow future periodic
	// publishing or metrics updates if needed.
	c.wg.Add(1)
	go c.run(c.ctx)

	c.logger.Info("health checker started", "subject", c.subject)
	return nil
}

// Stop stops the health checker and closes the NATS connection.
func (c *Checker) Stop() {
	c.mu.Lock()
	cancel := c.cancel
	nc := c.nc
	c.cancel = nil
	c.nc = nil
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if nc != nil {
		nc.Close()
	}

	c.wg.Wait()
	c.logger.Info("health checker stopped")
}

// SetRole updates the current role reported by the checker.
func (c *Checker) SetRole(role string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.role = role
}

// SetLeader updates the current leader reported by the checker.
func (c *Checker) SetLeader(leader string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leader = leader
}

// SetBackendStatus updates the backend service status reported by the checker.
func (c *Checker) SetBackendStatus(status string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.backendStatus = status
}

// SetReplicationLag updates the replication lag (in arbitrary units, e.g. seconds or frames).
func (c *Checker) SetReplicationLag(lag uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.replicationLag = int64(lag)
}

// SetWALSequence updates the current WAL sequence number.
func (c *Checker) SetWALSequence(seq uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.walSequence = seq
}

// QueryNode performs a health check request against another node.
func (c *Checker) QueryNode(ctx context.Context, nodeID string, timeout time.Duration) (HealthResponse, error) {
	if nodeID == "" {
		return HealthResponse{}, fmt.Errorf("nodeID is required")
	}

	subject := fmt.Sprintf("convex.health.%s", nodeID)

	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	c.mu.RLock()
	nc := c.nc
	urls := append([]string(nil), c.cfg.NATSURLs...)
	c.mu.RUnlock()

	var (
		msg *nats.Msg
		err error
	)

	if nc != nil && nc.IsConnected() {
		msg, err = nc.RequestWithContext(reqCtx, subject, nil)
	} else {
		if len(urls) == 0 {
			return HealthResponse{}, fmt.Errorf("no NATS URLs configured")
		}
		// Connect to NATS with automatic failover and cluster discovery
		tmp, errConn := natsutil.Connect(natsutil.ConnectOptions{
			URLs: urls,
		})
		if errConn != nil {
			return HealthResponse{}, fmt.Errorf("connect NATS: %w", errConn)
		}
		defer tmp.Close()

		msg, err = tmp.RequestWithContext(reqCtx, subject, nil)
	}

	if err != nil {
		return HealthResponse{}, err
	}

	var resp HealthResponse
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return HealthResponse{}, err
	}
	return resp, nil
}

// handleRequest handles incoming NATS requests for this node's health.
func (c *Checker) handleRequest(msg *nats.Msg) {
	// Only respond to proper request-reply messages.
	if msg.Reply == "" {
		return
	}

	resp := c.buildResponse()
	data, err := json.Marshal(resp)
	if err != nil {
		c.logger.Error("failed to marshal health response", "error", err)
		return
	}

	if err := msg.Respond(data); err != nil {
		c.logger.Error("failed to respond to health request", "error", err)
	}
}

// buildResponse builds a HealthResponse snapshot from the current state.
func (c *Checker) buildResponse() HealthResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()

	now := time.Now()
	var uptimeMs int64
	if !c.startedAt.IsZero() {
		uptimeMs = now.Sub(c.startedAt).Milliseconds()
	}

	return HealthResponse{
		NodeID:         c.cfg.NodeID,
		Role:           c.role,
		Leader:         c.leader,
		BackendStatus:  c.backendStatus,
		ReplicationLag: c.replicationLag,
		WALSequence:    c.walSequence,
		UptimeMs:       uptimeMs,
		Timestamp:      now.UnixMilli(),
	}
}

// run executes a simple ticker loop so the checker can be extended later
// to perform periodic publishing or metrics updates. Currently it only
// waits for shutdown.
func (c *Checker) run(ctx context.Context) {
	defer c.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// No-op for now; health responses are served via request-reply.
		}
	}
}
