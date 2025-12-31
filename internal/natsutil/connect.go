package natsutil

import (
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// ConnectOptions configures the NATS connection.
type ConnectOptions struct {
	URLs        []string
	Credentials string
	Logger      *slog.Logger
}

// Connect creates a NATS connection with automatic failover and cluster discovery.
// It configures:
// - Infinite reconnection attempts
// - Automatic server discovery (learns about new nodes from cluster gossip)
// - Reconnection to discovered servers when connection is lost
// - Logging of connection events for debugging
func Connect(opts ConnectOptions) (*nats.Conn, error) {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	natsOpts := []nats.Option{
		// Infinite reconnection attempts
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2 * time.Second),

		// Enable automatic discovery of new servers in the cluster.
		// When connected to a NATS cluster, the client will receive
		// updates about new servers joining or leaving.
		nats.DontRandomize(), // Keep initial order for predictable first connection

		// Handler called when new servers are discovered via cluster gossip.
		nats.DiscoveredServersHandler(func(nc *nats.Conn) {
			servers := nc.DiscoveredServers()
			known := nc.Servers()
			logger.Debug("NATS cluster topology updated",
				"discovered", servers,
				"all_known", known,
			)
		}),

		// Handler called on successful reconnection.
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Info("NATS reconnected",
				"url", nc.ConnectedUrl(),
				"server_id", nc.ConnectedServerId(),
			)
		}),

		// Handler called when connection is lost.
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				logger.Warn("NATS disconnected", "error", err)
			} else {
				logger.Debug("NATS disconnected gracefully")
			}
		}),

		// Handler called when connection is permanently closed.
		nats.ClosedHandler(func(nc *nats.Conn) {
			logger.Info("NATS connection closed")
		}),

		// Handler for async errors (e.g., slow consumer).
		nats.ErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
			if sub != nil {
				logger.Error("NATS async error",
					"subject", sub.Subject,
					"error", err,
				)
			} else {
				logger.Error("NATS async error", "error", err)
			}
		}),
	}

	// Add credentials if provided.
	if opts.Credentials != "" {
		natsOpts = append(natsOpts, nats.UserCredentials(opts.Credentials))
	}

	// Connect with all configured URLs for automatic failover.
	// The client will try each URL in order, and if disconnected,
	// will try all known URLs (including discovered ones).
	nc, err := nats.Connect(strings.Join(opts.URLs, ","), natsOpts...)
	if err != nil {
		return nil, err
	}

	logger.Info("NATS connected",
		"url", nc.ConnectedUrl(),
		"server_id", nc.ConnectedServerId(),
		"cluster_name", nc.ConnectedClusterName(),
	)

	return nc, nil
}
