package agent

import (
	"context"
	"log"
	"time"

	logfluxv1 "github.com/TohidTonekaboni/LogFlux/proto/logflux/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client is the SDK applications embed to stream logs to the ingestion server.
type Client struct {
	cfg    Config
	conn   *grpc.ClientConn
	client logfluxv1.LogIngestClient
	queue  chan *logfluxv1.LogEntry
}

func New(cfg Config) (*Client, error) {
	cfg = cfg.withDefaults()

	conn, err := grpc.NewClient(cfg.ServerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &Client{
		cfg:    cfg,
		conn:   conn,
		client: logfluxv1.NewLogIngestClient(conn),
		queue:  make(chan *logfluxv1.LogEntry, cfg.QueueCapacity),
	}, nil
}

// Enqueue is non-blocking. If the queue is full, it drops the oldest queued
// entry to make room — recent logs matter more than old ones under
// sustained overload (ARCHITECTURE.md §3.1).
func (c *Client) Enqueue(entry *logfluxv1.LogEntry) {
	for k, v := range c.cfg.StaticLabels {
		if entry.Fields == nil {
			entry.Fields = make(map[string]string)
		}
		if _, exists := entry.Fields[k]; !exists {
			entry.Fields[k] = v
		}
	}

	select {
	case c.queue <- entry:
		return
	default:
	}

	select {
	case <-c.queue:
	default:
	}
	select {
	case c.queue <- entry:
	default:
	}
}

// Run streams queued entries until ctx is canceled, reconnecting with
// backoff if the stream fails. Entries batched but not yet acknowledged by
// a failed stream are retried on the next connection attempt.
func (c *Client) Run(ctx context.Context) error {
	backoff := time.Second
	const maxBackoff = 30 * time.Second
	pending := make([]*logfluxv1.LogEntry, 0, c.cfg.BatchSize)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		var err error
		pending, err = c.runStream(ctx, pending)
		if err == nil {
			return nil // only nil on clean ctx cancellation
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		log.Printf("logflux agent: stream error, reconnecting in %s: %v", backoff, err)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
		if backoff < maxBackoff {
			backoff *= 2
		}
	}
}

// runStream opens one stream and serves it until ctx is canceled or the
// stream errors, returning any batched-but-unsent entries for retry.
func (c *Client) runStream(ctx context.Context, pending []*logfluxv1.LogEntry) ([]*logfluxv1.LogEntry, error) {
	stream, err := c.client.StreamLogs(ctx)
	if err != nil {
		return pending, err
	}

	ticker := time.NewTicker(c.cfg.FlushInterval)
	defer ticker.Stop()

	flush := func() error {
		for len(pending) > 0 {
			if err := stream.Send(pending[0]); err != nil {
				return err
			}
			pending = pending[1:]
		}
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			_ = flush()
			_, err := stream.CloseAndRecv()
			if err != nil {
				return pending, err
			}
			return pending, nil
		case entry := <-c.queue:
			pending = append(pending, entry)
			if len(pending) >= c.cfg.BatchSize {
				if err := flush(); err != nil {
					return pending, err
				}
			}
		case <-ticker.C:
			if err := flush(); err != nil {
				return pending, err
			}
		}
	}
}

func (c *Client) Close() error {
	return c.conn.Close()
}
