package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/TohidTonekaboni/LogFlux/internal/agent"
	logfluxv1 "github.com/TohidTonekaboni/LogFlux/proto/logflux/v1"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client, err := agent.New(agent.Config{
		ServerAddr:    "localhost:9000",
		BatchSize:     50,
		FlushInterval: time.Second,
		StaticLabels:  map[string]string{"environment": "dev"},
	})
	if err != nil {
		log.Fatalf("logflux agent: %v", err)
	}
	defer client.Close()

	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				client.Enqueue(&logfluxv1.LogEntry{
					ServiceName:     "demo-agent",
					TimestampUnixMs: time.Now().UnixMilli(),
					Level:           logfluxv1.Level_INFO,
					Message:         "demo log line",
				})
			}
		}
	}()

	if err := client.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("logflux agent: %v", err)
	}
}
