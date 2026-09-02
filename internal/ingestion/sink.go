package ingestion

import (
	"context"
	"log"
	"sync"

	logfluxv1 "github.com/TohidTonekaboni/LogFlux/proto/logflux/v1"
)

// Sink is where a validated LogEntry goes after StreamLogs accepts it.
// Phase 2 adds a Kafka-backed implementation; StreamLogs itself doesn't change.
type Sink interface {
	Publish(ctx context.Context, entry *logfluxv1.LogEntry) error
}

// LogSink writes entries to the standard logger. Stands in for Kafka until Phase 2.
type LogSink struct{}

func (LogSink) Publish(_ context.Context, entry *logfluxv1.LogEntry) error {
	log.Printf("logflux: %s [%s] %s", entry.GetServiceName(), entry.GetLevel(), entry.GetMessage())
	return nil
}

// MemorySink buffers entries in memory; used by tests to assert on what the
// server accepted.
type MemorySink struct {
	mu      sync.Mutex
	entries []*logfluxv1.LogEntry
}

func NewMemorySink() *MemorySink {
	return &MemorySink{}
}

func (s *MemorySink) Publish(_ context.Context, entry *logfluxv1.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, entry)
	return nil
}

func (s *MemorySink) Entries() []*logfluxv1.LogEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*logfluxv1.LogEntry, len(s.entries))
	copy(out, s.entries)
	return out
}