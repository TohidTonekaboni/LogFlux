package ingestion

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/TohidTonekaboni/LogFlux/internal/kafka"
	"github.com/TohidTonekaboni/LogFlux/internal/metrics"
	logfluxv1 "github.com/TohidTonekaboni/LogFlux/proto/logflux/v1"
	"google.golang.org/protobuf/proto"
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

// KafkaSink publishes entries to Kafka via a Producer, keyed by service name.
type KafkaSink struct {
	producer *kafka.Producer
}

func NewKafkaSink(producer *kafka.Producer) *KafkaSink {
	return &KafkaSink{producer: producer}
}

func (s *KafkaSink) Publish(ctx context.Context, entry *logfluxv1.LogEntry) error {
	value, err := proto.Marshal(entry)
	if err != nil {
		return err
	}

	start := time.Now()
	err = s.producer.Publish(ctx, entry.GetServiceName(), value)
	metrics.KafkaPublishDuration.Observe(time.Since(start).Seconds())
	return err
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
