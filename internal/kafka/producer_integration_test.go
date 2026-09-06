//go:build integration

package kafka_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/TohidTonekaboni/LogFlux/internal/kafka"
	kafkago "github.com/segmentio/kafka-go"
)

// Requires a live Kafka cluster (the docker-compose-infra.yml kafka-1/kafka-2
// external listeners). Run with: go test -tags=integration ./internal/kafka/...
func TestProducer_PublishesToTopic(t *testing.T) {
	brokers := []string{"localhost:9094", "localhost:9095"}
	topic := fmt.Sprintf("logs.raw.test.%d", time.Now().UnixNano())

	producer := kafka.NewProducer(brokers, topic)
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	want := []byte("integration-test-message")
	if err := producer.Publish(ctx, "svc-a", want); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	msg, err := reader.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if string(msg.Value) != string(want) {
		t.Errorf("message value = %q, want %q", msg.Value, want)
	}
	if string(msg.Key) != "svc-a" {
		t.Errorf("message key = %q, want %q", msg.Key, "svc-a")
	}
}
