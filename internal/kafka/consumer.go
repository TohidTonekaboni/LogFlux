package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
)

// Consumer wraps a group-managed Kafka reader: the broker assigns partitions
// across however many Consumer instances share GroupID, giving horizontal
// scaling per ARCHITECTURE.md §5.

type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			Topic:    topic,
			GroupID:  groupID,
			MinBytes: 1,
			MaxBytes: 10e6,
		}),
	}
}

func (c *Consumer) FetchMessage(ctx context.Context) (kafka.Message, error) {
	return c.reader.FetchMessage(ctx)
}

func (c *Consumer) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	return c.reader.CommitMessages(ctx, msgs...)
}

func (c *Consumer) Lag() int64 {
	return c.reader.Stats().Lag
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}