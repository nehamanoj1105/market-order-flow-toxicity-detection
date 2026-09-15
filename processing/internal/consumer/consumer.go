package consumer

import (
	"context"
	"errors"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// Consumer wraps kafka-go Reader for consuming raw trade streams.
type Consumer struct {
	reader *kafka.Reader
}

// New creates a new Kafka Consumer listening on specified brokers, group ID and topic.
func New(brokers []string, groupID string, topic string) (*Consumer, error) {
	if len(brokers) == 0 {
		return nil, errors.New("brokers list cannot be empty")
	}
	if topic == "" {
		topic = "trades.raw"
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		StartOffset:    kafka.FirstOffset,
		MinBytes:       1,
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
	})
	return &Consumer{reader: r}, nil
}

// Poll reads a single message from the Kafka consumer stream.
func (c *Consumer) Poll(ctx context.Context) (kafka.Message, error) {
	return c.reader.ReadMessage(ctx)
}

// Close gracefully closes the underlying Kafka reader connection.
func (c *Consumer) Close() error {
	if c.reader != nil {
		return c.reader.Close()
	}
	return nil
}
