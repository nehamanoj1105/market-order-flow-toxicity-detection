package forwarder

import (
	"context"
	"errors"

	kafka "github.com/segmentio/kafka-go"
)

// Forwarder publishes processed trade payloads to an output Kafka topic.
type Forwarder struct {
	writer *kafka.Writer
}

// New creates a new Kafka Forwarder targeting outputTopic.
func New(brokers []string, outputTopic string) (*Forwarder, error) {
	if len(brokers) == 0 {
		return nil, errors.New("brokers list cannot be empty")
	}
	if outputTopic == "" {
		outputTopic = "trades.normalized"
	}
	w := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  outputTopic,
		Balancer:               &kafka.Hash{},
		AllowAutoTopicCreation: true,
	}
	return &Forwarder{writer: w}, nil
}

// Forward sends trade payload to the output topic.
func (f *Forwarder) Forward(ctx context.Context, key []byte, payload []byte) error {
	return f.writer.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: payload,
	})
}

// Close closes the underlying Kafka writer connection.
func (f *Forwarder) Close() error {
	if f.writer != nil {
		return f.writer.Close()
	}
	return nil
}
