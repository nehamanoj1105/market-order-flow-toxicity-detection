package producer

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/config"
)

// NewSink builds the transport selected by cfg.Broker.Kind.
func NewSink(cfg config.Config, log *slog.Logger) (Sink, error) {
	switch cfg.Broker.Kind {
	case config.BrokerKafka:
		return NewKafkaSink(cfg.Kafka, cfg.Broker.BatchSize, log)
	case config.BrokerRabbit:
		return NewRabbitSink(cfg.RabbitMQ, log)
	case config.BrokerStdout:
		return NewStdoutSink(os.Stdout, os.Stderr, log), nil
	case config.BrokerDiscard:
		return NewDiscardSink(), nil
	default:
		return nil, fmt.Errorf("unsupported BROKER %q", cfg.Broker.Kind)
	}
}

// NewAsyncProducer builds the batching producer wired to the service config.
func NewAsyncProducer(cfg config.Config, sink Sink, log *slog.Logger, stats Stats) *AsyncProducer {
	return NewAsync(sink, AsyncOptions{
		QueueSize:    cfg.Broker.QueueSize,
		BatchSize:    cfg.Broker.BatchSize,
		Linger:       cfg.Broker.Linger,
		MaxRetries:   cfg.Broker.MaxRetries,
		RetryBase:    cfg.Broker.RetryBase,
		RetryMax:     cfg.Broker.RetryMax,
		DLQBatchSize: 64,
	}, log, stats)
}
