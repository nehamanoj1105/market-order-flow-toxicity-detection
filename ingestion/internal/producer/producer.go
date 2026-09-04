// Package producer owns everything between a normalized trade and the message
// broker: serialization, batching, retries, dead lettering and the two
// concrete transports (Kafka and RabbitMQ).
//
// The shape is intentionally simple:
//
//	feed -> normalize -> AsyncProducer (queue + batching + retry) -> Sink -> broker
//
// Sink is the small synchronous interface a transport must implement; everything
// else (batching, backoff, DLQ, metrics) is transport independent and lives in
// batcher.go. Adding a third broker means implementing Sink, nothing more.
package producer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/model"
)

// ErrClosed is returned by Publish after the producer has been closed.
var ErrClosed = errors.New("producer is closed")

// Message is the transport agnostic unit of work handed to a Sink.
type Message struct {
	// Key is the partition/routing key ("exchange:symbol").
	Key []byte
	// Value is the serialized JSON body.
	Value []byte
	// Headers travel as broker headers so consumers can filter cheaply.
	Headers map[string]string
}

// NewMessage serializes a trade into a Message.
func NewMessage(t model.Trade) (Message, error) {
	body, err := json.Marshal(t)
	if err != nil {
		return Message{}, fmt.Errorf("marshal trade %s: %w", t.EventID, err)
	}
	return Message{Key: []byte(t.Key()), Value: body, Headers: t.Headers()}, nil
}

// Sink is a synchronous transport. Send must be safe for concurrent use only
// through the batcher, which calls it from a single goroutine; implementations
// that are called directly should guard themselves.
type Sink interface {
	// Name identifies the transport in logs and metrics, e.g. "kafka".
	Name() string
	// Destination is the topic or exchange messages land in.
	Destination() string
	// Send publishes the batch, returning an error if none of it was accepted.
	Send(ctx context.Context, msgs []Message) error
	// Close releases connections. It is called once, after the final flush.
	Close() error
}

// DeadLetterer is implemented by sinks that can route poison messages
// somewhere durable instead of dropping them on the floor.
type DeadLetterer interface {
	SendDeadLetter(ctx context.Context, msgs []Message) error
}

// Stats is the metrics surface the batcher reports to.
type Stats interface {
	PublishedTotal(broker, destination string, n int)
	PublishErrorTotal(broker, destination string)
	PublishRetryTotal(broker string)
	DeadLetteredTotal(broker, destination string, n int)
	DroppedTotal(broker string, n int)
	ObserveBatch(n int)
	ObservePublishLatency(d time.Duration)
	SetQueueDepth(n int)
}

// noopStats discards metrics (used by tests).
type noopStats struct{}

func (noopStats) PublishedTotal(string, string, int)    {}
func (noopStats) PublishErrorTotal(string, string)      {}
func (noopStats) PublishRetryTotal(string)              {}
func (noopStats) DeadLetteredTotal(string, string, int) {}
func (noopStats) DroppedTotal(string, int)              {}
func (noopStats) ObserveBatch(int)                      {}
func (noopStats) ObservePublishLatency(time.Duration)   {}
func (noopStats) SetQueueDepth(int)                     {}

// NoopStats returns a Stats implementation that discards everything.
func NoopStats() Stats { return noopStats{} }

// Counters is the runtime tally kept by the async producer.
type Counters struct {
	Published    uint64
	Batches      uint64
	Retries      uint64
	Failed       uint64
	DeadLettered uint64
	Dropped      uint64
	Bytes        uint64
}

// MessageSize returns the serialized size in bytes, used for batch byte budgets.
func (m Message) MessageSize() int {
	n := len(m.Key) + len(m.Value)
	for k, v := range m.Headers {
		n += len(k) + len(v)
	}
	return n
}
