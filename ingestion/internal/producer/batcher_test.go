package producer

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/model"
)

// mockSink is a controllable Sink + DeadLetterer used across the tests.
type mockSink struct {
	mu        sync.Mutex
	batches   [][]Message
	dlq       [][]Message
	calls     int
	failFirst int // first N Send calls fail
	closed    bool
}

func (m *mockSink) Name() string        { return "mock" }
func (m *mockSink) Destination() string { return "mock-topic" }

func (m *mockSink) Send(_ context.Context, msgs []Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.calls <= m.failFirst {
		return errors.New("simulated broker failure")
	}
	m.batches = append(m.batches, msgs)
	return nil
}

func (m *mockSink) SendDeadLetter(_ context.Context, msgs []Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dlq = append(m.dlq, msgs)
	return nil
}

func (m *mockSink) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *mockSink) received() []Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []Message{}
	for _, b := range m.batches {
		out = append(out, b...)
	}
	return out
}

func (m *mockSink) deadLettered() []Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []Message{}
	for _, b := range m.dlq {
		out = append(out, b...)
	}
	return out
}

// waitFor polls cond until it is true or the timeout expires.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

func testTrade(id int64) model.Trade {
	return model.Trade{
		EventID:       "event-" + time.Now().Format("150405.000000000"),
		SchemaVersion: model.SchemaVersion,
		Source:        model.SourceCSV,
		Exchange:      "binance",
		Symbol:        "BTCUSDT",
		TradeID:       id,
		Price:         65000.5,
		Quantity:      0.25,
		QuoteQuantity: 16250.125,
		Side:          model.SideBuy,
		TradeTimeMS:   1704153600000,
		EventTimeMS:   1704153600000,
		IngestedAtMS:  1704153600001,
	}
}

func TestNewMessageSerializesTrade(t *testing.T) {
	tr := testTrade(7)
	msg, err := NewMessage(tr)
	if err != nil {
		t.Fatalf("NewMessage: %v", err)
	}
	if string(msg.Key) != "binance:BTCUSDT" {
		t.Errorf("key = %q, want binance:BTCUSDT", msg.Key)
	}
	var decoded map[string]any
	if err := json.Unmarshal(msg.Value, &decoded); err != nil {
		t.Fatalf("body is not valid JSON: %v", err)
	}
	if decoded["symbol"] != "BTCUSDT" || decoded["trade_id"] != float64(7) {
		t.Errorf("unexpected body: %s", msg.Value)
	}
	if msg.Headers["schema_version"] != model.SchemaVersion {
		t.Errorf("schema_version header = %q", msg.Headers["schema_version"])
	}
}

func TestBatcherFlushesOnBatchSize(t *testing.T) {
	sink := &mockSink{}
	p := NewAsync(sink, AsyncOptions{QueueSize: 16, BatchSize: 5, Linger: 10 * time.Second, RetryBase: time.Millisecond}, nil, NoopStats())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)
	defer p.Close()

	for i := 1; i <= 5; i++ {
		if err := p.Publish(ctx, testTrade(int64(i))); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	waitFor(t, 2*time.Second, "a full batch", func() bool { return len(sink.received()) == 5 })
	if got := len(sink.batches); got != 1 {
		t.Errorf("batches = %d, want 1", got)
	}
}

func TestBatcherFlushesOnLinger(t *testing.T) {
	sink := &mockSink{}
	p := NewAsync(sink, AsyncOptions{QueueSize: 16, BatchSize: 1000, Linger: 30 * time.Millisecond, RetryBase: time.Millisecond}, nil, NoopStats())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)
	defer p.Close()

	for i := 1; i <= 3; i++ {
		if err := p.Publish(ctx, testTrade(int64(i))); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	waitFor(t, 2*time.Second, "a linger flush", func() bool { return len(sink.received()) == 3 })
}

func TestBatcherRetriesThenSucceeds(t *testing.T) {
	sink := &mockSink{failFirst: 2}
	p := NewAsync(sink, AsyncOptions{
		QueueSize: 16, BatchSize: 10, Linger: 20 * time.Millisecond,
		MaxRetries: 5, RetryBase: time.Millisecond, RetryMax: 10 * time.Millisecond,
	}, nil, NoopStats())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)
	defer p.Close()

	if err := p.Publish(ctx, testTrade(1)); err != nil {
		t.Fatalf("publish: %v", err)
	}
	waitFor(t, 3*time.Second, "delivery after retries", func() bool { return len(sink.received()) == 1 })
	if got := p.Counters().Retries; got != 2 {
		t.Errorf("retries = %d, want 2", got)
	}
	if got := p.Counters().Failed; got != 0 {
		t.Errorf("failed = %d, want 0", got)
	}
}

func TestBatcherDeadLettersAfterExhaustingRetries(t *testing.T) {
	sink := &mockSink{failFirst: 1000}
	p := NewAsync(sink, AsyncOptions{
		QueueSize: 16, BatchSize: 10, Linger: 20 * time.Millisecond,
		MaxRetries: 2, RetryBase: time.Millisecond, RetryMax: 5 * time.Millisecond,
	}, nil, NoopStats())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)
	defer p.Close()

	for i := 1; i <= 3; i++ {
		if err := p.Publish(ctx, testTrade(int64(i))); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	waitFor(t, 5*time.Second, "dead lettering", func() bool { return len(sink.deadLettered()) == 3 })

	c := p.Counters()
	if c.Failed != 3 || c.DeadLettered != 3 || c.Published != 0 {
		t.Errorf("counters = %+v, want failed=3 dead_lettered=3 published=0", c)
	}
}

func TestBatcherCloseFlushesRemainingMessages(t *testing.T) {
	sink := &mockSink{}
	p := NewAsync(sink, AsyncOptions{QueueSize: 16, BatchSize: 10_000, Linger: 10 * time.Second, RetryBase: time.Millisecond}, nil, NoopStats())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	for i := 1; i <= 4; i++ {
		if err := p.Publish(ctx, testTrade(int64(i))); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	p.Close()

	if got := len(sink.received()); got != 4 {
		t.Errorf("received %d messages after Close, want 4", got)
	}
	if !sink.closed {
		t.Error("Close must close the sink")
	}
	if err := p.Publish(context.Background(), testTrade(99)); !errors.Is(err, ErrClosed) {
		t.Errorf("Publish after Close = %v, want ErrClosed", err)
	}
}

func TestBatcherDeadLetterEnvelope(t *testing.T) {
	sink := &mockSink{}
	p := NewAsync(sink, AsyncOptions{QueueSize: 16, BatchSize: 1, Linger: 10 * time.Millisecond, RetryBase: time.Millisecond}, nil, NoopStats())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)
	defer p.Close()

	rec := DLQRecord{
		FailedAtMS: 1704153600000,
		Reason:     "bad_price",
		Detail:     `price="abc"`,
		Exchange:   "binance",
		Symbol:     "BTCUSDT",
		TradeID:    42,
		RawPayload: "42,abc,1,1,1704153600000,false,true",
		Host:       "ingestor-0",
	}
	if err := p.DeadLetter(ctx, rec); err != nil {
		t.Fatalf("DeadLetter: %v", err)
	}
	waitFor(t, 2*time.Second, "dlq delivery", func() bool { return len(sink.deadLettered()) == 1 })

	var got DLQRecord
	if err := json.Unmarshal(sink.deadLettered()[0].Value, &got); err != nil {
		t.Fatalf("decode dlq record: %v", err)
	}
	if got != rec {
		t.Errorf("dlq record round trip mismatch:\n got %+v\nwant %+v", got, rec)
	}
	if string(sink.deadLettered()[0].Key) != "binance:BTCUSDT" {
		t.Errorf("dlq key = %q", sink.deadLettered()[0].Key)
	}
}

func TestBatcherAppliesBackpressure(t *testing.T) {
	block := make(chan struct{})
	sink := &blockingSink{release: block}
	p := NewAsync(sink, AsyncOptions{
		QueueSize: 1, BatchSize: 1, Linger: 5 * time.Millisecond, RetryBase: time.Millisecond,
	}, nil, NoopStats())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	// First message: the flusher picks it up, the linger fires and Send blocks.
	if err := p.Publish(ctx, testTrade(1)); err != nil {
		t.Fatalf("publish 1: %v", err)
	}
	waitFor(t, 2*time.Second, "the flusher to block inside Send", func() bool { return sink.attempted() })

	// Second message fits in the single queue slot.
	if err := p.Publish(ctx, testTrade(2)); err != nil {
		t.Fatalf("publish 2: %v", err)
	}

	// Third must block: the queue is full and the sink is stuck.
	done := make(chan error, 1)
	go func() { done <- p.Publish(ctx, testTrade(3)) }()
	select {
	case <-done:
		t.Fatal("Publish did not block on a full queue: backpressure is broken")
	case <-time.After(150 * time.Millisecond):
	}

	close(block)
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("publish after release: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("publish stayed blocked after the sink drained")
	}
	cancel()
	p.Close()
}

// blockingSink accepts nothing until release is closed; it models a broker
// outage so we can assert the producer applies backpressure instead of
// buffering unboundedly.
type blockingSink struct {
	release  chan struct{}
	mu       sync.Mutex
	sent     int
	inFlight bool
}

func (b *blockingSink) Name() string        { return "blocking" }
func (b *blockingSink) Destination() string { return "blocking" }

func (b *blockingSink) Send(ctx context.Context, msgs []Message) error {
	b.mu.Lock()
	b.inFlight = true
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.inFlight = false
		b.mu.Unlock()
	}()
	select {
	case <-b.release:
		b.mu.Lock()
		b.sent += len(msgs)
		b.mu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *blockingSink) SendDeadLetter(context.Context, []Message) error { return nil }
func (b *blockingSink) Close() error                                    { return nil }

func (b *blockingSink) attempted() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.inFlight
}

func TestBackoffCapping(t *testing.T) {
	base, max := 100*time.Millisecond, 1*time.Second
	if got := backoff(0, base, max); got != base {
		t.Errorf("backoff(0) = %s, want %s", got, base)
	}
	if got := backoff(1, base, max); got != 200*time.Millisecond {
		t.Errorf("backoff(1) = %s, want 200ms", got)
	}
	if got := backoff(100, base, max); got != max {
		t.Errorf("backoff(100) = %s, want capped at %s", got, max)
	}
}
