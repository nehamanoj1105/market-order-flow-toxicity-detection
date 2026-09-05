package producer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/model"
)

// AsyncOptions tunes the batching and retry behaviour of AsyncProducer.
type AsyncOptions struct {
	// QueueSize is the bounded backlog between the pipeline workers and the
	// broker. When it is full, producers block: backpressure all the way to the
	// feed, which is what keeps memory flat during a broker outage.
	QueueSize int
	// BatchSize is the number of messages per broker write.
	BatchSize int
	// Linger is the maximum time a partially filled batch waits before flushing.
	Linger time.Duration
	// MaxRetries is the number of *additional* attempts after the first failure.
	MaxRetries int
	// RetryBase / RetryMax bound the exponential backoff between attempts.
	RetryBase time.Duration
	RetryMax  time.Duration
	// DLQBatchSize is how many dead letter records are coalesced per write.
	DLQBatchSize int
}

// DLQRecord is the envelope used when a record cannot be normalized or
// published. Keeping the original payload means an operator can fix the bug and
// replay the data instead of losing it.
type DLQRecord struct {
	FailedAtMS int64  `json:"failed_at_ms"`
	Reason     string `json:"reason"`
	Detail     string `json:"detail"`
	Exchange   string `json:"exchange"`
	Symbol     string `json:"symbol"`
	TradeID    int64  `json:"trade_id,omitempty"`
	RawPayload string `json:"raw_payload,omitempty"`
	Host       string `json:"host,omitempty"`
}

// envelope tags queued work as normal traffic or dead letter traffic.
type envelope struct {
	msg Message
	dlq bool
}

// AsyncProducer decouples the normalize workers from broker latency.
//
// Guarantees, stated plainly because they matter to the scorer:
//   - At-least-once delivery. A batch that times out is retried, so a message
//     may be delivered twice after a failover. Consumers must be idempotent
//     (model.Trade.EventID is the idempotency key).
//   - Ordering. One flusher goroutine preserves the order in which messages
//     were queued, so per-symbol ordering set by the dispatcher survives.
//   - Backpressure. The queue is bounded; Publish blocks when it is full.
type AsyncProducer struct {
	sink  Sink
	opts  AsyncOptions
	log   *slog.Logger
	stats Stats

	in        chan envelope
	quit      chan struct{}
	closeOnce sync.Once
	mu        sync.RWMutex
	closed    bool
	done      chan struct{}
	wg        sync.WaitGroup

	published    atomic.Uint64
	batches      atomic.Uint64
	retries      atomic.Uint64
	failed       atomic.Uint64
	deadLettered atomic.Uint64
	dropped      atomic.Uint64
	bytes        atomic.Uint64
}

// NewAsync wraps sink with an asynchronous, batching, retrying producer.
func NewAsync(sink Sink, opts AsyncOptions, log *slog.Logger, stats Stats) *AsyncProducer {
	if opts.QueueSize <= 0 {
		opts.QueueSize = 4096
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 500
	}
	if opts.Linger <= 0 {
		opts.Linger = 20 * time.Millisecond
	}
	if opts.DLQBatchSize <= 0 {
		opts.DLQBatchSize = 64
	}
	if opts.RetryBase <= 0 {
		opts.RetryBase = 100 * time.Millisecond
	}
	if opts.RetryMax <= 0 {
		opts.RetryMax = 5 * time.Second
	}
	if log == nil {
		log = slog.Default()
	}
	if stats == nil {
		stats = NoopStats()
	}
	return &AsyncProducer{
		sink:  sink,
		opts:  opts,
		log:   log,
		stats: stats,
		in:    make(chan envelope, opts.QueueSize),
		quit:  make(chan struct{}),
		done:  make(chan struct{}),
	}
}

// Start launches the flusher goroutine. It returns immediately.
func (p *AsyncProducer) Start(ctx context.Context) {
	p.wg.Add(1)
	go p.loop(ctx)
}

// Publish normalizes a trade onto the queue. It blocks while the queue is full,
// which is how backpressure reaches the feed.
func (p *AsyncProducer) Publish(ctx context.Context, t model.Trade) error {
	msg, err := NewMessage(t)
	if err != nil {
		return err
	}
	return p.enqueue(ctx, envelope{msg: msg})
}

// DeadLetter queues a poison record for the dead letter destination.
func (p *AsyncProducer) DeadLetter(ctx context.Context, rec DLQRecord) error {
	body, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal dlq record: %w", err)
	}
	key := rec.Exchange + ":" + rec.Symbol
	msg := Message{
		Key:   []byte(key),
		Value: body,
		Headers: map[string]string{
			"reason":       rec.Reason,
			"exchange":     rec.Exchange,
			"symbol":       rec.Symbol,
			"failed_at_ms": fmtInt(rec.FailedAtMS),
			"content-type": "application/json",
		},
	}
	return p.enqueue(ctx, envelope{msg: msg, dlq: true})
}

// enqueue publishes onto the queue. The read lock is held for the whole send:
// Close takes the write lock, so it can only close the queue once every
// in-flight publisher has left this function. That is what makes
// "publish concurrently with shutdown" panic free.
func (p *AsyncProducer) enqueue(ctx context.Context, e envelope) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return ErrClosed
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.quit:
		return ErrClosed
	case p.in <- e:
		p.stats.SetQueueDepth(len(p.in))
		return nil
	}
}

// Close stops accepting work, drains what is queued and releases the sink.
// It is safe to call from the shutdown path after all publishers have stopped.
func (p *AsyncProducer) Close() {
	// Wake any publisher that is blocked on a full queue before locking, so it
	// cannot be stranded waiting for a flusher that is about to exit.
	p.closeOnce.Do(func() { close(p.quit) })

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.in)
	p.mu.Unlock()

	// Wait for the flusher to drain the channel, but never hang forever.
	waitDone := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-time.After(30 * time.Second):
		p.log.Warn("producer flush timed out during shutdown; some messages may be lost")
	}

	if err := p.sink.Close(); err != nil {
		p.log.Error("error closing sink", "sink", p.sink.Name(), "error", err)
	}
	close(p.done)
}

// Counters returns a snapshot of producer tallies.
func (p *AsyncProducer) Counters() Counters {
	return Counters{
		Published:    p.published.Load(),
		Batches:      p.batches.Load(),
		Retries:      p.retries.Load(),
		Failed:       p.failed.Load(),
		DeadLettered: p.deadLettered.Load(),
		Dropped:      p.dropped.Load(),
		Bytes:        p.bytes.Load(),
	}
}

// QueueDepth reports the current backlog.
func (p *AsyncProducer) QueueDepth() int { return len(p.in) }

func (p *AsyncProducer) loop(ctx context.Context) {
	defer p.wg.Done()

	mainBatch := make([]Message, 0, p.opts.BatchSize)
	dlqBatch := make([]Message, 0, p.opts.DLQBatchSize)
	linger := time.NewTicker(p.opts.Linger)
	defer linger.Stop()

	flushMain := func() {
		if len(mainBatch) == 0 {
			return
		}
		p.deliver(ctx, mainBatch, false)
		mainBatch = mainBatch[:0]
	}
	flushDLQ := func() {
		if len(dlqBatch) == 0 {
			return
		}
		p.deliver(ctx, dlqBatch, true)
		dlqBatch = dlqBatch[:0]
	}

	for {
		select {
		case e, ok := <-p.in:
			if !ok {
				// Channel closed: final drain.
				flushMain()
				flushDLQ()
				return
			}
			if e.dlq {
				dlqBatch = append(dlqBatch, e.msg)
				if len(dlqBatch) >= p.opts.DLQBatchSize {
					flushDLQ()
				}
				continue
			}
			mainBatch = append(mainBatch, e.msg)
			if len(mainBatch) >= p.opts.BatchSize {
				flushMain()
				linger.Reset(p.opts.Linger)
			}
			p.stats.SetQueueDepth(len(p.in))

		case <-linger.C:
			flushMain()
			flushDLQ()

		case <-ctx.Done():
			// Best effort final flush of whatever is already queued.
			for {
				select {
				case e, ok := <-p.in:
					if !ok {
						break // nothing left; the loop below breaks out
					}
					if e.dlq {
						dlqBatch = append(dlqBatch, e.msg)
					} else {
						mainBatch = append(mainBatch, e.msg)
					}
					continue
				default:
				}
				break
			}
			flushMain()
			flushDLQ()
			return
		}
	}
}

// deliver writes one batch, retrying transient failures before dead lettering.
func (p *AsyncProducer) deliver(ctx context.Context, batch []Message, dlq bool) {
	if len(batch) == 0 {
		return
	}
	name := p.sink.Name()
	dest := p.sink.Destination()

	var err error
	start := time.Now()
	for attempt := 0; ; attempt++ {
		if dlq {
			err = p.sendDeadLetter(ctx, batch)
		} else {
			err = p.sink.Send(ctx, batch)
		}
		if err == nil {
			break
		}
		if attempt >= p.opts.MaxRetries || ctx.Err() != nil {
			break
		}
		p.retries.Add(1)
		p.stats.PublishRetryTotal(name)
		delay := backoff(attempt, p.opts.RetryBase, p.opts.RetryMax)
		p.log.Warn("broker write failed, retrying",
			"sink", name, "attempt", attempt+1, "batch", len(batch), "delay", delay, "error", err)
		if sleepErr := sleep(ctx, delay); sleepErr != nil {
			break
		}
	}

	elapsed := time.Since(start)
	var sent int
	for _, m := range batch {
		sent += m.MessageSize()
	}

	if err != nil {
		p.failed.Add(uint64(len(batch)))
		if !dlq {
			p.stats.PublishErrorTotal(name, dest)
			p.log.Error("broker write failed permanently, dead lettering batch",
				"sink", name, "batch", len(batch), "error", err)
			if dlErr := p.sendDeadLetter(ctx, batch); dlErr == nil {
				p.deadLettered.Add(uint64(len(batch)))
				p.stats.DeadLetteredTotal(name, dest, len(batch))
			} else {
				p.dropped.Add(uint64(len(batch)))
				p.stats.DroppedTotal(name, len(batch))
				p.log.Error("dead lettering failed, batch dropped",
					"sink", name, "batch", len(batch), "error", dlErr)
			}
		} else {
			// The DLQ itself is down: nothing left to try.
			p.dropped.Add(uint64(len(batch)))
			p.stats.DroppedTotal(name, len(batch))
			p.log.Error("dead letter write failed, records dropped",
				"sink", name, "batch", len(batch), "error", err)
		}
		return
	}

	p.bytes.Add(uint64(sent))
	p.batches.Add(1)
	p.stats.ObserveBatch(len(batch))
	p.stats.ObservePublishLatency(elapsed)
	if dlq {
		p.deadLettered.Add(uint64(len(batch)))
		p.stats.DeadLetteredTotal(name, dest, len(batch))
	} else {
		p.published.Add(uint64(len(batch)))
		p.stats.PublishedTotal(name, dest, len(batch))
	}
}

// sendDeadLetter routes a batch to the DLQ if the sink supports one.
func (p *AsyncProducer) sendDeadLetter(ctx context.Context, batch []Message) error {
	dl, ok := p.sink.(DeadLetterer)
	if !ok {
		return errors.New("sink does not support dead lettering")
	}
	return dl.SendDeadLetter(ctx, batch)
}

// backoff is capped exponential backoff; attempt is zero based.
func backoff(attempt int, base, max time.Duration) time.Duration {
	d := base
	for i := 0; i < attempt; i++ {
		d *= 2
		if d >= max {
			return max
		}
	}
	if d > max {
		return max
	}
	return d
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func fmtInt(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		if v == -9223372036854775808 {
			return "-9223372036854775808"
		}
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
