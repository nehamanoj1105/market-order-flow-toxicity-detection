package producer

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
)

// StdoutSink writes newline delimited JSON to a writer.
//
// It is the default BROKER: running `./ingestor` with no infrastructure prints
// the exact payloads that would have gone to Kafka, which is the fastest way to
// eyeball the schema or pipe into jq. It is also what the unit tests use.
type StdoutSink struct {
	name    string
	dest    string
	out     *bufio.Writer
	dlqOut  io.Writer
	mu      sync.Mutex
	log     *slog.Logger
	written atomic.Uint64
}

// NewStdoutSink builds a sink writing to out (normal traffic) and dlqOut
// (dead letters). Either may be nil to discard.
func NewStdoutSink(out, dlqOut io.Writer, log *slog.Logger) *StdoutSink {
	if out == nil {
		out = io.Discard
	}
	if dlqOut == nil {
		dlqOut = io.Discard
	}
	if log == nil {
		log = slog.Default()
	}
	return &StdoutSink{name: "stdout", dest: "stdout", out: bufio.NewWriterSize(out, 64*1024), dlqOut: dlqOut, log: log}
}

func (s *StdoutSink) Name() string        { return s.name }
func (s *StdoutSink) Destination() string { return s.dest }

// Send writes each message as one JSON line.
func (s *StdoutSink) Send(_ context.Context, msgs []Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range msgs {
		if _, err := s.out.Write(m.Value); err != nil {
			return err
		}
		if err := s.out.WriteByte('\n'); err != nil {
			return err
		}
		s.written.Add(1)
	}
	return nil
}

// SendDeadLetter writes dead letters as JSON lines prefixed with a marker so
// they can be filtered out of the main stream with grep.
func (s *StdoutSink) SendDeadLetter(_ context.Context, msgs []Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range msgs {
		if _, err := io.WriteString(s.dlqOut, "DLQ "); err != nil {
			return err
		}
		if _, err := s.dlqOut.Write(m.Value); err != nil {
			return err
		}
		if _, err := io.WriteString(s.dlqOut, "\n"); err != nil {
			return err
		}
	}
	return nil
}

// Flush flushes buffered output.
func (s *StdoutSink) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.out.Flush()
}

// Close flushes and reports how many records were written.
func (s *StdoutSink) Close() error {
	if err := s.Flush(); err != nil {
		return err
	}
	s.log.Info("stdout sink closed", "records_written", s.written.Load())
	return nil
}

// DiscardSink accepts and throws away everything. It exists so the pipeline can
// be benchmarked (feed + normalize only) without I/O in the way.
type DiscardSink struct {
	count atomic.Uint64
	dlq   atomic.Uint64
}

// NewDiscardSink builds a sink that drops all traffic.
func NewDiscardSink() *DiscardSink { return &DiscardSink{} }

func (d *DiscardSink) Name() string        { return "discard" }
func (d *DiscardSink) Destination() string { return "discard" }

// Send discards the batch.
func (d *DiscardSink) Send(context.Context, []Message) error {
	return nil
}

// SendDeadLetter discards the batch.
func (d *DiscardSink) SendDeadLetter(context.Context, []Message) error {
	return nil
}

// Close is a no-op.
func (d *DiscardSink) Close() error { return nil }

// compile-time assertions that the built-in sinks satisfy the contract.
var (
	_ Sink         = (*StdoutSink)(nil)
	_ DeadLetterer = (*StdoutSink)(nil)
	_ Sink         = (*DiscardSink)(nil)
	_ DeadLetterer = (*DiscardSink)(nil)
	_ Sink         = (*KafkaSink)(nil)
	_ DeadLetterer = (*KafkaSink)(nil)
	_ Sink         = (*RabbitSink)(nil)
	_ DeadLetterer = (*RabbitSink)(nil)
	_              = os.Stdout
)
