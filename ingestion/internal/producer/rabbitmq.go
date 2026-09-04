package producer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/config"
)

// RabbitSink publishes to RabbitMQ using a topic exchange.
//
// Topology:
//
//	exchange "trades" (topic, durable)  <- routing key trade.<exchange>.<symbol>
//	exchange "trades.dlx" (topic, durable) <- dead letters
//
// Publisher confirms are enabled by default, so Send only returns nil once the
// broker has accepted the batch. Combined with persistent delivery mode that
// gives at-least-once delivery; consumers de-duplicate on model.Trade.EventID.
type RabbitSink struct {
	cfg config.RabbitMQConfig
	log *slog.Logger

	mu       sync.Mutex
	conn     *amqp.Connection
	ch       *amqp.Channel
	confirms chan amqp.Confirmation
	closeErr chan *amqp.Error
}

// NewRabbitSink dials the broker and declares the topology.
func NewRabbitSink(cfg config.RabbitMQConfig, log *slog.Logger) (*RabbitSink, error) {
	if cfg.URL == "" {
		return nil, errors.New("RABBIT_URL is empty")
	}
	s := &RabbitSink{cfg: cfg, log: log}
	if err := s.connect(); err != nil {
		return nil, err
	}
	log.Info("rabbitmq sink configured",
		"exchange", cfg.Exchange, "type", cfg.ExchangeType,
		"dlx", cfg.DLX, "confirms", cfg.Confirms, "queue", cfg.Queue)
	return s, nil
}

func (s *RabbitSink) Name() string        { return "rabbitmq" }
func (s *RabbitSink) Destination() string { return s.cfg.Exchange }

// Send publishes a batch to the trades exchange.
func (s *RabbitSink) Send(ctx context.Context, msgs []Message) error {
	ch, err := s.channel()
	if err != nil {
		return err
	}
	for i, m := range msgs {
		if err := ch.PublishWithContext(ctx,
			s.cfg.Exchange,
			routingKey(s.cfg.RoutingKeyTmpl, m.Headers),
			false, // mandatory
			false, // immediate
			toAMQP(m, s.cfg.Persistent),
		); err != nil {
			s.invalidate(err)
			return fmt.Errorf("publish %d/%d: %w", i+1, len(msgs), err)
		}
	}
	return s.awaitConfirms(ctx, len(msgs))
}

// SendDeadLetter publishes a batch to the dead letter exchange.
func (s *RabbitSink) SendDeadLetter(ctx context.Context, msgs []Message) error {
	ch, err := s.channel()
	if err != nil {
		return err
	}
	for i, m := range msgs {
		if err := ch.PublishWithContext(ctx,
			s.cfg.DLX,
			routingKey(s.cfg.RoutingKeyTmpl, m.Headers),
			false, false,
			toAMQP(m, s.cfg.Persistent),
		); err != nil {
			s.invalidate(err)
			return fmt.Errorf("dead letter %d/%d: %w", i+1, len(msgs), err)
		}
	}
	return s.awaitConfirms(ctx, len(msgs))
}

// Ping reports whether the channel is still usable.
func (s *RabbitSink) Ping(context.Context) error {
	_, err := s.channel()
	return err
}

// Close tears down the channel and connection.
func (s *RabbitSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var errs []error
	if s.ch != nil {
		errs = append(errs, s.ch.Close())
		s.ch = nil
	}
	if s.conn != nil {
		errs = append(errs, s.conn.Close())
		s.conn = nil
	}
	return errors.Join(errs...)
}

// ---------- internals ----------

// channel returns a live channel, reconnecting if the previous one died.
func (s *RabbitSink) channel() (*amqp.Channel, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ch != nil {
		select {
		case err := <-s.closeErr:
			s.log.Warn("rabbitmq connection lost, reconnecting", "error", err)
			s.ch = nil
		default:
			return s.ch, nil
		}
	}
	if err := s.connectLocked(); err != nil {
		return nil, err
	}
	return s.ch, nil
}

// invalidate drops the cached channel after a terminal error so the next
// attempt reconnects instead of reusing a broken one.
func (s *RabbitSink) invalidate(err error) {
	if errors.Is(err, amqp.ErrClosed) {
		s.mu.Lock()
		s.ch = nil
		s.mu.Unlock()
	}
}

func (s *RabbitSink) connect() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connectLocked()
}

func (s *RabbitSink) connectLocked() error {
	conn, err := amqp.DialConfig(s.cfg.URL, amqp.Config{
		Heartbeat: 10 * time.Second,
		Locale:    "en_US",
	})
	if err != nil {
		return fmt.Errorf("dial rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("open rabbitmq channel: %w", err)
	}

	if s.cfg.Confirms {
		if err := ch.Confirm(false); err != nil {
			ch.Close()
			conn.Close()
			return fmt.Errorf("enable publisher confirms: %w", err)
		}
		s.confirms = ch.NotifyPublish(make(chan amqp.Confirmation, 4096))
	}

	if err := ch.ExchangeDeclare(s.cfg.Exchange, s.cfg.ExchangeType,
		s.cfg.Durable, false, false, false, nil); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("declare exchange %s: %w", s.cfg.Exchange, err)
	}
	if err := ch.ExchangeDeclare(s.cfg.DLX, "topic",
		s.cfg.Durable, false, false, false, nil); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("declare dead letter exchange %s: %w", s.cfg.DLX, err)
	}

	// Optional convenience queue for local development: it lets a single
	// ingestor + consumer work without extra topology work.
	if s.cfg.Queue != "" {
		if _, err := ch.QueueDeclare(s.cfg.Queue, s.cfg.Durable, false, false, false, amqp.Table{
			"x-dead-letter-exchange": s.cfg.DLX,
		}); err != nil {
			ch.Close()
			conn.Close()
			return fmt.Errorf("declare queue %s: %w", s.cfg.Queue, err)
		}
		if err := ch.QueueBind(s.cfg.Queue, "#", s.cfg.Exchange, false, nil); err != nil {
			ch.Close()
			conn.Close()
			return fmt.Errorf("bind queue %s: %w", s.cfg.Queue, err)
		}
	}
	if s.cfg.PrefetchCount > 0 {
		if err := ch.Qos(s.cfg.PrefetchCount, 0, false); err != nil {
			s.log.Warn("could not set prefetch", "error", err)
		}
	}

	s.conn = conn
	s.ch = ch
	s.closeErr = conn.NotifyClose(make(chan *amqp.Error, 1))
	return nil
}

// awaitConfirms waits for broker acknowledgements for a batch.
func (s *RabbitSink) awaitConfirms(ctx context.Context, n int) error {
	if !s.cfg.Confirms {
		return nil
	}
	timeout := s.cfg.ConfirmTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for i := 0; i < n; i++ {
		select {
		case c, ok := <-s.confirms:
			if !ok {
				s.invalidate(amqp.ErrClosed)
				return errors.New("confirm channel closed")
			}
			if !c.Ack {
				return fmt.Errorf("broker rejected message %d", c.DeliveryTag)
			}
		case <-timer.C:
			return fmt.Errorf("timed out after %s waiting for %d confirms (got %d)", timeout, n, i)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func toAMQP(m Message, persistent bool) amqp.Publishing {
	mode := amqp.Transient
	if persistent {
		mode = amqp.Persistent
	}
	headers := amqp.Table{}
	for k, v := range m.Headers {
		headers[k] = v
	}
	return amqp.Publishing{
		Headers:      headers,
		ContentType:  "application/json",
		DeliveryMode: mode,
		Timestamp:    time.Now(),
		MessageId:    m.Headers["event_id"],
		Body:         m.Value,
	}
}

// routingKey builds "trade.<exchange>.<symbol>" from message headers, falling
// back to the partition key when headers are absent.
func routingKey(tmpl string, headers map[string]string) string {
	exchange := headers["exchange"]
	symbol := strings.ToLower(headers["symbol"])
	if (exchange == "" || symbol == "") && headers != nil {
		// Last resort: the key is "exchange:symbol".
		if k := string(headers["partition_key"]); k != "" {
			parts := strings.SplitN(k, ":", 2)
			if len(parts) == 2 {
				exchange, symbol = parts[0], strings.ToLower(parts[1])
			}
		}
	}
	if exchange == "" {
		exchange = "unknown"
	}
	if symbol == "" {
		symbol = "unknown"
	}
	if tmpl == "" {
		tmpl = "trade.%s.%s"
	}
	return fmt.Sprintf(tmpl, exchange, symbol)
}
