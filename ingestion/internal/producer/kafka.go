package producer

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"

	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/config"
)

// KafkaSink publishes to Apache Kafka using segmentio/kafka-go.
//
// Design notes that matter for this project:
//   - The partition key is "exchange:symbol" and the balancer hashes it, so all
//     trades of one market land in one partition and arrive in order. The
//     scorer keeps per-symbol EWMA state, so per-symbol ordering is the only
//     ordering guarantee it needs.
//   - RequiredAcks defaults to "all" and auto topic creation is on for local
//     dev; a production deployment should create topics explicitly and grant
//     the ingestor only the Describe/Write ACLs it needs.
//   - Internal retries are disabled (MaxAttempts = 1): backoff and dead
//     lettering are owned by the batcher, so there is exactly one retry policy.
type KafkaSink struct {
	cfg       config.KafkaConfig
	batchSize int
	log       *slog.Logger
	main      *kafka.Writer
	dlq       *kafka.Writer
}

// NewKafkaSink builds the Kafka transport, creating the writers eagerly so a
// misconfiguration fails at startup rather than on the first trade.
func NewKafkaSink(cfg config.KafkaConfig, batchSize int, log *slog.Logger) (*KafkaSink, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("KAFKA_BROKERS is empty")
	}
	mechanism, err := saslMechanism(cfg)
	if err != nil {
		return nil, err
	}
	codec, err := compressionCodec(cfg.Compression)
	if err != nil {
		return nil, err
	}
	acks, err := requiredAcks(cfg.RequiredAcks)
	if err != nil {
		return nil, err
	}

	transport := &kafka.Transport{
		ClientID:    cfg.ClientID,
		SASL:        mechanism,
		DialTimeout: 10 * time.Second,
	}
	if cfg.TLSEnabled {
		transport.TLS = &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: cfg.TLSSkipVerify, // never enable outside a lab
		}
	}

	addr := kafka.TCP(cfg.Brokers...)
	newWriter := func(topic string) *kafka.Writer {
		return &kafka.Writer{
			Addr:                   addr,
			Topic:                  topic,
			Transport:              transport,
			Balancer:               &kafka.Hash{},
			MaxAttempts:            1,
			BatchSize:              batchSize,
			BatchTimeout:           5 * time.Millisecond,
			BatchBytes:             cfg.BatchBytes,
			WriteTimeout:           cfg.WriteTimeout,
			RequiredAcks:           kafka.RequiredAcks(acks),
			Compression:            codec,
			AllowAutoTopicCreation: cfg.AutoTopicCreation,
			Async:                  false,
		}
	}

	log.Info("kafka sink configured",
		"brokers", cfg.Brokers, "topic", cfg.Topic, "dlq_topic", cfg.DLQTopic,
		"acks", cfg.RequiredAcks, "compression", cfg.Compression)

	return &KafkaSink{
		cfg:       cfg,
		batchSize: batchSize,
		log:       log,
		main:      newWriter(cfg.Topic),
		dlq:       newWriter(cfg.DLQTopic),
	}, nil
}

func (k *KafkaSink) Name() string        { return "kafka" }
func (k *KafkaSink) Destination() string { return k.cfg.Topic }

// Send writes one batch to Kafka.
func (k *KafkaSink) Send(ctx context.Context, msgs []Message) error {
	return k.main.WriteMessages(ctx, toKafkaMessages(msgs)...)
}

// SendDeadLetter writes one batch to the dead letter topic.
func (k *KafkaSink) SendDeadLetter(ctx context.Context, msgs []Message) error {
	return k.dlq.WriteMessages(ctx, toKafkaMessages(msgs)...)
}

// Ping checks broker reachability. Used to flip /readyz.
func (k *KafkaSink) Ping(ctx context.Context) error {
	conn, err := kafka.DialContext(ctx, "tcp", k.cfg.Brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Brokers()
	return err
}

// Close releases both writers.
func (k *KafkaSink) Close() error {
	errs := []error{k.main.Close(), k.dlq.Close()}
	return errors.Join(errs...)
}

func toKafkaMessages(msgs []Message) []kafka.Message {
	out := make([]kafka.Message, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, kafka.Message{
			Key:     m.Key,
			Value:   m.Value,
			Time:    time.Now(),
			Headers: toKafkaHeaders(m.Headers),
		})
	}
	return out
}

// toKafkaHeaders sorts headers so that byte-for-byte identical batches are
// produced for identical input (map iteration order is random in Go).
func toKafkaHeaders(h map[string]string) []kafka.Header {
	if len(h) == 0 {
		return nil
	}
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]kafka.Header, 0, len(keys))
	for _, k := range keys {
		out = append(out, kafka.Header{Key: k, Value: []byte(h[k])})
	}
	return out
}

func saslMechanism(cfg config.KafkaConfig) (sasl.Mechanism, error) {
	switch cfg.SASLMechanism {
	case "":
		return nil, nil
	case "plain":
		if cfg.SASLUser == "" {
			return nil, errors.New("KAFKA_SASL_USER is required for SASL/PLAIN")
		}
		return plain.Mechanism{Username: cfg.SASLUser, Password: cfg.SASLPassword}, nil
	case "scram-sha-256":
		return scram.Mechanism(scram.SHA256, cfg.SASLUser, cfg.SASLPassword)
	case "scram-sha-512":
		return scram.Mechanism(scram.SHA512, cfg.SASLUser, cfg.SASLPassword)
	default:
		return nil, fmt.Errorf("unsupported KAFKA_SASL_MECHANISM %q", cfg.SASLMechanism)
	}
}

func compressionCodec(name string) (kafka.Compression, error) {
	switch name {
	case "", "none":
		return 0, nil
	case "gzip":
		return kafka.Gzip, nil
	case "snappy":
		return kafka.Snappy, nil
	case "lz4":
		return kafka.Lz4, nil
	case "zstd":
		return kafka.Zstd, nil
	default:
		return 0, fmt.Errorf("unsupported KAFKA_COMPRESSION %q", name)
	}
}

func requiredAcks(name string) (int, error) {
	switch name {
	case "", "all":
		return int(kafka.RequireAll), nil
	case "one":
		return int(kafka.RequireOne), nil
	case "none":
		return int(kafka.RequireNone), nil
	default:
		return 0, fmt.Errorf("unsupported KAFKA_REQUIRED_ACKS %q", name)
	}
}
