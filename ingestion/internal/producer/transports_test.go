package producer

import (
	"context"
	"testing"

	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/config"
)

// These tests cover the pure configuration helpers of both transports, so the
// mapping from config text to broker options is verified without a live broker.

func TestCompressionCodecMapping(t *testing.T) {
	for _, name := range []string{"", "none", "gzip", "snappy", "lz4", "zstd"} {
		if _, err := compressionCodec(name); err != nil {
			t.Errorf("compressionCodec(%q): %v", name, err)
		}
	}
	if _, err := compressionCodec("bzip2"); err == nil {
		t.Error("expected an error for an unsupported codec")
	}
}

func TestRequiredAcksMapping(t *testing.T) {
	cases := map[string]int{"": -1, "all": -1, "one": 1, "none": 0}
	for name, want := range cases {
		got, err := requiredAcks(name)
		if err != nil {
			t.Errorf("requiredAcks(%q): %v", name, err)
			continue
		}
		if got != want {
			t.Errorf("requiredAcks(%q) = %d, want %d", name, got, want)
		}
	}
	if _, err := requiredAcks("most"); err == nil {
		t.Error("expected an error for an unsupported ack mode")
	}
}

func TestSASLMechanismMapping(t *testing.T) {
	cfg := config.KafkaConfig{}
	if m, err := saslMechanism(cfg); err != nil || m != nil {
		t.Errorf("no mechanism configured: got %v, %v", m, err)
	}

	cfg.SASLMechanism = "plain"
	if _, err := saslMechanism(cfg); err == nil {
		t.Error("PLAIN without a user must fail")
	}
	cfg.SASLUser = "ingestor"
	cfg.SASLPassword = "secret"
	if _, err := saslMechanism(cfg); err != nil {
		t.Errorf("PLAIN: %v", err)
	}

	for _, mech := range []string{"scram-sha-256", "scram-sha-512"} {
		cfg.SASLMechanism = mech
		if _, err := saslMechanism(cfg); err != nil {
			t.Errorf("%s: %v", mech, err)
		}
	}

	cfg.SASLMechanism = "ntlm"
	if _, err := saslMechanism(cfg); err == nil {
		t.Error("expected an error for an unsupported mechanism")
	}
}

func TestKafkaSinkRejectsEmptyBrokers(t *testing.T) {
	if _, err := NewKafkaSink(config.KafkaConfig{}, 10, nil); err == nil {
		t.Error("expected an error when KAFKA_BROKERS is empty")
	}
}

func TestKafkaHeadersAreSorted(t *testing.T) {
	headers := map[string]string{"z": "1", "a": "2", "m": "3"}
	out := toKafkaHeaders(headers)
	if len(out) != 3 {
		t.Fatalf("got %d headers, want 3", len(out))
	}
	want := []string{"a", "m", "z"}
	for i, h := range out {
		if h.Key != want[i] {
			t.Errorf("header %d = %q, want %q (headers must be sorted for deterministic batches)", i, h.Key, want[i])
		}
	}
	if out := toKafkaHeaders(nil); out != nil {
		t.Errorf("nil headers should produce nil, got %v", out)
	}
}

func TestRabbitRoutingKey(t *testing.T) {
	headers := map[string]string{"exchange": "binance", "symbol": "BTCUSDT"}
	// Symbols are lower-cased: RabbitMQ routing keys are case sensitive and
	// consumers should not have to guess the casing of a symbol.
	if got, want := routingKey("trade.%s.%s", headers), "trade.binance.btcusdt"; got != want {
		t.Errorf("routingKey = %q, want %q", got, want)
	}
	if got, want := routingKey("", headers), "trade.binance.btcusdt"; got != want {
		t.Errorf("default template routing key = %q, want %q", got, want)
	}
	// Missing headers must degrade gracefully rather than panic.
	if got := routingKey("trade.%s.%s", nil); got != "trade.unknown.unknown" {
		t.Errorf("routingKey with no headers = %q", got)
	}
	// The partition key can act as a fallback.
	if got := routingKey("trade.%s.%s", map[string]string{"partition_key": "coinbase:ETHUSDT"}); got != "trade.coinbase.ethusdt" {
		t.Errorf("routingKey fallback = %q", got)
	}
}

func TestToAMQPPublishing(t *testing.T) {
	msg := Message{
		Key:     []byte("binance:BTCUSDT"),
		Value:   []byte(`{"symbol":"BTCUSDT"}`),
		Headers: map[string]string{"event_id": "abc", "symbol": "BTCUSDT"},
	}
	pub := toAMQP(msg, true)
	if pub.DeliveryMode != 2 {
		t.Errorf("delivery mode = %d, want 2 (persistent)", pub.DeliveryMode)
	}
	if pub.ContentType != "application/json" {
		t.Errorf("content type = %q", pub.ContentType)
	}
	if pub.MessageId != "abc" {
		t.Errorf("message id = %q, want the event id header", pub.MessageId)
	}
	if string(pub.Body) != string(msg.Value) {
		t.Error("body must be the serialized trade")
	}
	if toAMQP(msg, false).DeliveryMode != 1 {
		t.Error("non persistent mode should be transient")
	}
}

func TestNewSinkFactory(t *testing.T) {
	cfg := config.Config{Broker: config.BrokerConfig{Kind: config.BrokerStdout}}
	sink, err := NewSink(cfg, nil)
	if err != nil {
		t.Fatalf("stdout sink: %v", err)
	}
	if sink.Name() != "stdout" {
		t.Errorf("sink name = %q", sink.Name())
	}

	cfg.Broker.Kind = config.BrokerDiscard
	if sink, err = NewSink(cfg, nil); err != nil || sink.Name() != "discard" {
		t.Errorf("discard sink: %v %v", sink, err)
	}

	cfg.Broker.Kind = "pigeon"
	if _, err := NewSink(cfg, nil); err == nil {
		t.Error("expected an error for an unknown broker")
	}
}

func TestDeadLettererContract(t *testing.T) {
	// The batcher only dead letters when the sink supports it; verify the
	// assertion used at runtime for both the supporting and plain sinks.
	var stdoutSink interface{} = NewStdoutSink(nil, nil, nil)
	if _, ok := stdoutSink.(DeadLetterer); !ok {
		t.Error("*StdoutSink must implement DeadLetterer")
	}
	if _, ok := interface{}(&mockSink{}).(DeadLetterer); !ok {
		t.Error("*mockSink must implement DeadLetterer")
	}
	var plain any = &plainSink{}
	if _, ok := plain.(DeadLetterer); ok {
		t.Error("a sink without SendDeadLetter must not satisfy DeadLetterer")
	}
}

// plainSink is the minimum Sink implementation: no dead lettering.
type plainSink struct{}

func (plainSink) Name() string                          { return "plain" }
func (plainSink) Destination() string                   { return "plain" }
func (plainSink) Send(context.Context, []Message) error { return nil }
func (plainSink) Close() error                          { return nil }
