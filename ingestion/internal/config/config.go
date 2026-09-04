// Package config loads and validates the ingestor's runtime configuration from
// environment variables (optionally seeded from a .env style file).
//
// Every setting has a sane default so that the binary runs with no environment
// at all: `./ingestor` replays data/sample_binance_trades.csv and writes JSON
// lines to stdout. That makes the service trivial to demo and to unit test,
// while still supporting production knobs (SASL, TLS, batching, dedup).
package config

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

// FeedMode selects the market data source implementation.
type FeedMode string

const (
	FeedLive      FeedMode = "live"
	FeedCSV       FeedMode = "csv"
	FeedSynthetic FeedMode = "synthetic"
)

// BrokerKind selects the producer implementation.
type BrokerKind string

const (
	BrokerKafka   BrokerKind = "kafka"
	BrokerRabbit  BrokerKind = "rabbitmq"
	BrokerStdout  BrokerKind = "stdout"
	BrokerDiscard BrokerKind = "discard"
)

// CSVFormat selects the column layout of a replay file.
type CSVFormat string

const (
	CSVFormatTrades    CSVFormat = "binance-trades"    // data.binance.vision daily trade dumps
	CSVFormatAggTrades CSVFormat = "binance-aggtrades" // aggregated trade stream dumps
	CSVFormatAuto      CSVFormat = "auto"              // detect from the header row
)

// Config is the fully resolved configuration of the ingestor.
type Config struct {
	Service   ServiceConfig
	Feed      FeedConfig
	CSV       CSVConfig
	Synthetic SyntheticConfig
	Binance   BinanceConfig
	Pipeline  PipelineConfig
	Dedup     DedupConfig
	Broker    BrokerConfig
	Kafka     KafkaConfig
	RabbitMQ  RabbitMQConfig
	Normalize NormalizeConfig
}

// ServiceConfig covers logging and the observability HTTP server.
type ServiceConfig struct {
	Name        string // service name used in logs and metrics
	Environment string // dev | staging | prod, exported as a metric label
	LogLevel    string // debug | info | warn | error
	LogFormat   string // json | text
	LogOutput   string // auto | stdout | stderr
	HTTPAddr    string // listen address for /metrics, /healthz, /readyz
	StatsEvery  time.Duration
}

// FeedConfig is shared feed behaviour.
type FeedConfig struct {
	Mode      FeedMode
	Symbols   []string
	Exchange  string
	QueueSize int // buffered channel capacity between feed and workers
}

// CSVConfig configures historical replay.
type CSVConfig struct {
	Path      string
	Format    CSVFormat
	HasHeader string // auto | true | false
	Speed     float64
	Loop      bool
	MaxRows   int
	StartRow  int
}

// SyntheticConfig configures the built-in market simulator, which is what makes
// the toxicity detector testable end to end without an exchange connection.
type SyntheticConfig struct {
	Seed            int64
	TradesPerSecond float64
	StartPrice      float64
	Volatility      float64 // annualised-ish sigma used by the GBM price path
	TickSize        float64
	BaseQuantity    float64
	BurstEvery      time.Duration // start a toxic burst this often
	BurstDuration   time.Duration
	BurstImbalance  float64 // probability that a burst trade is on the toxic side
	BurstRateFactor float64 // arrival-rate multiplier during a burst
	BurstSizeFactor float64 // quantity multiplier during a burst
	MaxDuration     time.Duration
}

// BinanceConfig configures the live websocket (and optional REST backfill).
type BinanceConfig struct {
	WSURL         string
	RESTURL       string
	StreamKind    string // aggTrade | trade
	Backfill      bool
	BackfillLimit int
	ReadTimeout   time.Duration
	PingInterval  time.Duration
	ReconnectMin  time.Duration
	ReconnectMax  time.Duration
	MaxReconnects int // 0 = unlimited
}

// PipelineConfig controls the concurrent normalize/publish stage.
type PipelineConfig struct {
	Workers       int
	ChannelSize   int
	ShutdownGrace time.Duration
}

// DedupConfig controls the in-process duplicate suppressor.
type DedupConfig struct {
	Enabled bool
	TTL     time.Duration
	MaxKeys int
}

// BrokerConfig selects and tunes the producer.
type BrokerConfig struct {
	Kind       BrokerKind
	BatchSize  int
	Linger     time.Duration
	QueueSize  int
	MaxRetries int
	RetryBase  time.Duration
	RetryMax   time.Duration
}

// KafkaConfig configures the kafka-go writer.
type KafkaConfig struct {
	Brokers           []string
	Topic             string
	DLQTopic          string
	ClientID          string
	Compression       string // none | gzip | snappy | lz4 | zstd
	RequiredAcks      string // none | one | all
	WriteTimeout      time.Duration
	BatchBytes        int64
	AutoTopicCreation bool
	TLSEnabled        bool
	TLSSkipVerify     bool
	SASLMechanism     string // "" | plain | scram-sha-256 | scram-sha-512
	SASLUser          string
	SASLPassword      string
}

// RabbitMQConfig configures the AMQP publisher.
type RabbitMQConfig struct {
	URL            string
	Exchange       string
	ExchangeType   string
	DLX            string
	Queue          string // optional queue bound by the ingestor (dev convenience)
	RoutingKeyTmpl string
	Durable        bool
	Persistent     bool
	Confirms       bool
	ConfirmTimeout time.Duration
	PrefetchCount  int
}

// NormalizeConfig tunes the normalizer.
type NormalizeConfig struct {
	SchemaVersion   string
	ComputeQuoteQty bool
	DropInvalid     bool
	MaxPrice        float64
	MaxQuantity     float64
	MaxAge          time.Duration // drop trades older than this (0 disables)
}

// LoadOptions are inputs to Load.
type LoadOptions struct {
	// EnvFile, when set, is a KEY=VALUE file applied *under* real env vars.
	EnvFile string
}

// Load resolves the configuration and validates it.
func Load(opts LoadOptions) (Config, error) {
	if opts.EnvFile != "" {
		if err := applyEnvFile(opts.EnvFile); err != nil {
			return Config{}, err
		}
	}

	var errs []error
	cfg := Config{}

	cfg.Service = ServiceConfig{
		Name:        envString("SERVICE_NAME", "ingestor"),
		Environment: envString("ENVIRONMENT", "dev"),
		LogLevel:    strings.ToLower(envString("LOG_LEVEL", "info")),
		LogFormat:   strings.ToLower(envString("LOG_FORMAT", "json")),
		LogOutput:   strings.ToLower(envString("LOG_OUTPUT", "auto")),
		HTTPAddr:    envString("HTTP_ADDR", ":9090"),
		StatsEvery:  envDuration("STATS_EVERY", 15*time.Second, &errs),
	}

	cfg.Feed = FeedConfig{
		Mode:      FeedMode(strings.ToLower(envString("FEED_MODE", string(FeedCSV)))),
		Symbols:   upperAll(envList("FEED_SYMBOLS", []string{"BTCUSDT"})),
		Exchange:  strings.ToLower(envString("EXCHANGE", "binance")),
		QueueSize: envInt("FEED_QUEUE_SIZE", 8192, &errs),
	}

	cfg.CSV = CSVConfig{
		Path:      envString("CSV_PATH", "data/sample_binance_trades.csv"),
		Format:    CSVFormat(strings.ToLower(envString("CSV_FORMAT", string(CSVFormatAuto)))),
		HasHeader: strings.ToLower(envString("CSV_HAS_HEADER", "auto")),
		Speed:     envFloat("CSV_SPEED", 1.0, &errs),
		Loop:      envBool("CSV_LOOP", false, &errs),
		MaxRows:   envInt("CSV_MAX_ROWS", 0, &errs),
		StartRow:  envInt("CSV_START_ROW", 0, &errs),
	}

	cfg.Synthetic = SyntheticConfig{
		Seed:            int64(envInt("SYNTH_SEED", 42, &errs)),
		TradesPerSecond: envFloat("SYNTH_TRADES_PER_SEC", 50, &errs),
		StartPrice:      envFloat("SYNTH_START_PRICE", 65000, &errs),
		Volatility:      envFloat("SYNTH_VOLATILITY", 0.35, &errs),
		TickSize:        envFloat("SYNTH_TICK_SIZE", 0.01, &errs),
		BaseQuantity:    envFloat("SYNTH_BASE_QTY", 0.05, &errs),
		BurstEvery:      envDuration("SYNTH_BURST_EVERY", 90*time.Second, &errs),
		BurstDuration:   envDuration("SYNTH_BURST_DURATION", 12*time.Second, &errs),
		BurstImbalance:  envFloat("SYNTH_BURST_IMBALANCE", 0.88, &errs),
		BurstRateFactor: envFloat("SYNTH_BURST_RATE_FACTOR", 4, &errs),
		BurstSizeFactor: envFloat("SYNTH_BURST_SIZE_FACTOR", 3, &errs),
		MaxDuration:     envDuration("SYNTH_MAX_DURATION", 0, &errs),
	}

	cfg.Binance = BinanceConfig{
		WSURL:         envString("BINANCE_WS_URL", "wss://stream.binance.com:9443"),
		RESTURL:       envString("BINANCE_REST_URL", "https://api.binance.com"),
		StreamKind:    strings.ToLower(envString("BINANCE_STREAM", "aggTrade")),
		Backfill:      envBool("BINANCE_BACKFILL", false, &errs),
		BackfillLimit: envInt("BINANCE_BACKFILL_LIMIT", 1000, &errs),
		ReadTimeout:   envDuration("BINANCE_READ_TIMEOUT", 60*time.Second, &errs),
		PingInterval:  envDuration("BINANCE_PING_INTERVAL", 20*time.Second, &errs),
		ReconnectMin:  envDuration("BINANCE_RECONNECT_MIN", 500*time.Millisecond, &errs),
		ReconnectMax:  envDuration("BINANCE_RECONNECT_MAX", 30*time.Second, &errs),
		MaxReconnects: envInt("BINANCE_MAX_RECONNECTS", 0, &errs),
	}

	cfg.Pipeline = PipelineConfig{
		Workers:       envInt("PIPELINE_WORKERS", 4, &errs),
		ChannelSize:   envInt("PIPELINE_CHANNEL_SIZE", 1024, &errs),
		ShutdownGrace: envDuration("PIPELINE_SHUTDOWN_GRACE", 20*time.Second, &errs),
	}

	cfg.Dedup = DedupConfig{
		Enabled: envBool("DEDUP_ENABLED", true, &errs),
		TTL:     envDuration("DEDUP_TTL", 10*time.Minute, &errs),
		MaxKeys: envInt("DEDUP_MAX_KEYS", 1_000_000, &errs),
	}

	cfg.Broker = BrokerConfig{
		Kind:       BrokerKind(strings.ToLower(envString("BROKER", string(BrokerStdout)))),
		BatchSize:  envInt("PRODUCER_BATCH_SIZE", 500, &errs),
		Linger:     envDuration("PRODUCER_LINGER", 20*time.Millisecond, &errs),
		QueueSize:  envInt("PRODUCER_QUEUE_SIZE", 16384, &errs),
		MaxRetries: envInt("PRODUCER_MAX_RETRIES", 5, &errs),
		RetryBase:  envDuration("PRODUCER_RETRY_BASE", 100*time.Millisecond, &errs),
		RetryMax:   envDuration("PRODUCER_RETRY_MAX", 5*time.Second, &errs),
	}

	cfg.Kafka = KafkaConfig{
		Brokers:           envList("KAFKA_BROKERS", []string{"localhost:9092"}),
		Topic:             envString("KAFKA_TOPIC", "trades.normalized"),
		DLQTopic:          envString("KAFKA_DLQ_TOPIC", "trades.normalized.dlq"),
		ClientID:          envString("KAFKA_CLIENT_ID", "tofd-ingestor"),
		Compression:       strings.ToLower(envString("KAFKA_COMPRESSION", "snappy")),
		RequiredAcks:      strings.ToLower(envString("KAFKA_REQUIRED_ACKS", "all")),
		WriteTimeout:      envDuration("KAFKA_WRITE_TIMEOUT", 15*time.Second, &errs),
		BatchBytes:        int64(envInt("KAFKA_BATCH_BYTES", 1_048_576, &errs)),
		AutoTopicCreation: envBool("KAFKA_AUTO_CREATE_TOPIC", true, &errs),
		TLSEnabled:        envBool("KAFKA_TLS_ENABLED", false, &errs),
		TLSSkipVerify:     envBool("KAFKA_TLS_SKIP_VERIFY", false, &errs),
		SASLMechanism:     strings.ToLower(envString("KAFKA_SASL_MECHANISM", "")),
		SASLUser:          envString("KAFKA_SASL_USER", ""),
		SASLPassword:      envString("KAFKA_SASL_PASSWORD", ""),
	}

	cfg.RabbitMQ = RabbitMQConfig{
		URL:            envString("RABBIT_URL", "amqp://guest:guest@localhost:5672/"),
		Exchange:       envString("RABBIT_EXCHANGE", "trades"),
		ExchangeType:   strings.ToLower(envString("RABBIT_EXCHANGE_TYPE", "topic")),
		DLX:            envString("RABBIT_DLX", "trades.dlx"),
		Queue:          envString("RABBIT_QUEUE", ""),
		RoutingKeyTmpl: envString("RABBIT_ROUTING_KEY", "trade.%s.%s"), // exchange, symbol
		Durable:        envBool("RABBIT_DURABLE", true, &errs),
		Persistent:     envBool("RABBIT_PERSISTENT", true, &errs),
		Confirms:       envBool("RABBIT_CONFIRMS", true, &errs),
		ConfirmTimeout: envDuration("RABBIT_CONFIRM_TIMEOUT", 10*time.Second, &errs),
		PrefetchCount:  envInt("RABBIT_PREFETCH", 0, &errs),
	}

	cfg.Normalize = NormalizeConfig{
		SchemaVersion:   envString("SCHEMA_VERSION", "1.0.0"),
		ComputeQuoteQty: envBool("NORMALIZE_COMPUTE_QUOTE_QTY", true, &errs),
		DropInvalid:     envBool("NORMALIZE_DROP_INVALID", true, &errs),
		MaxPrice:        envFloat("NORMALIZE_MAX_PRICE", 0, &errs),
		MaxQuantity:     envFloat("NORMALIZE_MAX_QUANTITY", 0, &errs),
		MaxAge:          envDuration("NORMALIZE_MAX_AGE", 0, &errs),
	}

	if err := errors.Join(errs...); err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate enforces cross-field invariants that per-field parsing cannot catch.
func (c Config) Validate() error {
	var errs []error

	switch c.Feed.Mode {
	case FeedLive, FeedCSV, FeedSynthetic:
	default:
		errs = append(errs, fmt.Errorf("FEED_MODE must be one of live|csv|synthetic, got %q", c.Feed.Mode))
	}
	if len(c.Feed.Symbols) == 0 {
		errs = append(errs, errors.New("FEED_SYMBOLS must contain at least one symbol"))
	}
	if c.Feed.QueueSize <= 0 {
		errs = append(errs, errors.New("FEED_QUEUE_SIZE must be > 0"))
	}

	switch c.Service.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Errorf("LOG_LEVEL must be debug|info|warn|error, got %q", c.Service.LogLevel))
	}
	switch c.Service.LogFormat {
	case "json", "text":
	default:
		errs = append(errs, fmt.Errorf("LOG_FORMAT must be json|text, got %q", c.Service.LogFormat))
	}
	switch c.Service.LogOutput {
	case "auto", "stdout", "stderr":
	default:
		errs = append(errs, fmt.Errorf("LOG_OUTPUT must be auto|stdout|stderr, got %q", c.Service.LogOutput))
	}

	switch c.Broker.Kind {
	case BrokerKafka:
		if len(c.Kafka.Brokers) == 0 {
			errs = append(errs, errors.New("KAFKA_BROKERS must not be empty"))
		}
		switch c.Kafka.Compression {
		case "none", "gzip", "snappy", "lz4", "zstd":
		default:
			errs = append(errs, fmt.Errorf("KAFKA_COMPRESSION must be none|gzip|snappy|lz4|zstd, got %q", c.Kafka.Compression))
		}
		switch c.Kafka.RequiredAcks {
		case "none", "one", "all":
		default:
			errs = append(errs, fmt.Errorf("KAFKA_REQUIRED_ACKS must be none|one|all, got %q", c.Kafka.RequiredAcks))
		}
		switch c.Kafka.SASLMechanism {
		case "", "plain", "scram-sha-256", "scram-sha-512":
		default:
			errs = append(errs, fmt.Errorf("KAFKA_SASL_MECHANISM must be empty|plain|scram-sha-256|scram-sha-512, got %q", c.Kafka.SASLMechanism))
		}
		if c.Kafka.SASLMechanism != "" && c.Kafka.SASLUser == "" {
			errs = append(errs, errors.New("KAFKA_SASL_USER is required when KAFKA_SASL_MECHANISM is set"))
		}
	case BrokerRabbit:
		if c.RabbitMQ.URL == "" {
			errs = append(errs, errors.New("RABBIT_URL must not be empty"))
		}
	case BrokerStdout, BrokerDiscard:
	default:
		errs = append(errs, fmt.Errorf("BROKER must be one of kafka|rabbitmq|stdout|discard, got %q", c.Broker.Kind))
	}

	if c.Broker.BatchSize <= 0 {
		errs = append(errs, errors.New("PRODUCER_BATCH_SIZE must be > 0"))
	}
	if c.Broker.QueueSize <= 0 {
		errs = append(errs, errors.New("PRODUCER_QUEUE_SIZE must be > 0"))
	}
	if c.Pipeline.Workers <= 0 {
		errs = append(errs, errors.New("PIPELINE_WORKERS must be > 0"))
	}

	switch c.CSV.Format {
	case CSVFormatTrades, CSVFormatAggTrades, CSVFormatAuto:
	default:
		errs = append(errs, fmt.Errorf("CSV_FORMAT must be auto|binance-trades|binance-aggtrades, got %q", c.CSV.Format))
	}
	switch c.CSV.HasHeader {
	case "auto", "true", "false":
	default:
		errs = append(errs, fmt.Errorf("CSV_HAS_HEADER must be auto|true|false, got %q", c.CSV.HasHeader))
	}
	if c.CSV.Speed < 0 {
		errs = append(errs, errors.New("CSV_SPEED must be >= 0 (0 replays as fast as possible)"))
	}

	if c.Synthetic.TradesPerSecond <= 0 {
		errs = append(errs, errors.New("SYNTH_TRADES_PER_SEC must be > 0"))
	}
	if c.Synthetic.BurstImbalance < 0 || c.Synthetic.BurstImbalance > 1 {
		errs = append(errs, errors.New("SYNTH_BURST_IMBALANCE must be within [0,1]"))
	}

	return errors.Join(errs...)
}

// String returns a human readable, secret-redacted dump used by `ingestor -check`.
func (c Config) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "service.name=%s service.env=%s log=%s/%s http=%s\n",
		c.Service.Name, c.Service.Environment, c.Service.LogLevel, c.Service.LogFormat, c.Service.HTTPAddr)
	fmt.Fprintf(&b, "feed.mode=%s feed.exchange=%s feed.symbols=%s feed.queue=%d\n",
		c.Feed.Mode, c.Feed.Exchange, strings.Join(c.Feed.Symbols, ","), c.Feed.QueueSize)
	switch c.Feed.Mode {
	case FeedCSV:
		fmt.Fprintf(&b, "csv.path=%s csv.format=%s csv.header=%s csv.speed=%.2f csv.loop=%t\n",
			c.CSV.Path, c.CSV.Format, c.CSV.HasHeader, c.CSV.Speed, c.CSV.Loop)
	case FeedSynthetic:
		fmt.Fprintf(&b, "synth.rate=%.1f/s synth.seed=%d synth.burst=every %s for %s (imbalance %.2f)\n",
			c.Synthetic.TradesPerSecond, c.Synthetic.Seed, c.Synthetic.BurstEvery, c.Synthetic.BurstDuration, c.Synthetic.BurstImbalance)
	case FeedLive:
		fmt.Fprintf(&b, "binance.ws=%s binance.stream=%s backfill=%t\n", c.Binance.WSURL, c.Binance.StreamKind, c.Binance.Backfill)
	}
	fmt.Fprintf(&b, "pipeline.workers=%d dedup=%t(ttl=%s)\n", c.Pipeline.Workers, c.Dedup.Enabled, c.Dedup.TTL)
	fmt.Fprintf(&b, "broker.kind=%s broker.batch=%d linger=%s queue=%d retries=%d\n",
		c.Broker.Kind, c.Broker.BatchSize, c.Broker.Linger, c.Broker.QueueSize, c.Broker.MaxRetries)
	if c.Broker.Kind == BrokerKafka {
		auth := "none"
		if c.Kafka.SASLMechanism != "" {
			auth = c.Kafka.SASLMechanism + " (user=" + c.Kafka.SASLUser + ", password=***)"
		}
		fmt.Fprintf(&b, "kafka.brokers=%s kafka.topic=%s kafka.dlq=%s acks=%s compression=%s tls=%t sasl=%s\n",
			strings.Join(c.Kafka.Brokers, ","), c.Kafka.Topic, c.Kafka.DLQTopic,
			c.Kafka.RequiredAcks, c.Kafka.Compression, c.Kafka.TLSEnabled, auth)
	}
	if c.Broker.Kind == BrokerRabbit {
		fmt.Fprintf(&b, "rabbit.exchange=%s (%s) dlx=%s confirms=%t persistent=%t queue=%q\n",
			c.RabbitMQ.Exchange, c.RabbitMQ.ExchangeType, c.RabbitMQ.DLX, c.RabbitMQ.Confirms, c.RabbitMQ.Persistent, c.RabbitMQ.Queue)
	}
	return b.String()
}

// ---------- env helpers ----------

func envString(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envInt(key string, def int, errs *[]error) int {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return def
	}
	var v int
	if _, err := fmt.Sscanf(raw, "%d", &v); err != nil {
		*errs = append(*errs, fmt.Errorf("%s=%q is not an integer", key, raw))
		return def
	}
	return v
}

func envFloat(key string, def float64, errs *[]error) float64 {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return def
	}
	var v float64
	if _, err := fmt.Sscanf(raw, "%g", &v); err != nil {
		*errs = append(*errs, fmt.Errorf("%s=%q is not a number", key, raw))
		return def
	}
	return v
}

func envBool(key string, def bool, errs *[]error) bool {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return def
	}
	switch strings.ToLower(raw) {
	case "1", "true", "t", "yes", "y", "on":
		return true
	case "0", "false", "f", "no", "n", "off":
		return false
	default:
		*errs = append(*errs, fmt.Errorf("%s=%q is not a boolean", key, raw))
		return def
	}
}

func envDuration(key string, def time.Duration, errs *[]error) time.Duration {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return def
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s=%q is not a duration (e.g. 5s, 1m)", key, raw))
		return def
	}
	return v
}

// upperAll upper-cases every element; used for market symbols only.
func upperAll(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		out = append(out, strings.ToUpper(s))
	}
	return out
}

func envList(key string, def []string) []string {
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return def
	}
	parts := strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || r == ' ' || r == '\t' })
	if len(parts) == 0 {
		return def
	}
	out := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if _, dup := seen[p]; dup {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	sort.Strings(out)
	if len(out) == 0 {
		return def
	}
	return out
}
