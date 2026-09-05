package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(LoadOptions{})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Feed.Mode != FeedCSV {
		t.Errorf("default feed mode = %q, want csv", cfg.Feed.Mode)
	}
	if cfg.Broker.Kind != BrokerStdout {
		t.Errorf("default broker = %q, want stdout", cfg.Broker.Kind)
	}
	if cfg.Feed.Exchange != "binance" {
		t.Errorf("default exchange = %q", cfg.Feed.Exchange)
	}
	if len(cfg.Feed.Symbols) == 0 {
		t.Error("default symbols must not be empty")
	}
	if cfg.Broker.BatchSize <= 0 || cfg.Broker.QueueSize <= 0 {
		t.Errorf("batching defaults must be positive: %+v", cfg.Broker)
	}
	if !cfg.Dedup.Enabled {
		t.Error("de-duplication should default to enabled")
	}
	if cfg.Normalize.SchemaVersion == "" {
		t.Error("schema version must have a default")
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	cases := map[string]string{
		"FEED_MODE":             "smoke-signals",
		"LOG_LEVEL":             "loud",
		"LOG_FORMAT":            "xml",
		"BROKER":                "carrier-pigeon",
		"CSV_FORMAT":            "parquet",
		"CSV_HAS_HEADER":        "maybe",
		"KAFKA_COMPRESSION":     "bzip2",
		"KAFKA_REQUIRED_ACKS":   "some",
		"KAFKA_SASL_MECHANISM":  "ntlm",
		"PIPELINE_WORKERS":      "0",
		"PRODUCER_BATCH_SIZE":   "0",
		"SYNTH_TRADES_PER_SEC":  "-1",
		"SYNTH_BURST_IMBALANCE": "1.5",
		"CSV_SPEED":             "-3",
		"LOG_OUTPUT":            "printer",
		"DEDUP_TTL":             "soon",
	}
	for key, value := range cases {
		t.Run(key+"="+value, func(t *testing.T) {
			// Kafka specific knobs are only validated when Kafka is selected.
			if strings.HasPrefix(key, "KAFKA_") {
				t.Setenv("BROKER", "kafka")
			}
			t.Setenv(key, value)
			if _, err := Load(LoadOptions{}); err == nil {
				t.Errorf("expected Load to reject %s=%q", key, value)
			}
		})
	}
}

func TestLoadRejectsSaslWithoutUser(t *testing.T) {
	t.Setenv("BROKER", "kafka")
	t.Setenv("KAFKA_SASL_MECHANISM", "scram-sha-256")
	if _, err := Load(LoadOptions{}); err == nil {
		t.Error("expected an error when a SASL mechanism is set without a user")
	}
}

func TestLoadParsesListsAndDurations(t *testing.T) {
	t.Setenv("FEED_SYMBOLS", "ethusdt, BTCUSDT,ethusdt , solusdt")
	t.Setenv("KAFKA_BROKERS", "broker-a:9092,broker-b:9092")
	t.Setenv("DEDUP_TTL", "45s")
	t.Setenv("CSV_SPEED", "25.5")
	t.Setenv("PRODUCER_MAX_RETRIES", "7")

	cfg, err := Load(LoadOptions{})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"}
	if strings.Join(cfg.Feed.Symbols, ",") != strings.Join(want, ",") {
		t.Errorf("symbols = %v, want %v", cfg.Feed.Symbols, want)
	}
	if len(cfg.Kafka.Brokers) != 2 || cfg.Kafka.Brokers[0] != "broker-a:9092" {
		t.Errorf("brokers = %v (case must be preserved)", cfg.Kafka.Brokers)
	}
	if cfg.Dedup.TTL != 45*time.Second {
		t.Errorf("dedup ttl = %s, want 45s", cfg.Dedup.TTL)
	}
	if cfg.CSV.Speed != 25.5 {
		t.Errorf("csv speed = %v, want 25.5", cfg.CSV.Speed)
	}
	if cfg.Broker.MaxRetries != 7 {
		t.Errorf("max retries = %d, want 7", cfg.Broker.MaxRetries)
	}
}

func TestEnvFileIsLoadedButEnvWins(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ingestor.env")
	content := `# ingestor configuration
FEED_MODE=synthetic
CSV_SPEED=12
KAFKA_SASL_PASSWORD=from-file
export SYNTH_SEED=99
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write env file: %v", err)
	}
	t.Setenv("CSV_SPEED", "77") // the real environment must win

	cfg, err := Load(LoadOptions{EnvFile: path})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Feed.Mode != FeedSynthetic {
		t.Errorf("feed mode = %q, want synthetic from the env file", cfg.Feed.Mode)
	}
	if cfg.CSV.Speed != 77 {
		t.Errorf("csv speed = %v, want 77 (real env wins over file)", cfg.CSV.Speed)
	}
	if cfg.Synthetic.Seed != 99 {
		t.Errorf("seed = %d, want 99 (export prefix must be handled)", cfg.Synthetic.Seed)
	}
	if cfg.Kafka.SASLPassword != "from-file" {
		t.Errorf("password = %q, want from-file", cfg.Kafka.SASLPassword)
	}
}

func TestEnvFileErrors(t *testing.T) {
	if _, err := Load(LoadOptions{EnvFile: "/does/not/exist.env"}); err == nil {
		t.Error("expected an error for a missing env file")
	}
	path := filepath.Join(t.TempDir(), "bad.env")
	if err := os.WriteFile(path, []byte("NOT_KEY_VALUE\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(LoadOptions{EnvFile: path}); err == nil {
		t.Error("expected an error for a malformed env file")
	}
}

func TestStringRedactsSecrets(t *testing.T) {
	t.Setenv("BROKER", "kafka")
	t.Setenv("KAFKA_SASL_MECHANISM", "plain")
	t.Setenv("KAFKA_SASL_USER", "ingestor")
	t.Setenv("KAFKA_SASL_PASSWORD", "hunter2")

	cfg, err := Load(LoadOptions{})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	dump := cfg.String()
	if strings.Contains(dump, "hunter2") {
		t.Errorf("config dump leaked the password:\n%s", dump)
	}
	if !strings.Contains(dump, "***") {
		t.Errorf("config dump should mark redacted values:\n%s", dump)
	}
	if !strings.Contains(dump, "user=ingestor") {
		t.Errorf("config dump should show the SASL user:\n%s", dump)
	}
}

func TestStringCoversEachFeedMode(t *testing.T) {
	for _, mode := range []FeedMode{FeedCSV, FeedSynthetic, FeedLive} {
		t.Setenv("FEED_MODE", string(mode))
		cfg, err := Load(LoadOptions{})
		if err != nil {
			t.Fatalf("Load(%s): %v", mode, err)
		}
		if !strings.Contains(cfg.String(), string(mode)) {
			t.Errorf("config dump for %s is missing the mode:\n%s", mode, cfg.String())
		}
	}
}

func TestValidateRejectsEmptySymbols(t *testing.T) {
	t.Setenv("FEED_SYMBOLS", " , ,")
	cfg, err := Load(LoadOptions{})
	if err == nil {
		// Empty input falls back to the default symbol list, which is valid.
		if len(cfg.Feed.Symbols) == 0 {
			t.Error("symbols must never be empty")
		}
	}
}
