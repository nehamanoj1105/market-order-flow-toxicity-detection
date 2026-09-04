package observability

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/config"
	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/model"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(&strings.Builder{}, nil))
}

func loggerConfig(format string) config.ServiceConfig {
	return config.ServiceConfig{
		Name:      "ingestor",
		LogLevel:  "info",
		LogFormat: format,
	}
}

func TestServerHealthAndReadiness(t *testing.T) {
	m := NewMetrics()
	srv := NewServer(":0", m.Registry(), testLogger(), map[string]any{"service": "ingestor"})

	rec := httptest.NewRecorder()
	srv.srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("/healthz = %d, want 200", rec.Code)
	}

	// Before the pipeline is up, /readyz must fail so the orchestrator does not
	// route traffic to a half-started pod.
	rec = httptest.NewRecorder()
	srv.srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("/readyz before ready = %d, want 503", rec.Code)
	}

	srv.SetReady(true)
	rec = httptest.NewRecorder()
	srv.srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("/readyz after ready = %d, want 200", rec.Code)
	}

	rec = httptest.NewRecorder()
	srv.srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/ = %d, want 200", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("index is not JSON: %v", err)
	}
	if body["service"] != "ingestor" {
		t.Errorf("index body = %v, want service=ingestor", body)
	}
	if body["ready"] != true {
		t.Errorf("index body ready flag = %v, want true", body)
	}
}

func TestServerExposesMetrics(t *testing.T) {
	m := NewMetrics()
	m.SetBuildInfo("1.2.3", "abc123", "2026-01-01", "test")
	m.FeedReadTotal("binance", "BTCUSDT", model.SourceLive)
	m.NormalizedTotal("binance", "BTCUSDT")
	m.PublishedTotal("kafka", "trades.normalized", 10)

	srv := NewServer(":0", m.Registry(), testLogger(), nil)
	rec := httptest.NewRecorder()
	srv.srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		`tofd_ingestor_feed_read_total{exchange="binance",source="live",symbol="BTCUSDT"} 1`,
		`tofd_ingestor_normalized_total{exchange="binance",symbol="BTCUSDT"} 1`,
		`tofd_ingestor_published_total{broker="kafka",destination="trades.normalized"} 10`,
		`tofd_ingestor_build_info{commit="abc123",date="2026-01-01",environment="test",version="1.2.3"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics output missing %q", want)
		}
	}
}

func TestMetricsCountersAndObservers(t *testing.T) {
	m := NewMetrics()
	for i := 0; i < 3; i++ {
		m.PublishRetryTotal("kafka")
	}
	if got := testutil.ToFloat64(m.publishRetries.WithLabelValues("kafka")); got != 3 {
		t.Errorf("retries = %v, want 3", got)
	}
	m.DeadLetteredTotal("kafka", "trades.normalized.dlq", 7)
	if got := testutil.ToFloat64(m.dlq.WithLabelValues("kafka", "trades.normalized.dlq")); got != 7 {
		t.Errorf("dead lettered = %v, want 7", got)
	}
	m.SetQueueDepth(42)
	if got := testutil.ToFloat64(m.queueDepth); got != 42 {
		t.Errorf("queue depth = %v, want 42", got)
	}
	m.SetDedupKeys(11)
	if got := testutil.ToFloat64(m.dedupKeys); got != 11 {
		t.Errorf("dedup keys = %v, want 11", got)
	}
	m.ObserveBatch(250)
	m.ObservePublishLatency(0)
	m.IngestLagMilliseconds("binance", "BTCUSDT", 12)
}

func TestParseLevel(t *testing.T) {
	cases := map[string]string{
		"debug": "DEBUG", "INFO": "INFO", "warn": "WARN", "warning": "WARN",
		"error": "ERROR", "nonsense": "INFO", "": "INFO",
	}
	for in, want := range cases {
		if got := parseLevel(in).String(); got != want {
			t.Errorf("parseLevel(%q) = %s, want %s", in, got, want)
		}
	}
}

func TestNewLoggerRespectsFormat(t *testing.T) {
	var sb strings.Builder
	logger := NewLogger(loggerConfig("json"), &sb)
	logger.Info("hello", "k", "v")
	if !strings.Contains(sb.String(), `"msg":"hello"`) {
		t.Errorf("json logger output = %q", sb.String())
	}

	sb.Reset()
	logger = NewLogger(loggerConfig("text"), &sb)
	logger.Info("hello", "k", "v")
	if !strings.Contains(sb.String(), "msg=hello") {
		t.Errorf("text logger output = %q", sb.String())
	}
}
