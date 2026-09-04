package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/config"
	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/feed"
	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/model"
	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/normalize"
	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/observability"
	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/producer"
)

// testApp wires the real pipeline together against an in-memory sink, so this
// test exercises feed -> dispatch -> dedup -> normalize -> producer end to end.
func testApp(t *testing.T, cfg config.Config, sink producer.Sink) (*app, *bytes.Buffer) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := observability.NewMetrics()
	source, err := feed.New(cfg, logger, metrics)
	if err != nil {
		t.Fatalf("feed.New: %v", err)
	}
	var buf bytes.Buffer
	if sink == nil {
		sink = producer.NewStdoutSink(&buf, io.Discard, logger)
	}
	a := &app{
		cfg:     cfg,
		log:     logger,
		metrics: metrics,
		server:  observability.NewServer(":0", metrics.Registry(), logger, nil),
		source:  source,
		norm:    normalize.New(cfg.Normalize, "test-host", metrics, logger),
		prod:    producer.NewAsyncProducer(cfg, sink, logger, metrics),
	}
	if cfg.Dedup.Enabled {
		a.dedup = normalize.NewDedup(cfg.Dedup.TTL, cfg.Dedup.MaxKeys)
	}
	return a, &buf
}

func csvConfig(t *testing.T, rows string) config.Config {
	t.Helper()
	path := filepath.Join(t.TempDir(), "BTCUSDT-trades-2024-01-01.csv")
	if err := os.WriteFile(path, []byte(rows), 0o600); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	t.Setenv("FEED_MODE", "csv")
	t.Setenv("CSV_PATH", path)
	t.Setenv("CSV_SPEED", "0")
	t.Setenv("BROKER", "stdout")
	t.Setenv("FEED_SYMBOLS", "BTCUSDT")
	t.Setenv("PIPELINE_WORKERS", "3")
	t.Setenv("STATS_EVERY", "10ms")

	cfg, err := config.Load(config.LoadOptions{})
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	return cfg
}

func decodeLines(t *testing.T, buf *bytes.Buffer) []model.Trade {
	t.Helper()
	var trades []model.Trade
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var tr model.Trade
		if err := json.Unmarshal([]byte(line), &tr); err != nil {
			t.Fatalf("output line is not a Trade: %q (%v)", line, err)
		}
		trades = append(trades, tr)
	}
	return trades
}

func TestPipelineCSVReplayEndToEnd(t *testing.T) {
	rows := "id,price,qty,quoteQty,time,isBuyerMaker,isBestMatch\n" +
		"1,42000.10,0.5,21000.05,1704153600000,false,true\n" +
		"2,42000.20,1.0,42000.20,1704153600100,true,true\n" +
		"3,42000.30,2.0,84000.60,1704153600200,false,true\n"
	cfg := csvConfig(t, rows)
	a, buf := testApp(t, cfg, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	a.prod.Start(ctx)
	code := a.runPipeline(ctx)
	a.prod.Close()

	if code != 0 {
		t.Fatalf("runPipeline exit code = %d, want 0", code)
	}
	trades := decodeLines(t, buf)
	if len(trades) != 3 {
		t.Fatalf("got %d trades, want 3", len(trades))
	}
	for i, tr := range trades {
		if tr.Symbol != "BTCUSDT" || tr.Exchange != "binance" {
			t.Errorf("trade %d identity = %s/%s", i, tr.Symbol, tr.Exchange)
		}
		if tr.Source != model.SourceCSV {
			t.Errorf("trade %d source = %q, want csv", i, tr.Source)
		}
		if err := tr.Validate(); err != nil {
			t.Errorf("trade %d failed validation: %v", i, err)
		}
		if tr.Host != "test-host" {
			t.Errorf("trade %d host = %q", i, tr.Host)
		}
	}
	if trades[0].Side != model.SideBuy || trades[1].Side != model.SideSell {
		t.Errorf("side derivation wrong: %s then %s", trades[0].Side, trades[1].Side)
	}
	if c := a.prod.Counters(); c.Published != 3 {
		t.Errorf("counters = %+v, want published=3", c)
	}
}

func TestPipelineSuppressesDuplicatesAndDeadLettersBadRows(t *testing.T) {
	rows := "id,price,qty,quoteQty,time,isBuyerMaker,isBestMatch\n" +
		"1,42000.10,0.5,21000.05,1704153600000,false,true\n" +
		"1,42000.10,0.5,21000.05,1704153600000,false,true\n" + // duplicate trade id
		"2,oops,1.0,1.0,1704153600100,true,true\n" + // unparseable price
		"3,42000.30,2.0,84000.60,1704153600200,false,true\n"
	cfg := csvConfig(t, rows)
	a, buf := testApp(t, cfg, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	a.prod.Start(ctx)
	code := a.runPipeline(ctx)
	a.prod.Close()

	if code != 0 {
		t.Fatalf("exit code = %d", code)
	}
	trades := decodeLines(t, buf)
	if len(trades) != 2 {
		t.Fatalf("got %d trades, want 2 (one duplicate, one invalid dropped)", len(trades))
	}
	if trades[0].TradeID != 1 || trades[1].TradeID != 3 {
		t.Errorf("unexpected trades: %d, %d", trades[0].TradeID, trades[1].TradeID)
	}
	if c := a.prod.Counters(); c.DeadLettered != 1 {
		t.Errorf("counters = %+v, want dead_lettered=1", c)
	}
}

func TestPipelineSyntheticFeedProducesTrades(t *testing.T) {
	t.Setenv("FEED_MODE", "synthetic")
	t.Setenv("BROKER", "stdout")
	t.Setenv("FEED_SYMBOLS", "BTCUSDT,ETHUSDT")
	t.Setenv("SYNTH_TRADES_PER_SEC", "100")
	t.Setenv("SYNTH_MAX_DURATION", "300ms")
	t.Setenv("PIPELINE_WORKERS", "2")

	cfg, err := config.Load(config.LoadOptions{})
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	a, buf := testApp(t, cfg, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	a.prod.Start(ctx)
	code := a.runPipeline(ctx)
	a.prod.Close()

	if code != 0 {
		t.Fatalf("exit code = %d", code)
	}
	trades := decodeLines(t, buf)
	if len(trades) < 5 {
		t.Fatalf("got %d trades, want at least 5", len(trades))
	}
	symbols := map[string]bool{}
	for _, tr := range trades {
		symbols[tr.Symbol] = true
		if tr.Source != model.SourceSynthetic {
			t.Errorf("source = %q, want synthetic", tr.Source)
		}
	}
	if len(symbols) != 2 {
		t.Errorf("symbols = %v, want both BTCUSDT and ETHUSDT", symbols)
	}
}

func TestShardIndexIsStableAndBounded(t *testing.T) {
	workers := 4
	first := shardIndex("BTCUSDT", workers)
	for i := 0; i < 10; i++ {
		if got := shardIndex("BTCUSDT", workers); got != first {
			t.Fatalf("shardIndex is not stable: %d != %d", got, first)
		}
	}
	// The same symbol in different case/spacing must map to the same worker,
	// otherwise per-symbol ordering would break.
	if shardIndex("btcusdt", workers) != shardIndex("BTC-USDT", workers) {
		t.Error("symbols that normalize to each other must shard identically")
	}
	for i := 0; i < 100; i++ {
		idx := shardIndex(string(rune('A'+i%26))+"USDT", workers)
		if idx < 0 || idx >= workers {
			t.Fatalf("shardIndex out of range: %d", idx)
		}
	}
	if shardIndex("BTCUSDT", 1) != 0 {
		t.Error("a single worker must get everything")
	}
}

func TestResolveLogOutput(t *testing.T) {
	// With the stdout broker, logs must move to stderr so `ingestor | jq` is clean.
	t.Setenv("BROKER", "stdout")
	t.Setenv("LOG_OUTPUT", "auto")
	cfg, err := config.Load(config.LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resolveLogOutput(cfg) != os.Stderr {
		t.Error("auto + stdout broker should log to stderr")
	}

	t.Setenv("BROKER", "kafka")
	cfg, err = config.Load(config.LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resolveLogOutput(cfg) != os.Stdout {
		t.Error("auto + kafka broker should log to stdout")
	}

	t.Setenv("LOG_OUTPUT", "stderr")
	cfg, err = config.Load(config.LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resolveLogOutput(cfg) != os.Stderr {
		t.Error("explicit LOG_OUTPUT=stderr should be honoured")
	}
}
