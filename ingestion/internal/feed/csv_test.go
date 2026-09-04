package feed

import (
	"context"
	"testing"
	"time"

	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/config"
	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/model"
)

const tradesHeader = "id,price,qty,quoteQty,time,isBuyerMaker,isBestMatch\n"

func TestCSVFeedReadsBinanceTradesDump(t *testing.T) {
	path := writeTempFile(t, "BTCUSDT-trades-2024-01-01.csv", tradesHeader+
		"1,42000.10,0.50000000,21000.05000000,1704153600000,false,true\n"+
		"2,42000.20,1.25000000,52500.25000000,1704153600100,true,true\n")

	f := NewCSVFeed(csvTestConfig(path), testLogger(), NoopStats())
	got := collect(t, f, 5*time.Second)

	if len(got) != 2 {
		t.Fatalf("got %d records, want 2", len(got))
	}
	first := got[0]
	if first.TradeID != 1 || first.Price != "42000.10" || first.Quantity != "0.50000000" {
		t.Errorf("unexpected first record: %+v", first)
	}
	if first.QuoteQuantity != "21000.05000000" {
		t.Errorf("quote quantity = %q", first.QuoteQuantity)
	}
	if first.IsBuyerMaker {
		t.Error("isBuyerMaker should be false for the first row")
	}
	if !got[1].IsBuyerMaker {
		t.Error("isBuyerMaker should be true for the second row")
	}
	if first.Symbol != "BTCUSDT" {
		t.Errorf("symbol = %q, want BTCUSDT", first.Symbol)
	}
	if first.Exchange != "binance" {
		t.Errorf("exchange = %q", first.Exchange)
	}
	if first.Source != model.SourceCSV {
		t.Errorf("source = %q, want csv", first.Source)
	}
	if first.TradeTimeMS != 1704153600000 || first.EventTimeMS != 1704153600000 {
		t.Errorf("timestamps = %d/%d", first.TradeTimeMS, first.EventTimeMS)
	}
	if first.Payload == nil {
		t.Error("raw payload should be retained for dead lettering")
	}
}

func TestCSVFeedReadsHeaderlessDumps(t *testing.T) {
	// Six column layout (older dumps, no quoteQty).
	sixCols := "1,42000.10,0.5,1704153600000,false,true\n" +
		"2,42000.20,1.25,1704153600100,true,true\n"
	path := writeTempFile(t, "BTCUSDT-trades-old.csv", sixCols)

	got := collect(t, NewCSVFeed(csvTestConfig(path), testLogger(), NoopStats()), 5*time.Second)
	if len(got) != 2 {
		t.Fatalf("six column layout: got %d records, want 2", len(got))
	}
	if got[0].Quantity != "0.5" {
		t.Errorf("quantity = %q, want 0.5", got[0].Quantity)
	}

	// Seven column layout (current dumps, with quoteQty).
	sevenCols := "1,42000.10,0.5,21000.05,1704153600000,false,true\n"
	path = writeTempFile(t, "BTCUSDT-trades-new.csv", sevenCols)
	got = collect(t, NewCSVFeed(csvTestConfig(path), testLogger(), NoopStats()), 5*time.Second)
	if len(got) != 1 {
		t.Fatalf("seven column layout: got %d records, want 1", len(got))
	}
	if got[0].QuoteQuantity != "21000.05" {
		t.Errorf("quote quantity = %q, want 21000.05", got[0].QuoteQuantity)
	}
}

func TestCSVFeedReadsAggTradesDump(t *testing.T) {
	path := writeTempFile(t, "BTCUSDT-aggTrades-2024-01-01.csv",
		"aggTradeId,price,quantity,firstTradeId,lastTradeId,transactTime,isBuyerMaker,isBestMatch\n"+
			"10,42000.10,0.5,100,100,1704153600000,false,true\n")

	cfg := csvTestConfig(path)
	got := collect(t, NewCSVFeed(cfg, testLogger(), NoopStats()), 5*time.Second)
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
	if got[0].TradeID != 10 {
		t.Errorf("trade id = %d, want the aggregate id 10", got[0].TradeID)
	}
	if got[0].Quantity != "0.5" {
		t.Errorf("quantity = %q, want 0.5", got[0].Quantity)
	}
}

func TestCSVFeedHonoursMaxRowsStartRowAndLoop(t *testing.T) {
	// Rows are 10,20,30,40,50 in the id column.
	content := tradesHeader +
		"10,42000.10,0.5,21000.05,1704153600000,false,true\n" +
		"20,42000.10,0.5,21000.05,1704153600001,false,true\n" +
		"30,42000.10,0.5,21000.05,1704153600002,false,true\n" +
		"40,42000.10,0.5,21000.05,1704153600003,false,true\n" +
		"50,42000.10,0.5,21000.05,1704153600004,false,true\n"
	path := writeTempFile(t, "BTCUSDT-trades-2024-01-01.csv", content)

	cfg := csvTestConfig(path)
	cfg.CSV.MaxRows = 2
	if got := collect(t, NewCSVFeed(cfg, testLogger(), NoopStats()), 5*time.Second); len(got) != 2 {
		t.Errorf("MaxRows=2: got %d records", len(got))
	}

	cfg = csvTestConfig(path)
	cfg.CSV.StartRow = 3
	got := collect(t, NewCSVFeed(cfg, testLogger(), NoopStats()), 5*time.Second)
	if len(got) != 3 {
		t.Fatalf("StartRow=3: got %d records, want 3", len(got))
	}
	if got[0].TradeID != 30 {
		t.Errorf("StartRow=3: first trade id = %d, want 30", got[0].TradeID)
	}

	cfg = csvTestConfig(path)
	cfg.CSV.Loop = true
	cfg.CSV.MaxRows = 7
	if got := collect(t, NewCSVFeed(cfg, testLogger(), NoopStats()), 5*time.Second); len(got) != 7 {
		t.Errorf("loop with MaxRows=7: got %d records", len(got))
	}
}

func TestCSVFeedSkipsMalformedRows(t *testing.T) {
	path := writeTempFile(t, "BTCUSDT-trades-2024-01-01.csv", tradesHeader+
		"1,42000.10,0.5,21000.05,1704153600000,false,true\n"+
		"not-an-id,42000.10,0.5,21000.05,1704153600001,false,true\n"+
		"3,42000.10,0.5,21000.05,1704153600002,false,true\n")

	got := collect(t, NewCSVFeed(csvTestConfig(path), testLogger(), NoopStats()), 5*time.Second)
	if len(got) != 2 {
		t.Fatalf("got %d records, want 2 (the bad row is skipped)", len(got))
	}
	if got[1].TradeID != 3 {
		t.Errorf("second record id = %d, want 3", got[1].TradeID)
	}
}

func TestCSVFeedReplaysAtConfiguredSpeed(t *testing.T) {
	// Three trades one second apart: at speed 10 the replay must take ~200 ms.
	path := writeTempFile(t, "BTCUSDT-trades-2024-01-01.csv", tradesHeader+
		"1,42000.10,0.5,21000.05,1704153600000,false,true\n"+
		"2,42000.10,0.5,21000.05,1704153601000,false,true\n"+
		"3,42000.10,0.5,21000.05,1704153602000,false,true\n")

	cfg := csvTestConfig(path)
	cfg.CSV.Speed = 10

	start := time.Now()
	got := collect(t, NewCSVFeed(cfg, testLogger(), NoopStats()), 5*time.Second)
	elapsed := time.Since(start)

	if len(got) != 3 {
		t.Fatalf("got %d records, want 3", len(got))
	}
	if elapsed < 150*time.Millisecond {
		t.Errorf("replay took %s, want at least ~200 ms at speed 10", elapsed)
	}
}

func TestCSVFeedStopsOnContextCancel(t *testing.T) {
	path := writeTempFile(t, "BTCUSDT-trades-2024-01-01.csv", tradesHeader+
		"1,42000.10,0.5,21000.05,1704153600000,false,true\n")

	cfg := csvTestConfig(path)
	cfg.CSV.Loop = true

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	f := NewCSVFeed(cfg, testLogger(), NoopStats())
	out := make(chan RawTrade, 8)
	done := make(chan error, 1)
	go func() { done <- f.Run(ctx, out) }()

	select {
	case err := <-done:
		if err != nil && ctx.Err() == nil {
			t.Errorf("Run returned %v, want nil or context error", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("feed ignored context cancellation")
	}
}

func TestCSVFeedReportsMissingFile(t *testing.T) {
	cfg := csvTestConfig("/definitely/not/here.csv")
	f := NewCSVFeed(cfg, testLogger(), NoopStats())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := f.Run(ctx, make(chan RawTrade, 1)); err == nil {
		t.Fatal("expected an error for a missing file")
	}
}

func TestResolveSymbolFromFile(t *testing.T) {
	cases := []struct {
		file string
		want string
	}{
		{"BTCUSDT-trades-2024-01-01.csv", "BTCUSDT"},
		{"ETHUSDT-trades-2024-01-01.csv", "ETHUSDT"},
		{"sample_binance_trades.csv", "BTCUSDT"}, // not a symbol: fall back to config
		{"BTCUSDT-aggTrades-2024-01-01.csv", "BTCUSDT"},
	}
	for _, tc := range cases {
		f := NewCSVFeed(csvTestConfig(tc.file), testLogger(), NoopStats())
		if got := f.resolveSymbol(tc.file); got != tc.want {
			t.Errorf("resolveSymbol(%q) = %q, want %q", tc.file, got, tc.want)
		}
	}
}

func TestCSVTimestampParsing(t *testing.T) {
	cases := map[string]int64{
		"1704153600000":        1704153600000, // epoch millis
		"1704153600":           1704153600000, // epoch seconds
		"2024-01-02T00:00:00Z": 1704153600000, // RFC3339
		"2024-01-02 00:00:00":  1704153600000,
	}
	for in, want := range cases {
		got, err := parseIntTime(in)
		if err != nil {
			t.Errorf("parseIntTime(%q): %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("parseIntTime(%q) = %d, want %d", in, got, want)
		}
	}
	if _, err := parseIntTime("yesterday"); err == nil {
		t.Error("parseIntTime should reject unparseable timestamps")
	}
}

func TestFeedFactory(t *testing.T) {
	cfg := csvTestConfig("x.csv")
	f, err := New(cfg, testLogger(), NoopStats())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if f.Name() != "csv-replay" {
		t.Errorf("Name() = %q", f.Name())
	}
	if _, ok := f.(*CSVFeed); !ok {
		t.Errorf("New returned %T, want *CSVFeed", f)
	}

	cfg.Feed.Mode = "nonsense"
	if _, err := New(cfg, testLogger(), NoopStats()); err == nil {
		t.Error("expected an error for an unknown feed mode")
	}
}

func TestCSVFormatDetection(t *testing.T) {
	if got := inferFormat([]string{"aggTradeId", "price", "quantity", "transactTime"}); got != config.CSVFormatAggTrades {
		t.Errorf("inferFormat(aggTradeId...) = %q", got)
	}
	if got := inferFormat([]string{"id", "price", "qty", "quoteQty", "time"}); got != config.CSVFormatTrades {
		t.Errorf("inferFormat(id,price,qty...) = %q", got)
	}
}
