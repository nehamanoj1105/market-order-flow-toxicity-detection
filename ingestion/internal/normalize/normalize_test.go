package normalize

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/config"
	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/feed"
	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/model"
)

func testConfig() config.NormalizeConfig {
	return config.NormalizeConfig{
		SchemaVersion:   model.SchemaVersion,
		ComputeQuoteQty: true,
		DropInvalid:     true,
	}
}

func rawTrade() feed.RawTrade {
	return feed.RawTrade{
		Exchange:     "binance",
		Symbol:       "btcusdt",
		Source:       model.SourceCSV,
		TradeID:      4001,
		Price:        "65000.25",
		Quantity:     "0.5",
		IsBuyerMaker: true,
		TradeTimeMS:  1704153600000,
		EventTimeMS:  1704153600005,
	}
}

func TestNormalizeHappyPath(t *testing.T) {
	n := New(testConfig(), "test-host", nil, nil)

	tr, err := n.Normalize(rawTrade())
	if err != nil {
		t.Fatalf("Normalize returned error: %v", err)
	}
	if tr.Symbol != "BTCUSDT" {
		t.Errorf("symbol = %q, want upper-cased BTCUSDT", tr.Symbol)
	}
	if tr.Price != 65000.25 || tr.Quantity != 0.5 {
		t.Errorf("price/qty = %v/%v", tr.Price, tr.Quantity)
	}
	if tr.QuoteQuantity != 65000.25*0.5 {
		t.Errorf("quote quantity = %v, want %v", tr.QuoteQuantity, 65000.25*0.5)
	}
	if tr.Side != model.SideSell {
		t.Errorf("side = %q, want SELL (isBuyerMaker=true)", tr.Side)
	}
	if !tr.IsBuyerMaker {
		t.Error("raw isBuyerMaker flag must be preserved")
	}
	if tr.Exchange != "binance" {
		t.Errorf("exchange = %q", tr.Exchange)
	}
	if tr.Host != "test-host" {
		t.Errorf("host = %q", tr.Host)
	}
	if tr.Sequence != 1 {
		t.Errorf("sequence = %d, want 1", tr.Sequence)
	}
	if tr.SchemaVersion != model.SchemaVersion {
		t.Errorf("schema version = %q", tr.SchemaVersion)
	}
	if _, err := uuid.Parse(tr.EventID); err != nil {
		t.Errorf("event_id %q is not a UUID: %v", tr.EventID, err)
	}
	if tr.TradeTimeMS != 1704153600000 || tr.EventTimeMS != 1704153600005 {
		t.Errorf("timestamps = %d/%d", tr.TradeTimeMS, tr.EventTimeMS)
	}
	if tr.IngestedAtMS <= 0 {
		t.Error("ingested_at_ms must be stamped")
	}
}

func TestNormalizeBuyerMakerSideMapping(t *testing.T) {
	n := New(testConfig(), "", nil, nil)

	raw := rawTrade()
	raw.IsBuyerMaker = false
	tr, err := n.Normalize(raw)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if tr.Side != model.SideBuy {
		t.Errorf("side = %q, want BUY", tr.Side)
	}
}

func TestNormalizeUsesProvidedQuoteQuantity(t *testing.T) {
	n := New(testConfig(), "", nil, nil)
	raw := rawTrade()
	raw.QuoteQuantity = "99999"
	tr, err := n.Normalize(raw)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if tr.QuoteQuantity != 99999 {
		t.Errorf("quote quantity = %v, want the feed supplied 99999", tr.QuoteQuantity)
	}

	// A garbage quote quantity falls back to price*quantity instead of failing.
	raw.QuoteQuantity = "not-a-number"
	tr, err = n.Normalize(raw)
	if err != nil {
		t.Fatalf("normalize with bad quote: %v", err)
	}
	if tr.QuoteQuantity != 65000.25*0.5 {
		t.Errorf("quote quantity = %v, want computed %v", tr.QuoteQuantity, 65000.25*0.5)
	}
}

func TestNormalizeRejectsBadRecords(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*feed.RawTrade)
		reason string
	}{
		{"empty symbol", func(r *feed.RawTrade) { r.Symbol = "" }, ReasonEmptySymbol},
		{"non numeric price", func(r *feed.RawTrade) { r.Price = "abc" }, ReasonBadPrice},
		{"empty price", func(r *feed.RawTrade) { r.Price = "" }, ReasonBadPrice},
		{"negative price", func(r *feed.RawTrade) { r.Price = "-1" }, ReasonBadPrice},
		{"zero quantity", func(r *feed.RawTrade) { r.Quantity = "0" }, ReasonBadQuantity},
		{"bad quantity", func(r *feed.RawTrade) { r.Quantity = "NaNx" }, ReasonBadQuantity},
		{"missing timestamps", func(r *feed.RawTrade) {
			r.TradeTimeMS = 0
			r.EventTimeMS = 0
		}, ReasonBadTimestamp},
	}

	for _, tc := range cases {
		n := New(testConfig(), "", nil, nil)
		raw := rawTrade()
		tc.mutate(&raw)
		_, err := n.Normalize(raw)
		if err == nil {
			t.Fatalf("%s: expected an error", tc.name)
		}
		ne, ok := err.(*Error)
		if !ok {
			t.Fatalf("%s: error is %T, want *normalize.Error", tc.name, err)
		}
		if ne.Reason != tc.reason {
			t.Errorf("%s: reason = %q, want %q", tc.name, ne.Reason, tc.reason)
		}
		if ne.Raw.TradeID != raw.TradeID {
			t.Errorf("%s: error did not carry the offending record", tc.name)
		}
	}
}

func TestNormalizeRangeAndStaleness(t *testing.T) {
	cfg := testConfig()
	cfg.MaxPrice = 1000
	cfg.MaxQuantity = 10
	cfg.MaxAge = time.Minute

	n := New(cfg, "", nil, nil)

	raw := rawTrade()
	if _, err := n.Normalize(raw); err == nil {
		t.Error("price above MaxPrice should be rejected")
	} else if e := err.(*Error); e.Reason != ReasonPriceRange {
		t.Errorf("reason = %q, want %q", e.Reason, ReasonPriceRange)
	}

	raw = rawTrade()
	raw.Price = "10"
	raw.Quantity = "1000"
	if _, err := n.Normalize(raw); err == nil {
		t.Error("quantity above MaxQuantity should be rejected")
	} else if e := err.(*Error); e.Reason != ReasonQuantityRange {
		t.Errorf("reason = %q, want %q", e.Reason, ReasonQuantityRange)
	}

	raw = rawTrade()
	raw.Price = "10"
	raw.TradeTimeMS = time.Now().Add(-2 * time.Hour).UnixMilli()
	if _, err := n.Normalize(raw); err == nil {
		t.Error("stale trade should be rejected")
	} else if e := err.(*Error); e.Reason != ReasonStale {
		t.Errorf("reason = %q, want %q", e.Reason, ReasonStale)
	}
}

func TestNormalizeAllSeparatesGoodAndBad(t *testing.T) {
	n := New(testConfig(), "", nil, nil)
	good := rawTrade()
	badPrice := rawTrade()
	badPrice.TradeID = 4002
	badPrice.Price = "oops"
	badSymbol := rawTrade()
	badSymbol.TradeID = 4003
	badSymbol.Symbol = ""

	trades, errs := n.NormalizeAll([]feed.RawTrade{good, badPrice, badSymbol})
	if len(trades) != 1 {
		t.Fatalf("got %d good trades, want 1", len(trades))
	}
	if len(errs) != 2 {
		t.Fatalf("got %d errors, want 2", len(errs))
	}
	if n.Sequence() != 1 {
		t.Errorf("sequence = %d, want 1", n.Sequence())
	}
}

func TestNormalizerIsConcurrencySafe(t *testing.T) {
	n := New(testConfig(), "", nil, nil)
	var wg sync.WaitGroup
	const goroutines, perGoroutine = 8, 100
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				if _, err := n.Normalize(rawTrade()); err != nil {
					t.Errorf("unexpected error: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if got := n.Sequence(); got != goroutines*perGoroutine {
		t.Errorf("sequence = %d, want %d", got, goroutines*perGoroutine)
	}
}

func TestNormalizeReportsStats(t *testing.T) {
	stats := &recordingStats{}
	n := New(testConfig(), "", stats, nil)

	if _, err := n.Normalize(rawTrade()); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	bad := rawTrade()
	bad.Price = "nope"
	if _, err := n.Normalize(bad); err == nil {
		t.Fatal("expected error")
	}

	if stats.normalized != 1 {
		t.Errorf("normalized = %d, want 1", stats.normalized)
	}
	if stats.errors != 1 {
		t.Errorf("errors = %d, want 1", stats.errors)
	}
	if stats.lastReason != ReasonBadPrice {
		t.Errorf("last reason = %q, want %q", stats.lastReason, ReasonBadPrice)
	}
}

// recordingStats is a hand written test double for the Stats interface.
type recordingStats struct {
	normalized int
	errors     int
	duplicates int
	lastReason string
}

func (r *recordingStats) NormalizedTotal(_, _ string)                  { r.normalized++ }
func (r *recordingStats) NormalizeErrorTotal(_, _, reason string)      { r.errors++; r.lastReason = reason }
func (r *recordingStats) DuplicateTotal(_, _ string)                   { r.duplicates++ }
func (r *recordingStats) IngestLagMilliseconds(_, _ string, _ float64) {}
