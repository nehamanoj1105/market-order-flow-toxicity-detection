package feed

import (
	"context"
	"testing"
	"time"

	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/config"
	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/model"
)

func synthTestConfig() config.Config {
	return config.Config{
		Service: config.ServiceConfig{Name: "test", LogLevel: "error", LogFormat: "text"},
		Feed: config.FeedConfig{
			Mode:      config.FeedSynthetic,
			Symbols:   []string{"BTCUSDT"},
			Exchange:  "binance",
			QueueSize: 64,
		},
		Synthetic: config.SyntheticConfig{
			Seed:            42,
			TradesPerSecond: 200,
			StartPrice:      65000,
			Volatility:      0.35,
			TickSize:        0.01,
			BaseQuantity:    0.05,
			BurstEvery:      time.Hour, // effectively disabled by default
			BurstDuration:   0,
			BurstImbalance:  0.9,
			BurstRateFactor: 3,
			BurstSizeFactor: 3,
			MaxDuration:     400 * time.Millisecond,
		},
	}
}

func TestSyntheticFeedIsDeterministic(t *testing.T) {
	a := collect(t, NewSyntheticFeed(synthTestConfig(), testLogger(), NoopStats()), 5*time.Second)
	bcfg := synthTestConfig()
	b := collect(t, NewSyntheticFeed(bcfg, testLogger(), NoopStats()), 5*time.Second)

	if len(a) != len(b) || len(a) < 20 {
		t.Fatalf("sizes differ or too small: %d vs %d", len(a), len(b))
	}
	for i := 0; i < 20; i++ {
		if a[i].Price != b[i].Price || a[i].Quantity != b[i].Quantity ||
			a[i].IsBuyerMaker != b[i].IsBuyerMaker || a[i].TradeID != b[i].TradeID {
			t.Fatalf("record %d differs between two runs with the same seed: %+v vs %+v", i, a[i], b[i])
		}
	}

	// A different seed must produce a different stream.
	ccfg := synthTestConfig()
	ccfg.Synthetic.Seed = 1234
	c := collect(t, NewSyntheticFeed(ccfg, testLogger(), NoopStats()), 5*time.Second)
	if len(c) > 0 && c[0].Price == a[0].Price && c[1].Price == a[1].Price {
		t.Error("different seeds produced an identical stream")
	}
}

func TestSyntheticFeedProducesWellFormedRecords(t *testing.T) {
	got := collect(t, NewSyntheticFeed(synthTestConfig(), testLogger(), NoopStats()), 5*time.Second)
	if len(got) == 0 {
		t.Fatal("feed produced nothing")
	}
	for _, rt := range got {
		if rt.Symbol != "BTCUSDT" || rt.Exchange != "binance" {
			t.Fatalf("unexpected identity: %+v", rt)
		}
		if rt.Source != model.SourceSynthetic {
			t.Fatalf("source = %q, want synthetic", rt.Source)
		}
		if rt.Price == "" || rt.Quantity == "" {
			t.Fatalf("empty numeric fields: %+v", rt)
		}
		if rt.TradeTimeMS <= 0 || rt.EventTimeMS <= 0 {
			t.Fatalf("bad timestamps: %+v", rt)
		}
	}
}

func TestSyntheticFeedHonoursArrivalRate(t *testing.T) {
	cfg := synthTestConfig()
	cfg.Synthetic.TradesPerSecond = 200
	cfg.Synthetic.MaxDuration = 700 * time.Millisecond

	got := collect(t, NewSyntheticFeed(cfg, testLogger(), NoopStats()), 10*time.Second)
	// Expect roughly 200/s * 0.7s = 140, with a wide margin for slow CI boxes.
	if len(got) < 50 || len(got) > 400 {
		t.Errorf("got %d trades, want roughly 140 (50..400)", len(got))
	}
}

func TestSyntheticFeedInjectsToxicBurst(t *testing.T) {
	cfg := synthTestConfig()
	cfg.Synthetic.TradesPerSecond = 200
	cfg.Synthetic.BurstEvery = 50 * time.Millisecond
	cfg.Synthetic.BurstDuration = 5 * time.Second // we stop before it ends
	cfg.Synthetic.BurstImbalance = 0.95
	cfg.Synthetic.MaxDuration = 900 * time.Millisecond

	got := collect(t, NewSyntheticFeed(cfg, testLogger(), NoopStats()), 10*time.Second)
	if len(got) < 50 {
		t.Fatalf("got only %d trades", len(got))
	}

	// Skip the calm opening window, then measure the aggressor imbalance.
	buys, sells := 0, 0
	for _, rt := range got {
		// isBuyerMaker == false <=> aggressive BUY.
		if !rt.IsBuyerMaker {
			buys++
		} else {
			sells++
		}
	}
	share := float64(buys) / float64(buys+sells)
	if share < 0.70 && (1-share) < 0.70 {
		t.Errorf("burst imbalance = %.2f buys / %.2f sells, want one side >= 0.70", share, 1-share)
	}
}

func TestSyntheticFeedStopsOnContextCancel(t *testing.T) {
	cfg := synthTestConfig()
	cfg.Synthetic.MaxDuration = 0 // run forever
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	f := NewSyntheticFeed(cfg, testLogger(), NoopStats())
	out := make(chan RawTrade, 16)
	done := make(chan error, 1)
	go func() { done <- f.Run(ctx, out) }()

	select {
	case err := <-done:
		if err != nil && ctx.Err() == nil {
			t.Errorf("Run returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("synthetic feed ignored context cancellation")
	}
}

func TestSyntheticFeedStopsAfterMaxDuration(t *testing.T) {
	cfg := synthTestConfig()
	cfg.Synthetic.MaxDuration = 150 * time.Millisecond
	start := time.Now()
	got := collect(t, NewSyntheticFeed(cfg, testLogger(), NoopStats()), 10*time.Second)
	if len(got) == 0 {
		t.Fatal("no trades produced")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("feed ran for %s, expected it to stop at MaxDuration", elapsed)
	}
}

func TestSyntheticFeedSupportsMultipleSymbols(t *testing.T) {
	cfg := synthTestConfig()
	cfg.Feed.Symbols = []string{"BTCUSDT", "ETHUSDT"}
	cfg.Synthetic.MaxDuration = 300 * time.Millisecond

	got := collect(t, NewSyntheticFeed(cfg, testLogger(), NoopStats()), 10*time.Second)
	seen := map[string]int{}
	for _, rt := range got {
		seen[rt.Symbol]++
	}
	if len(seen) != 2 {
		t.Errorf("symbols seen = %v, want both BTCUSDT and ETHUSDT", seen)
	}
}
