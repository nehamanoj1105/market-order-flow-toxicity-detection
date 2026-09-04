package feed

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/config"
	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/model"
)

// SyntheticFeed is a seeded market simulator.
//
// Why it exists: toxicity scoring is a statistical detector, so it needs a
// controlled source of truth. This feed produces well-behaved two-sided flow
// most of the time and periodically injects a *toxic burst* — a window in which
// arrivals speed up, sizes grow and one side dominates (e.g. 88% of prints are
// aggressive buys) while price drifts in that direction. If the downstream
// EWMA/z-score scorer does not raise an alert during a burst, the pipeline is
// broken.
//
// With the same SYNTH_SEED the output is fully reproducible, which makes it
// usable in regression tests.
type SyntheticFeed struct {
	cfg   config.Config
	log   *slog.Logger
	stats Stats
	rng   *rand.Rand
}

// NewSyntheticFeed builds a simulator.
func NewSyntheticFeed(cfg config.Config, log *slog.Logger, stats Stats) *SyntheticFeed {
	seed := uint64(cfg.Synthetic.Seed)
	return &SyntheticFeed{
		cfg:   cfg,
		log:   log,
		stats: stats,
		rng:   rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15)),
	}
}

func (f *SyntheticFeed) Name() string { return "synthetic" }

// symState is the per-symbol simulation state.
type symState struct {
	symbol    string
	price     float64
	tradeID   int64
	nextAt    time.Time // wall-clock time of the next generated print
	burstEnd  time.Time // zero when not bursting
	burstSide model.Side
	nextBurst time.Time
}

// Run generates trades until ctx is cancelled (or MaxDuration elapses).
func (f *SyntheticFeed) Run(ctx context.Context, out chan<- RawTrade) error {
	sc := f.cfg.Synthetic
	symbols := f.cfg.Feed.Symbols
	if len(symbols) == 0 {
		return fmt.Errorf("FEED_SYMBOLS must contain at least one symbol")
	}

	now := time.Now()
	baseRate := sc.TradesPerSecond / float64(len(symbols))
	if baseRate <= 0 {
		baseRate = 1
	}

	states := make([]*symState, 0, len(symbols))
	for i, s := range symbols {
		symbol := model.NormalizeSymbol(s)
		// Spread starting prices so symbols are visually distinguishable.
		start := sc.StartPrice * (1 + 0.15*float64(i))
		if start <= 0 {
			start = 100
		}
		states = append(states, &symState{
			symbol:    symbol,
			price:     start,
			tradeID:   int64(4_000_000_000 + i*1_000_000),
			nextAt:    now,
			nextBurst: now.Add(sc.BurstEvery),
		})
	}

	f.log.Info("synthetic feed started",
		"symbols", symbols, "trades_per_sec", sc.TradesPerSecond,
		"seed", sc.Seed, "burst_every", sc.BurstEvery, "burst_duration", sc.BurstDuration,
		"burst_imbalance", sc.BurstImbalance)

	var deadline time.Time
	if sc.MaxDuration > 0 {
		deadline = now.Add(sc.MaxDuration)
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			return nil
		}

		// Pick the symbol whose next print is due soonest.
		next := states[0]
		for _, st := range states[1:] {
			if st.nextAt.Before(next.nextAt) {
				next = st
			}
		}

		wait := time.Until(next.nextAt)
		if wait > 0 {
			if err := sleepCtx(ctx, wait); err != nil {
				return err
			}
		}

		bursting := f.updateBurst(next, time.Now())
		rt := f.next(next, bursting)

		// Advance the symbol's clock by an exponential inter-arrival sample.
		rate := baseRate
		if bursting {
			rate *= multiplier(sc.BurstRateFactor)
		}
		gap := f.rng.ExpFloat64() / rate
		if gap > 1 {
			gap = 1 // never idle longer than a second per symbol
		}
		next.nextAt = next.nextAt.Add(time.Duration(gap * float64(time.Second)))
		if next.nextAt.Before(time.Now().Add(-2 * time.Second)) {
			next.nextAt = time.Now() // do not build an unbounded backlog
		}

		if err := send(ctx, out, rt); err != nil {
			return err
		}
		f.stats.FeedReadTotal(rt.Exchange, rt.Symbol, model.SourceSynthetic)
	}
}

// updateBurst starts/ends bursts and returns whether the symbol is bursting.
func (f *SyntheticFeed) updateBurst(st *symState, now time.Time) bool {
	sc := f.cfg.Synthetic
	if sc.BurstDuration <= 0 || sc.BurstEvery <= 0 {
		return false
	}
	if st.burstEnd.IsZero() {
		if !now.Before(st.nextBurst) {
			st.burstEnd = now.Add(sc.BurstDuration)
			if f.rng.Float64() < 0.5 {
				st.burstSide = model.SideBuy
			} else {
				st.burstSide = model.SideSell
			}
			f.log.Info("toxic burst started", "symbol", st.symbol, "side", st.burstSide,
				"duration", sc.BurstDuration, "imbalance", sc.BurstImbalance)
			f.stats.FeedEventTotal(f.Name(), "burst_start")
		}
		return false
	}
	if now.Before(st.burstEnd) {
		return true
	}
	st.burstEnd = time.Time{}
	st.nextBurst = now.Add(sc.BurstEvery)
	f.log.Info("toxic burst ended", "symbol", st.symbol)
	f.stats.FeedEventTotal(f.Name(), "burst_end")
	return false
}

// next produces one raw trade for the symbol, advancing its price path.
func (f *SyntheticFeed) next(st *symState, bursting bool) RawTrade {
	sc := f.cfg.Synthetic

	// Geometric Brownian Motion step. dt is expressed as a fraction of a year so
	// that SYNTH_VOLATILITY can be specified in the usual annualised terms.
	tradesPerYear := math.Max(sc.TradesPerSecond, 1) * 365 * 24 * 3600
	dt := 1 / tradesPerYear
	sigma := sc.Volatility
	if sigma <= 0 {
		sigma = 0.35
	}
	shock := sigma * math.Sqrt(dt) * f.rng.NormFloat64()

	drift := 0.0
	buyProb := 0.5
	sizeFactor := 1.0
	if bursting {
		if st.burstSide == model.SideBuy {
			buyProb = sc.BurstImbalance
			drift = 3e-6 * multiplier(sc.BurstSizeFactor)
		} else {
			buyProb = 1 - sc.BurstImbalance
			drift = -3e-6 * multiplier(sc.BurstSizeFactor)
		}
		sizeFactor = multiplier(sc.BurstSizeFactor)
	}
	st.price *= math.Exp(drift + shock)
	if st.price <= 0 {
		st.price = sc.StartPrice
	}

	price := roundTo(st.price, sc.TickSize)

	qty := sc.BaseQuantity * math.Exp(0.6*f.rng.NormFloat64()) * sizeFactor
	if qty <= 0 {
		qty = sc.BaseQuantity
	}
	qty = math.Round(qty*1e8) / 1e8

	isBuy := f.rng.Float64() < buyProb
	st.tradeID++

	now := time.Now().UnixMilli()
	return RawTrade{
		Exchange:     f.cfg.Feed.Exchange,
		Symbol:       st.symbol,
		Source:       model.SourceSynthetic,
		TradeID:      st.tradeID,
		Price:        strconv.FormatFloat(price, 'f', -1, 64),
		Quantity:     strconv.FormatFloat(qty, 'f', -1, 64),
		IsBuyerMaker: !isBuy, // aggressor BUY  <=> buyer was the taker <=> !isBuyerMaker
		TradeTimeMS:  now,
		EventTimeMS:  now,
	}
}

func multiplier(v float64) float64 {
	if v <= 0 {
		return 1
	}
	return v
}

func roundTo(price, tick float64) float64 {
	if tick <= 0 {
		return math.Round(price*1e8) / 1e8
	}
	return math.Round(price/tick) * tick
}
