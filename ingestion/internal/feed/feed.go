// Package feed implements the pluggable market data sources of the ingestor.
//
// Three implementations are provided:
//
//   - binance.go  : live Binance public websocket (aggTrade/trade streams) with
//     exponential-backoff reconnect and optional REST backfill of the gap.
//   - csv.go      : deterministic replay of historical dumps such as the files
//     published at https://data.binance.vision (used by tests and demos).
//   - synthetic.go: a seeded market simulator that can inject one-sided
//     "toxic" bursts on a timer, which is what makes the downstream
//     EWMA/z-score detector testable without an exchange account.
//
// Every feed converts its wire format into the common RawTrade struct; the
// normalize package is then responsible for turning RawTrade into the canonical
// model.Trade that matches shared/trade_schema.json.
package feed

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/config"
	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/model"
)

// RawTrade is the un-normalized representation of one trade as it arrives from
// a data feed. Numeric fields deliberately stay as strings so that the exchange
// decimal text is parsed exactly once, in the normalizer.
type RawTrade struct {
	// Exchange that produced the record, e.g. "binance".
	Exchange string
	// Symbol as reported by the feed (not yet normalized).
	Symbol string
	// Source is live, csv or synthetic.
	Source model.Source
	// TradeID is the exchange (aggregate) trade id.
	TradeID int64
	// Price is the raw decimal text of the execution price.
	Price string
	// Quantity is the raw decimal text of the executed base amount.
	Quantity string
	// QuoteQuantity is the raw decimal text of price*quantity, when the feed
	// provides it. Empty means "compute it in the normalizer".
	QuoteQuantity string
	// IsBuyerMaker is the raw exchange flag: true => aggressor is the SELLER.
	IsBuyerMaker bool
	// TradeTimeMS is the exchange match timestamp.
	TradeTimeMS int64
	// EventTimeMS is when the feed delivered the record.
	EventTimeMS int64
	// Payload is the original bytes (websocket frame or CSV line). It is
	// attached to dead letter records so bad data can be replayed later.
	Payload []byte
}

// Feed is implemented by every market data source.
//
// Run owns its own goroutine(s) and pushes records into out. It MUST return as
// soon as ctx is cancelled, and MUST NOT close out (the pipeline owns it).
// Backpressure is natural: sending on a full out channel blocks the feed.
type Feed interface {
	// Name identifies the feed in logs and metrics.
	Name() string
	// Run streams raw trades until ctx is cancelled or an unrecoverable error
	// occurs. A nil error means the feed finished cleanly (e.g. CSV EOF).
	Run(ctx context.Context, out chan<- RawTrade) error
}

// Stats is the metrics surface a feed is allowed to touch. Keeping it an
// interface keeps feeds unit-testable without a Prometheus registry.
type Stats interface {
	FeedReadTotal(exchange, symbol string, source model.Source)
	FeedErrorTotal(feed, reason string)
	FeedEventTotal(feed, kind string)
}

// New builds the feed selected by cfg.Feed.Mode.
func New(cfg config.Config, log *slog.Logger, stats Stats) (Feed, error) {
	switch cfg.Feed.Mode {
	case config.FeedLive:
		return NewBinanceFeed(cfg, log, stats), nil
	case config.FeedCSV:
		return NewCSVFeed(cfg, log, stats), nil
	case config.FeedSynthetic:
		return NewSyntheticFeed(cfg, log, stats), nil
	default:
		return nil, fmt.Errorf("unknown FEED_MODE %q", cfg.Feed.Mode)
	}
}

// noopStats is used when the caller has no metrics implementation.
type noopStats struct{}

func (noopStats) FeedReadTotal(string, string, model.Source) {}
func (noopStats) FeedErrorTotal(string, string)              {}
func (noopStats) FeedEventTotal(string, string)              {}

// NoopStats returns a Stats implementation that discards everything.
func NoopStats() Stats { return noopStats{} }

// backoffDelay computes exponential backoff with jitter-free doubling, clamped
// to max. attempt is zero based.
func backoffDelay(attempt int, base, max time.Duration) time.Duration {
	if base <= 0 {
		base = 500 * time.Millisecond
	}
	d := base
	for i := 0; i < attempt && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	return d
}

// sleepCtx waits for d, returning early (with ctx.Err()) on cancellation.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// send delivers a raw trade, honouring cancellation so shutdown is prompt even
// while the pipeline is saturated.
func send(ctx context.Context, out chan<- RawTrade, rt RawTrade) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case out <- rt:
		return nil
	}
}

func lowerSymbols(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		out = append(out, strings.ToLower(strings.TrimSpace(s)))
	}
	return out
}
