// Package normalize converts raw feed records into the canonical model.Trade
// defined by shared/trade_schema.json.
//
// Responsibilities (mirrors the RawTrade/Trade CRC cards):
//   - parse the exchange decimal text exactly once,
//   - derive the aggressor (taker) side from the isBuyerMaker flag,
//   - stamp identity (UUIDv4), provenance (source, host) and ordering
//     (monotonic sequence) onto the event,
//   - reject anything that would poison downstream statistics.
//
// The normalizer is stateless apart from its sequence counter and clock, so a
// single instance can be shared by every pipeline worker goroutine.
package normalize

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/config"
	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/feed"
	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/model"
)

// Reason values are stable strings: they are used as metric labels and in log
// search, so treat them as API.
const (
	ReasonEmptySymbol   = "empty_symbol"
	ReasonBadPrice      = "bad_price"
	ReasonBadQuantity   = "bad_quantity"
	ReasonBadQuoteQty   = "bad_quote_quantity"
	ReasonBadTimestamp  = "bad_timestamp"
	ReasonPriceRange    = "price_out_of_range"
	ReasonQuantityRange = "quantity_out_of_range"
	ReasonStale         = "stale_trade"
	ReasonInvalid       = "invalid_trade"
)

// Stats is the metrics surface used by the normalizer.
type Stats interface {
	NormalizedTotal(exchange, symbol string)
	NormalizeErrorTotal(exchange, symbol, reason string)
	DuplicateTotal(exchange, symbol string)
	IngestLagMilliseconds(exchange, symbol string, ms float64)
}

// Error describes a record that could not be normalized. It keeps the offending
// raw payload so the caller can dead letter it instead of silently dropping it.
type Error struct {
	Reason string
	Detail string
	Raw    feed.RawTrade
}

func (e *Error) Error() string {
	return fmt.Sprintf("normalize: %s: %s (symbol=%s trade_id=%d)", e.Reason, e.Detail, e.Raw.Symbol, e.Raw.TradeID)
}

// Normalizer turns feed.RawTrade into model.Trade.
type Normalizer struct {
	cfg   config.NormalizeConfig
	host  string
	seq   atomic.Uint64
	now   func() time.Time
	stats Stats
	log   *slog.Logger
}

// New builds a Normalizer. host is stamped on every event; pass "" to use the
// machine hostname automatically.
func New(cfg config.NormalizeConfig, host string, stats Stats, log *slog.Logger) *Normalizer {
	if host == "" {
		host, _ = os.Hostname()
	}
	if cfg.SchemaVersion == "" {
		cfg.SchemaVersion = model.SchemaVersion
	}
	if log == nil {
		log = slog.Default()
	}
	return &Normalizer{cfg: cfg, host: host, now: time.Now, stats: stats, log: log}
}

// Normalize converts one raw record. On failure it returns an *Error whose Raw
// field carries the original record for dead lettering.
func (n *Normalizer) Normalize(raw feed.RawTrade) (model.Trade, error) {
	symbol := model.NormalizeSymbol(raw.Symbol)
	if symbol == "" {
		return model.Trade{}, n.fail(raw, ReasonEmptySymbol, "symbol missing")
	}
	exchange := strings.ToLower(strings.TrimSpace(raw.Exchange))
	if exchange == "" {
		exchange = "unknown"
	}

	price, err := parseFloat(raw.Price)
	if err != nil || price <= 0 {
		return model.Trade{}, n.fail(raw, ReasonBadPrice, fmt.Sprintf("price=%q", raw.Price))
	}
	qty, err := parseFloat(raw.Quantity)
	if err != nil || qty <= 0 {
		return model.Trade{}, n.fail(raw, ReasonBadQuantity, fmt.Sprintf("quantity=%q", raw.Quantity))
	}

	quote := price * qty
	if raw.QuoteQuantity != "" {
		if q, err := parseFloat(raw.QuoteQuantity); err == nil && q > 0 {
			quote = q
		} else if !n.cfg.ComputeQuoteQty {
			return model.Trade{}, n.fail(raw, ReasonBadQuoteQty, fmt.Sprintf("quote_quantity=%q", raw.QuoteQuantity))
		}
	}

	if n.cfg.MaxPrice > 0 && price > n.cfg.MaxPrice {
		return model.Trade{}, n.fail(raw, ReasonPriceRange, fmt.Sprintf("price %v > %v", price, n.cfg.MaxPrice))
	}
	if n.cfg.MaxQuantity > 0 && qty > n.cfg.MaxQuantity {
		return model.Trade{}, n.fail(raw, ReasonQuantityRange, fmt.Sprintf("quantity %v > %v", qty, n.cfg.MaxQuantity))
	}

	tradeTime := raw.TradeTimeMS
	if tradeTime <= 0 {
		tradeTime = raw.EventTimeMS
	}
	if tradeTime <= 0 {
		return model.Trade{}, n.fail(raw, ReasonBadTimestamp, fmt.Sprintf("trade_time=%d event_time=%d", raw.TradeTimeMS, raw.EventTimeMS))
	}
	eventTime := raw.EventTimeMS
	if eventTime <= 0 {
		eventTime = tradeTime
	}

	nowMS := n.now().UnixMilli()
	if n.cfg.MaxAge > 0 && nowMS-tradeTime > n.cfg.MaxAge.Milliseconds() {
		return model.Trade{}, n.fail(raw, ReasonStale, fmt.Sprintf("trade is %d ms old", nowMS-tradeTime))
	}

	t := model.Trade{
		EventID:       uuid.NewString(),
		SchemaVersion: n.cfg.SchemaVersion,
		Source:        raw.Source,
		Exchange:      exchange,
		Symbol:        symbol,
		TradeID:       raw.TradeID,
		Price:         price,
		Quantity:      qty,
		QuoteQuantity: quote,
		Side:          model.SideFromBuyerMaker(raw.IsBuyerMaker),
		IsBuyerMaker:  raw.IsBuyerMaker,
		TradeTimeMS:   tradeTime,
		EventTimeMS:   eventTime,
		IngestedAtMS:  nowMS,
		Sequence:      n.seq.Add(1),
		Host:          n.host,
	}

	if err := t.Validate(); err != nil {
		return model.Trade{}, n.fail(raw, ReasonInvalid, err.Error())
	}

	if n.stats != nil {
		n.stats.NormalizedTotal(t.Exchange, t.Symbol)
		n.stats.IngestLagMilliseconds(t.Exchange, t.Symbol, float64(t.LagMS()))
	}
	return t, nil
}

// NormalizeAll converts a slice, returning the good trades and the failures.
// It never returns early: a single bad record must not cost the whole batch.
func (n *Normalizer) NormalizeAll(raws []feed.RawTrade) ([]model.Trade, []*Error) {
	out := make([]model.Trade, 0, len(raws))
	var errs []*Error
	for _, raw := range raws {
		t, err := n.Normalize(raw)
		if err != nil {
			var ne *Error
			if ok := asNormalizeError(err, &ne); ok {
				errs = append(errs, ne)
			}
			continue
		}
		out = append(out, t)
	}
	return out, errs
}

// Sequence reports the number of events normalized so far.
func (n *Normalizer) Sequence() uint64 { return n.seq.Load() }

func (n *Normalizer) fail(raw feed.RawTrade, reason, detail string) *Error {
	if n.stats != nil {
		n.stats.NormalizeErrorTotal(strings.ToLower(raw.Exchange), model.NormalizeSymbol(raw.Symbol), reason)
	}
	return &Error{Reason: reason, Detail: detail, Raw: raw}
}

func asNormalizeError(err error, target **Error) bool {
	if e, ok := err.(*Error); ok { //nolint:errorlint // hot path, no wrapping involved
		*target = e
		return true
	}
	return false
}

// parseFloat is a strict wrapper over strconv: it rejects NaN/Inf and the
// empty string, which strconv happily accepts in some forms.
func parseFloat(s string) (float64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty number")
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}
	return v, nil
}
