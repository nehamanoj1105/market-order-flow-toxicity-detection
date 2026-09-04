// Package model contains the canonical domain types shared by every stage of the
// ingestion service. The Trade struct defined here is the Go mirror of
// shared/trade_schema.json and MUST be kept in sync with it (model/trade_test.go
// enforces that the required schema properties are all emitted).
package model

import (
	"errors"
	"fmt"
	"math"
	"strings"
)

// SchemaVersion is stamped onto every emitted Trade so that consumers can reject
// payloads produced by an incompatible producer. Bump the major component on any
// breaking change to shared/trade_schema.json.
const SchemaVersion = "1.0.0"

// Source identifies where a record came from.
type Source string

const (
	SourceLive      Source = "live"      // exchange websocket / REST stream
	SourceCSV       Source = "csv"       // historical replay from data/*.csv
	SourceSynthetic Source = "synthetic" // locally generated market simulator
)

// Side is the aggressor (taker) side of a trade. Toxicity scoring is driven by
// the imbalance between BUY and SELL aggressor flow, so getting this right is
// the single most important responsibility of the normalizer.
type Side string

const (
	SideBuy  Side = "BUY"
	SideSell Side = "SELL"
)

// SideFromBuyerMaker converts the raw exchange flag into an aggressor side.
//
// Binance (and most venues) report `isBuyerMaker`:
//   - true  -> the buyer's order was resting on the book (maker), so the seller
//     crossed the spread and lifted it. Aggressor = SELL.
//   - false -> the seller's order was resting, the buyer crossed. Aggressor = BUY.
func SideFromBuyerMaker(isBuyerMaker bool) Side {
	if isBuyerMaker {
		return SideSell
	}
	return SideBuy
}

// Trade is the normalized, canonical trade event published to the message bus.
type Trade struct {
	// EventID is a UUIDv4 assigned by the ingestor (idempotency key downstream).
	EventID string `json:"event_id"`
	// SchemaVersion is the version of shared/trade_schema.json this obeys.
	SchemaVersion string `json:"schema_version"`
	// Source tells consumers whether the record is live, replayed or simulated.
	Source Source `json:"source"`
	// Exchange is the lowercase venue code, e.g. "binance".
	Exchange string `json:"exchange"`
	// Symbol is the uppercase market symbol, e.g. "BTCUSDT".
	Symbol string `json:"symbol"`
	// TradeID is the exchange trade id (unique per exchange+symbol).
	TradeID int64 `json:"trade_id"`
	// Price is the execution price in quote currency.
	Price float64 `json:"price"`
	// Quantity is the executed base-asset amount.
	Quantity float64 `json:"quantity"`
	// QuoteQuantity is price*quantity, pre-computed for the scorer.
	QuoteQuantity float64 `json:"quote_quantity"`
	// Side is the aggressor side: BUY or SELL.
	Side Side `json:"side"`
	// IsBuyerMaker is the raw exchange flag kept for auditability.
	IsBuyerMaker bool `json:"is_buyer_maker"`
	// TradeTimeMS is the exchange match timestamp (authoritative event time).
	TradeTimeMS int64 `json:"trade_time_ms"`
	// EventTimeMS is when the feed handed us the event.
	EventTimeMS int64 `json:"event_time_ms"`
	// IngestedAtMS is when the ingestor normalized the event.
	IngestedAtMS int64 `json:"ingested_at_ms"`
	// Sequence is a monotonic per-process counter used to detect drops.
	Sequence uint64 `json:"sequence"`
	// Host is the ingestor instance that produced the event.
	Host string `json:"ingestion_host"`
}

// Key returns the partition / routing key for the trade. Using exchange:symbol
// keeps every trade of a market inside a single partition, which is what gives
// the scorer per-symbol ordering without paying for global ordering.
func (t Trade) Key() string {
	return t.Exchange + ":" + t.Symbol
}

// Headers returns the message headers attached to the broker record. They let
// consumers filter and route without deserializing the body.
func (t Trade) Headers() map[string]string {
	return map[string]string{
		"schema_version": t.SchemaVersion,
		"event_id":       t.EventID,
		"exchange":       t.Exchange,
		"symbol":         t.Symbol,
		"source":         string(t.Source),
		"side":           string(t.Side),
		"trade_time_ms":  strconvItoa(t.TradeTimeMS),
		"ingested_at_ms": strconvItoa(t.IngestedAtMS),
		"content-type":   "application/json",
	}
}

// Validate reports whether the trade is well formed. Anything failing
// validation is never published; it is counted and (with its raw form) sent to
// the dead letter queue by the caller.
func (t Trade) Validate() error {
	switch {
	case t.EventID == "":
		return errors.New("event_id is required")
	case t.SchemaVersion == "":
		return errors.New("schema_version is required")
	case t.Exchange == "":
		return errors.New("exchange is required")
	case t.Symbol == "":
		return errors.New("symbol is required")
	case t.TradeID < 0:
		return fmt.Errorf("trade_id must be >= 0, got %d", t.TradeID)
	case !isFinitePositive(t.Price):
		return fmt.Errorf("price must be finite and > 0, got %v", t.Price)
	case !isFinitePositive(t.Quantity):
		return fmt.Errorf("quantity must be finite and > 0, got %v", t.Quantity)
	case !isFinitePositive(t.QuoteQuantity):
		return fmt.Errorf("quote_quantity must be finite and > 0, got %v", t.QuoteQuantity)
	case t.Side != SideBuy && t.Side != SideSell:
		return fmt.Errorf("side must be BUY or SELL, got %q", t.Side)
	case t.TradeTimeMS <= 0:
		return fmt.Errorf("trade_time_ms must be > 0, got %d", t.TradeTimeMS)
	case t.EventTimeMS <= 0:
		return fmt.Errorf("event_time_ms must be > 0, got %d", t.EventTimeMS)
	case t.IngestedAtMS <= 0:
		return fmt.Errorf("ingested_at_ms must be > 0, got %d", t.IngestedAtMS)
	}
	return nil
}

// LagMS returns the ingestion lag: how stale the trade was when we emitted it.
func (t Trade) LagMS() int64 {
	if t.IngestedAtMS < t.TradeTimeMS {
		return 0
	}
	return t.IngestedAtMS - t.TradeTimeMS
}

func isFinitePositive(v float64) bool {
	return v > 0 && !math.IsNaN(v) && !math.IsInf(v, 0)
}

// NormalizeSymbol upper-cases and trims a market symbol so that "btcusdt",
// "BTC-USDT" style inputs cannot silently create duplicate series downstream.
func NormalizeSymbol(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ToUpper(s)
	s = strings.NewReplacer("-", "", "/", "", "_", "").Replace(s)
	return s
}

// strconvItoa avoids pulling strconv into the hot path of every header build
// with an allocation-heavy fmt call.
func strconvItoa(v int64) string {
	return fmtInt(v)
}
