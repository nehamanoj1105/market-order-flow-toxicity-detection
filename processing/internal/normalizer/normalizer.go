package normalizer

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// ErrInvalidTrade is returned when a trade message fails validation.
var ErrInvalidTrade = errors.New("invalid trade")

// Trade represents the canonical normalized trade structure that flows
// from the processing stage to the scoring stage.
type Trade struct {
	EventID       string  `json:"event_id"`
	Exchange      string  `json:"exchange"`
	Symbol        string  `json:"symbol"`
	Price         float64 `json:"price"`
	Quantity      float64 `json:"quantity"`
	QuoteQuantity float64 `json:"quote_quantity"`
	Side          string  `json:"side"`
	IsBuyerMaker  bool    `json:"is_buyer_maker"`
	TradeTimeMs   int64   `json:"trade_time_ms"`
}

// Normalize validates and normalizes a raw JSON trade message.
//
// Validation rules:
//   - Must be valid JSON
//   - symbol must be non-empty
//   - side must be "BUY" or "SELL" (case-insensitive, normalized to upper)
//   - price must be > 0
//   - quantity must be > 0
//   - trade_time_ms must be > 0
//
// Normalization:
//   - symbol is uppercased
//   - side is uppercased
//   - event_id is generated if missing
func Normalize(raw []byte) ([]byte, error) {
	var trade map[string]interface{}
	if err := json.Unmarshal(raw, &trade); err != nil {
		return nil, fmt.Errorf("%w: malformed JSON: %v", ErrInvalidTrade, err)
	}

	// --- Symbol ---
	symbol, _ := trade["symbol"].(string)
	symbol = strings.TrimSpace(symbol)
	if symbol == "" {
		return nil, fmt.Errorf("%w: missing or empty symbol", ErrInvalidTrade)
	}
	trade["symbol"] = strings.ToUpper(symbol)

	// --- Side ---
	side, _ := trade["side"].(string)
	side = strings.ToUpper(strings.TrimSpace(side))
	if side != "BUY" && side != "SELL" {
		return nil, fmt.Errorf("%w: invalid side %q", ErrInvalidTrade, side)
	}
	trade["side"] = side

	// --- Price ---
	price, ok := toFloat64(trade["price"])
	if !ok || price <= 0 {
		return nil, fmt.Errorf("%w: invalid price", ErrInvalidTrade)
	}
	trade["price"] = price

	// --- Quantity ---
	qty, ok := toFloat64(trade["quantity"])
	if !ok || qty <= 0 {
		return nil, fmt.Errorf("%w: invalid quantity", ErrInvalidTrade)
	}
	trade["quantity"] = qty

	// --- TradeTimeMs ---
	tradeTime, ok := toInt64(trade["trade_time_ms"])
	if !ok || tradeTime <= 0 {
		return nil, fmt.Errorf("%w: invalid trade_time_ms", ErrInvalidTrade)
	}
	trade["trade_time_ms"] = tradeTime

	// --- EventID: ensure one exists ---
	eventID, _ := trade["event_id"].(string)
	if strings.TrimSpace(eventID) == "" {
		trade["event_id"] = uuid.New().String()
	}

	return json.Marshal(trade)
}

// toFloat64 attempts to convert a JSON-decoded value to float64.
func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

// toInt64 attempts to convert a JSON-decoded value to int64.
func toInt64(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	default:
		return 0, false
	}
}
