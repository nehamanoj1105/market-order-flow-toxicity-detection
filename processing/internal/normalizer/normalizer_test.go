package normalizer

import (
	"testing"
)

func TestNormalize_ValidTrade(t *testing.T) {
	raw := []byte(`{
		"event_id": "abc-123",
		"exchange": "BINANCE",
		"symbol": "btcusdt",
		"price": 68000.50,
		"quantity": 0.015,
		"quote_quantity": 1020.0075,
		"side": "buy",
		"is_buyer_maker": false,
		"trade_time_ms": 1696800000000
	}`)

	out, err := Normalize(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	s := string(out)
	// Symbol should be uppercased
	if !contains(s, `"BTCUSDT"`) {
		t.Error("expected symbol to be uppercased")
	}
	// Side should be uppercased
	if !contains(s, `"BUY"`) {
		t.Error("expected side to be uppercased")
	}
}

func TestNormalize_MissingSymbol(t *testing.T) {
	raw := []byte(`{"price": 100, "quantity": 1, "side": "BUY", "trade_time_ms": 1000}`)
	_, err := Normalize(raw)
	if err == nil {
		t.Error("expected error for missing symbol")
	}
}

func TestNormalize_InvalidSide(t *testing.T) {
	raw := []byte(`{"symbol": "ETHUSDT", "price": 100, "quantity": 1, "side": "HOLD", "trade_time_ms": 1000}`)
	_, err := Normalize(raw)
	if err == nil {
		t.Error("expected error for invalid side")
	}
}

func TestNormalize_ZeroPrice(t *testing.T) {
	raw := []byte(`{"symbol": "ETHUSDT", "price": 0, "quantity": 1, "side": "BUY", "trade_time_ms": 1000}`)
	_, err := Normalize(raw)
	if err == nil {
		t.Error("expected error for zero price")
	}
}

func TestNormalize_ZeroQuantity(t *testing.T) {
	raw := []byte(`{"symbol": "ETHUSDT", "price": 100, "quantity": 0, "side": "BUY", "trade_time_ms": 1000}`)
	_, err := Normalize(raw)
	if err == nil {
		t.Error("expected error for zero quantity")
	}
}

func TestNormalize_MissingEventID(t *testing.T) {
	raw := []byte(`{"symbol": "ETHUSDT", "price": 100, "quantity": 1, "side": "SELL", "trade_time_ms": 1000}`)
	out, err := Normalize(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !contains(string(out), `"event_id"`) {
		t.Error("expected event_id to be generated")
	}
}

func TestNormalize_MalformedJSON(t *testing.T) {
	raw := []byte(`{not json}`)
	_, err := Normalize(raw)
	if err == nil {
		t.Error("expected error for malformed JSON")
	}
}

func contains(s, substr string) bool {
	return len(s) > 0 && len(substr) > 0 && containsHelper(s, substr)
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
