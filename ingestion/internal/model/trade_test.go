package model

import (
	"encoding/json"
	"math"
	"os"
	"strings"
	"testing"
)

func TestSideFromBuyerMaker(t *testing.T) {
	// isBuyerMaker == true  => the buyer was resting => the seller crossed => SELL
	if got := SideFromBuyerMaker(true); got != SideSell {
		t.Errorf("SideFromBuyerMaker(true) = %q, want SELL", got)
	}
	if got := SideFromBuyerMaker(false); got != SideBuy {
		t.Errorf("SideFromBuyerMaker(false) = %q, want BUY", got)
	}
}

func TestNormalizeSymbol(t *testing.T) {
	cases := map[string]string{
		"btcusdt":   "BTCUSDT",
		" BTC-USDT": "BTCUSDT",
		"eth/usdt":  "ETHUSDT",
		"sol_usdt":  "SOLUSDT",
	}
	for in, want := range cases {
		if got := NormalizeSymbol(in); got != want {
			t.Errorf("NormalizeSymbol(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestTradeValidate(t *testing.T) {
	base := func() Trade {
		return Trade{
			EventID:       "5f2b0d3e-4b1a-4d1e-9c2a-0f1e2d3c4b5a",
			SchemaVersion: SchemaVersion,
			Source:        SourceCSV,
			Exchange:      "binance",
			Symbol:        "BTCUSDT",
			TradeID:       1,
			Price:         100.5,
			Quantity:      2,
			QuoteQuantity: 201,
			Side:          SideBuy,
			TradeTimeMS:   1704153600000,
			EventTimeMS:   1704153600000,
			IngestedAtMS:  1704153600001,
		}
	}

	if err := base().Validate(); err != nil {
		t.Fatalf("valid trade rejected: %v", err)
	}

	mutations := map[string]func(*Trade){
		"missing event id":    func(tr *Trade) { tr.EventID = "" },
		"missing symbol":      func(tr *Trade) { tr.Symbol = "" },
		"missing exchange":    func(tr *Trade) { tr.Exchange = "" },
		"zero price":          func(tr *Trade) { tr.Price = 0 },
		"negative quantity":   func(tr *Trade) { tr.Quantity = -1 },
		"nan price":           func(tr *Trade) { tr.Price = math.NaN() },
		"bad side":            func(tr *Trade) { tr.Side = "HOLD" },
		"zero trade time":     func(tr *Trade) { tr.TradeTimeMS = 0 },
		"negative trade id":   func(tr *Trade) { tr.TradeID = -3 },
		"missing schema vers": func(tr *Trade) { tr.SchemaVersion = "" },
	}
	for name, mutate := range mutations {
		tr := base()
		mutate(&tr)
		if err := tr.Validate(); err == nil {
			t.Errorf("%s: expected validation failure", name)
		}
	}
}

func TestTradeKeyAndHeaders(t *testing.T) {
	tr := Trade{Exchange: "binance", Symbol: "BTCUSDT", Side: SideBuy, TradeTimeMS: 1704153600000, IngestedAtMS: 1704153600005}
	if got, want := tr.Key(), "binance:BTCUSDT"; got != want {
		t.Errorf("Key() = %q, want %q", got, want)
	}
	h := tr.Headers()
	for _, k := range []string{"schema_version", "event_id", "exchange", "symbol", "source", "side", "trade_time_ms", "ingested_at_ms", "content-type"} {
		if _, ok := h[k]; !ok {
			t.Errorf("Headers() missing %q", k)
		}
	}
	if h["content-type"] != "application/json" {
		t.Errorf("unexpected content type header %q", h["content-type"])
	}
	if h["trade_time_ms"] != "1704153600000" {
		t.Errorf("trade_time_ms header = %q", h["trade_time_ms"])
	}
}

func TestTradeLagMS(t *testing.T) {
	tr := Trade{TradeTimeMS: 1000, IngestedAtMS: 1250}
	if got := tr.LagMS(); got != 250 {
		t.Errorf("LagMS() = %d, want 250", got)
	}
	// Clock skew must never produce a negative lag.
	tr.IngestedAtMS = 900
	if got := tr.LagMS(); got != 0 {
		t.Errorf("LagMS() with clock skew = %d, want 0", got)
	}
}

// TestTradeMatchesSharedSchema is the cross-language contract test: it reads
// shared/trade_schema.json and asserts the Go struct emits every required
// property, with a JSON type compatible with the schema.
func TestTradeMatchesSharedSchema(t *testing.T) {
	raw, err := os.ReadFile("../../../shared/trade_schema.json")
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	var schema struct {
		Required   []string `json:"required"`
		Properties map[string]struct {
			Type string `json:"type"`
			Enum []any  `json:"enum"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatalf("parse schema: %v", err)
	}
	if len(schema.Required) == 0 {
		t.Fatal("schema declares no required properties")
	}

	tr := Trade{
		EventID:       "5f2b0d3e-4b1a-4d1e-9c2a-0f1e2d3c4b5a",
		SchemaVersion: SchemaVersion,
		Source:        SourceLive,
		Exchange:      "binance",
		Symbol:        "BTCUSDT",
		TradeID:       42,
		Price:         65000.5,
		Quantity:      0.25,
		QuoteQuantity: 16250.125,
		Side:          SideSell,
		IsBuyerMaker:  true,
		TradeTimeMS:   1704153600000,
		EventTimeMS:   1704153600001,
		IngestedAtMS:  1704153600002,
		Sequence:      7,
		Host:          "ingestor-0",
	}
	body, err := json.Marshal(tr)
	if err != nil {
		t.Fatalf("marshal trade: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("unmarshal trade: %v", err)
	}

	for _, prop := range schema.Required {
		v, ok := decoded[prop]
		if !ok {
			t.Errorf("schema requires %q but the Go struct does not emit it", prop)
			continue
		}
		spec, ok := schema.Properties[prop]
		if !ok {
			continue
		}
		switch spec.Type {
		case "string":
			if _, ok := v.(string); !ok {
				t.Errorf("property %q must be a string, got %T", prop, v)
			}
		case "number":
			if _, ok := v.(float64); !ok {
				t.Errorf("property %q must be a number, got %T", prop, v)
			}
		case "integer":
			f, ok := v.(float64)
			if !ok || f != float64(int64(f)) {
				t.Errorf("property %q must be an integer, got %v", prop, v)
			}
		case "boolean":
			if _, ok := v.(bool); !ok {
				t.Errorf("property %q must be a boolean, got %T", prop, v)
			}
		}
		if len(spec.Enum) > 0 {
			allowed := false
			for _, e := range spec.Enum {
				if e == v {
					allowed = true
					break
				}
			}
			if !allowed {
				t.Errorf("property %q value %v is not in enum %v", prop, v, spec.Enum)
			}
		}
	}

	if got := decoded["schema_version"]; got != SchemaVersion {
		t.Errorf("schema_version = %v, want %v", got, SchemaVersion)
	}
	if strings.TrimSpace(string(body)) == "" {
		t.Fatal("marshalled trade is empty")
	}
}
