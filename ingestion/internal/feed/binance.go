package feed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/config"
	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/model"
)

// BinanceFeed streams live trades from the public Binance websocket.
//
// Endpoint: <WSURL>/stream?streams=<symbol>@aggTrade/<symbol>@trade/...
//
// It never authenticates (market data is public) and it reconnects with
// exponential backoff, emitting a FeedEventTotal("reconnect") metric each time
// so the dashboard can surface feed instability.
type BinanceFeed struct {
	cfg   config.Config
	log   *slog.Logger
	stats Stats
	http  *http.Client
}

// NewBinanceFeed builds a live feed.
func NewBinanceFeed(cfg config.Config, log *slog.Logger, stats Stats) *BinanceFeed {
	return &BinanceFeed{
		cfg:   cfg,
		log:   log,
		stats: stats,
		http:  &http.Client{Timeout: 15 * time.Second},
	}
}

func (f *BinanceFeed) Name() string { return "binance-websocket" }

// Run connects, streams, and reconnects until ctx is cancelled.
func (f *BinanceFeed) Run(ctx context.Context, out chan<- RawTrade) error {
	kind := f.cfg.Binance.StreamKind
	if kind != "aggtrade" && kind != "trade" {
		return fmt.Errorf("BINANCE_STREAM must be aggTrade or trade, got %q", f.cfg.Binance.StreamKind)
	}

	symbols := lowerSymbols(f.cfg.Feed.Symbols)
	streams := make([]string, 0, len(symbols))
	for _, s := range symbols {
		streams = append(streams, s+"@"+kind)
	}
	endpoint := strings.TrimRight(f.cfg.Binance.WSURL, "/") + "/stream?streams=" + strings.Join(streams, "/")

	if f.cfg.Binance.Backfill {
		for _, s := range symbols {
			n, err := f.backfill(ctx, s, out)
			if err != nil {
				// Backfill is best effort: a REST hiccup must not stop ingestion.
				f.log.Warn("rest backfill failed, continuing with live stream", "symbol", s, "error", err)
				f.stats.FeedErrorTotal(f.Name(), "backfill")
				break
			}
			f.log.Info("rest backfill complete", "symbol", s, "trades", n)
		}
	}

	for attempt := 0; ; attempt++ {
		if max := f.cfg.Binance.MaxReconnects; max > 0 && attempt >= max {
			return fmt.Errorf("giving up after %d websocket reconnect attempts", attempt)
		}
		if attempt > 0 {
			f.stats.FeedEventTotal(f.Name(), "reconnect")
		}
		err := f.consume(ctx, endpoint, out)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		delay := backoffDelay(attempt, f.cfg.Binance.ReconnectMin, f.cfg.Binance.ReconnectMax)
		f.log.Warn("websocket disconnected, reconnecting",
			"attempt", attempt, "delay", delay, "error", err)
		f.stats.FeedErrorTotal(f.Name(), "disconnect")
		if err := sleepCtx(ctx, delay); err != nil {
			return err
		}
	}
}

// consume holds one websocket connection open until it breaks.
func (f *BinanceFeed) consume(ctx context.Context, endpoint string, out chan<- RawTrade) error {
	dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(dialCtx, endpoint, &websocket.DialOptions{
		HTTPClient: f.http,
		HTTPHeader: http.Header{"User-Agent": []string{f.cfg.Service.Name + "/1.0"}},
	})
	if err != nil {
		if resp != nil && resp.Body != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		return fmt.Errorf("dial %s: %w", endpoint, err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "shutdown")
	conn.SetReadLimit(1 << 22) // 4 MiB frame cap: cheap protection against a hostile feed

	f.log.Info("websocket connected", "endpoint", endpoint)

	// A failed ping cancels reads, which forces a reconnect.
	readCtx, stopReads := context.WithCancel(ctx)
	defer stopReads()
	var stopOnce sync.Once
	go f.keepAlive(readCtx, conn, func() { stopOnce.Do(stopReads) })

	for {
		rc, rcCancel := context.WithTimeout(readCtx, f.cfg.Binance.ReadTimeout)
		_, frame, err := conn.Read(rc)
		rcCancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		// The frame buffer is reused by the library on the next Read, and we may
		// keep it around for dead lettering, so copy it.
		payload := append([]byte(nil), frame...)
		rt, err := decodeStreamMessage(payload)
		if err != nil {
			f.log.Warn("dropping undecodable frame", "error", err)
			f.stats.FeedErrorTotal(f.Name(), "decode")
			continue
		}
		rt.Payload = payload
		rt.Exchange = f.cfg.Feed.Exchange
		if err := send(ctx, out, rt); err != nil {
			return err
		}
		f.stats.FeedReadTotal(rt.Exchange, rt.Symbol, model.SourceLive)
	}
}

func (f *BinanceFeed) keepAlive(ctx context.Context, conn *websocket.Conn, onFailure func()) {
	interval := f.cfg.Binance.PingInterval
	if interval <= 0 {
		interval = 20 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := conn.Ping(pingCtx)
			cancel()
			if err != nil {
				if ctx.Err() == nil {
					f.log.Warn("ping failed", "error", err)
					onFailure()
				}
				return
			}
			f.stats.FeedEventTotal(f.Name(), "ping")
		}
	}
}

// backfill pulls the most recent aggregate trades over REST so that a restart
// does not leave a hole in the series the scorer is measuring.
func (f *BinanceFeed) backfill(ctx context.Context, symbol string, out chan<- RawTrade) (int, error) {
	limit := f.cfg.Binance.BackfillLimit
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	endpoint := fmt.Sprintf("%s/api/v3/aggTrades?symbol=%s&limit=%d",
		strings.TrimRight(f.cfg.Binance.RESTURL, "/"), strings.ToUpper(symbol), limit)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, err
	}
	resp, err := f.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return 0, fmt.Errorf("backfill http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var rows []aggTradeREST
	if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&rows); err != nil {
		return 0, fmt.Errorf("decode backfill: %w", err)
	}

	sent := 0
	for _, r := range rows {
		rt := RawTrade{
			Exchange:     f.cfg.Feed.Exchange,
			Symbol:       model.NormalizeSymbol(r.Symbol),
			Source:       model.SourceLive,
			TradeID:      r.AggTradeID,
			Price:        r.Price,
			Quantity:     r.Quantity,
			IsBuyerMaker: r.IsBuyerMaker,
			TradeTimeMS:  r.Timestamp,
			EventTimeMS:  time.Now().UnixMilli(),
		}
		if rt.Symbol == "" {
			rt.Symbol = model.NormalizeSymbol(symbol)
		}
		if err := send(ctx, out, rt); err != nil {
			return sent, err
		}
		sent++
		f.stats.FeedReadTotal(rt.Exchange, rt.Symbol, model.SourceLive)
	}
	return sent, nil
}

// ---------- wire types ----------

type combinedStream struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

type aggTradeStream struct {
	Event        string `json:"e"`
	EventTimeMS  int64  `json:"E"`
	Symbol       string `json:"s"`
	AggTradeID   int64  `json:"a"`
	Price        string `json:"p"`
	Quantity     string `json:"q"`
	FirstTradeID int64  `json:"f"`
	LastTradeID  int64  `json:"l"`
	TradeTimeMS  int64  `json:"T"`
	IsBuyerMaker bool   `json:"m"`
}

type tradeStream struct {
	Event        string `json:"e"`
	EventTimeMS  int64  `json:"E"`
	Symbol       string `json:"s"`
	TradeID      int64  `json:"t"`
	Price        string `json:"p"`
	Quantity     string `json:"q"`
	TradeTimeMS  int64  `json:"T"`
	IsBuyerMaker bool   `json:"m"`
}

type aggTradeREST struct {
	AggTradeID   int64  `json:"a"`
	Price        string `json:"p"`
	Quantity     string `json:"q"`
	FirstTradeID int64  `json:"f"`
	LastTradeID  int64  `json:"l"`
	Timestamp    int64  `json:"T"`
	IsBuyerMaker bool   `json:"m"`
	Symbol       string `json:"-"`
}

// decodeStreamMessage converts a combined-stream frame into a RawTrade.
func decodeStreamMessage(payload []byte) (RawTrade, error) {
	var env combinedStream
	if err := json.Unmarshal(payload, &env); err != nil {
		return RawTrade{}, err
	}
	data := env.Data
	if len(data) == 0 {
		data = payload // single (non-combined) stream payload
	}

	var event string
	var probe struct {
		Event string `json:"e"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return RawTrade{}, err
	}
	event = probe.Event

	switch event {
	case "aggTrade":
		var a aggTradeStream
		if err := json.Unmarshal(data, &a); err != nil {
			return RawTrade{}, err
		}
		now := time.Now().UnixMilli()
		return RawTrade{
			Symbol:       model.NormalizeSymbol(a.Symbol),
			Source:       model.SourceLive,
			TradeID:      a.AggTradeID,
			Price:        a.Price,
			Quantity:     a.Quantity,
			IsBuyerMaker: a.IsBuyerMaker,
			TradeTimeMS:  a.TradeTimeMS,
			EventTimeMS:  orNow(a.EventTimeMS, now),
		}, nil
	case "trade":
		var t tradeStream
		if err := json.Unmarshal(data, &t); err != nil {
			return RawTrade{}, err
		}
		now := time.Now().UnixMilli()
		return RawTrade{
			Symbol:       model.NormalizeSymbol(t.Symbol),
			Source:       model.SourceLive,
			TradeID:      t.TradeID,
			Price:        t.Price,
			Quantity:     t.Quantity,
			IsBuyerMaker: t.IsBuyerMaker,
			TradeTimeMS:  t.TradeTimeMS,
			EventTimeMS:  orNow(t.EventTimeMS, now),
		}, nil
	default:
		return RawTrade{}, fmt.Errorf("unsupported stream event %q", event)
	}
}

func orNow(v, fallback int64) int64 {
	if v <= 0 {
		return fallback
	}
	return v
}

// ParseAggTradeJSON is exported for tests and for the optional REST poller; it
// decodes a raw aggTrade frame payload into a RawTrade.
func ParseAggTradeJSON(payload []byte) (RawTrade, error) {
	rt, err := decodeStreamMessage(payload)
	if err != nil {
		return RawTrade{}, err
	}
	if rt.TradeID == 0 || rt.Price == "" || rt.TradeTimeMS == 0 {
		return RawTrade{}, errors.New("incomplete aggTrade payload")
	}
	return rt, nil
}
