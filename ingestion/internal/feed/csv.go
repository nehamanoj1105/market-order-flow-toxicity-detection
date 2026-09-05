package feed

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/config"
	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/model"
)

// CSVFeed replays a historical trade dump at a controllable speed.
//
// It is the workhorse for local development, integration tests and demos
// because it is deterministic, offline and needs no exchange credentials.
type CSVFeed struct {
	cfg   config.Config
	log   *slog.Logger
	stats Stats
}

// NewCSVFeed builds a replay feed from the CSV section of the config.
func NewCSVFeed(cfg config.Config, log *slog.Logger, stats Stats) *CSVFeed {
	return &CSVFeed{cfg: cfg, log: log, stats: stats}
}

func (f *CSVFeed) Name() string { return "csv-replay" }

// Run streams the file (optionally looping) until ctx is cancelled or the file
// is exhausted. Row pacing reproduces the original inter-arrival times divided
// by CSV_SPEED, so CSV_SPEED=100 replays a day of trades in ~14 minutes while
// CSV_SPEED=0 replays as fast as the pipeline can drain.
func (f *CSVFeed) Run(ctx context.Context, out chan<- RawTrade) error {
	path := f.cfg.CSV.Path
	if path == "" {
		return errors.New("CSV_PATH is empty")
	}
	symbol := f.resolveSymbol(path)
	format := f.cfg.CSV.Format
	emittedTotal := 0

	f.log.Info("starting csv replay",
		"path", path, "symbol", symbol, "format", format,
		"speed", f.cfg.CSV.Speed, "loop", f.cfg.CSV.Loop)

	maxRows := f.cfg.CSV.MaxRows
	for pass := 0; ; pass++ {
		remaining := -1 // negative means unlimited
		if maxRows > 0 {
			remaining = maxRows - emittedTotal
			if remaining <= 0 {
				return nil
			}
		}
		count, err := f.runPass(ctx, out, path, symbol, remaining)
		emittedTotal += count
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		f.log.Info("csv replay pass complete", "pass", pass, "rows", count)
		if !f.cfg.CSV.Loop {
			return nil
		}
		// Small pause between passes so a looping single-row file cannot spin.
		if err := sleepCtx(ctx, 250*time.Millisecond); err != nil {
			return err
		}
	}
}

func (f *CSVFeed) runPass(ctx context.Context, out chan<- RawTrade, path, symbol string, remaining int) (int, error) {
	fh, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open csv: %w", err)
	}
	defer fh.Close()

	r := csv.NewReader(fh)
	r.FieldsPerRecord = -1 // files vary between 6 and 7 columns
	r.LazyQuotes = true
	r.ReuseRecord = true

	layout, err := f.detectLayout(fh, path)
	if err != nil {
		return 0, err
	}
	if layout.HasHeader {
		if _, err := r.Read(); err != nil { // consume header
			return 0, fmt.Errorf("read csv header: %w", err)
		}
	}

	var (
		emitted  int
		lineNo   = layout.DataStartLine
		prevTime int64
	)
	for {
		if remaining >= 0 && emitted >= remaining {
			return emitted, nil
		}
		record, err := r.Read()
		if err == io.EOF {
			return emitted, nil
		}
		if err != nil {
			// A single malformed line must not kill the replay.
			f.log.Warn("skipping malformed csv line", "line", lineNo, "error", err)
			f.stats.FeedErrorTotal(f.Name(), "malformed_line")
			lineNo++
			continue
		}
		lineNo++

		if lineNo-1 < f.cfg.CSV.StartRow {
			continue
		}

		rt, err := layout.Parse(record, f.cfg.Feed.Exchange, symbol)
		if err != nil {
			f.log.Warn("skipping unusable csv row", "line", lineNo, "error", err)
			f.stats.FeedErrorTotal(f.Name(), "unparseable_row")
			continue
		}
		rt.Payload = []byte(strings.Join(append([]string{}, record...), ","))

		if err := f.pace(ctx, rt.TradeTimeMS, &prevTime); err != nil {
			return emitted, err
		}
		if err := send(ctx, out, rt); err != nil {
			return emitted, err
		}
		emitted++
		f.stats.FeedReadTotal(f.cfg.Feed.Exchange, symbol, model.SourceCSV)
	}
}

// pace sleeps for the (scaled) gap between this trade and the previous one.
func (f *CSVFeed) pace(ctx context.Context, now int64, prev *int64) error {
	if *prev == 0 {
		*prev = now
		return ctx.Err()
	}
	deltaMs := now - *prev
	*prev = now
	if deltaMs <= 0 {
		return ctx.Err() // out-of-order or duplicate timestamps: emit immediately
	}
	speed := f.cfg.CSV.Speed
	if speed <= 0 {
		return ctx.Err() // speed 0 == as fast as possible
	}
	// Cap the sleep so long quiet periods do not stall a demo for minutes.
	const maxSleep = 2 * time.Second
	d := time.Duration(float64(deltaMs)/speed) * time.Millisecond
	if d > maxSleep {
		d = maxSleep
	}
	return sleepCtx(ctx, d)
}

// resolveSymbol picks the symbol for a dump. A file named like
// "BTCUSDT-trades-2024-01-01.csv" (the data.binance.vision convention) wins,
// otherwise the first configured symbol is used.
func (f *CSVFeed) resolveSymbol(path string) string {
	head := strings.ToUpper(filepath.Base(path))
	for _, sep := range []string{"-", "_", "."} {
		idx := strings.Index(head, sep)
		if idx <= 3 {
			continue
		}
		candidate := model.NormalizeSymbol(head[:idx])
		for _, known := range f.cfg.Feed.Symbols {
			if candidate == model.NormalizeSymbol(known) {
				return candidate
			}
		}
		if looksLikeSymbol(candidate) {
			return candidate
		}
	}
	if len(f.cfg.Feed.Symbols) == 0 {
		return "BTCUSDT"
	}
	return model.NormalizeSymbol(f.cfg.Feed.Symbols[0])
}

// quoteAssets are the suffixes that make a token unambiguously a market symbol,
// e.g. "BTCUSDT" matches but "SAMPLE" (from sample_binance_trades.csv) does not.
var quoteAssets = []string{
	"USDT", "USDC", "BUSD", "FDUSD", "TUSD", "DAI", "EUR", "GBP", "AUD",
	"TRY", "BRL", "JPY", "RUB", "NGN", "ZAR", "PLN", "CZK", "UAH",
	"BTC", "ETH", "BNB", "XRP", "SOL", "PAX", "USDP", "VAI", "BIDR", "BVND",
}

func looksLikeSymbol(s string) bool {
	if len(s) < 5 || len(s) > 14 {
		return false
	}
	for _, r := range s {
		if (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
			return false
		}
	}
	for _, q := range quoteAssets {
		if strings.HasSuffix(s, q) && len(s) > len(q) {
			return true
		}
	}
	return false
}

// ---------- column layouts ----------

// layout knows how to turn a CSV record into a RawTrade.
type layout struct {
	Format        config.CSVFormat
	HasHeader     bool
	DataStartLine int
	Parse         func(record []string, exchange, symbol string) (RawTrade, error)
}

// detectLayout peeks at the first line to decide whether a header is present
// and which column order the file uses.
func (f *CSVFeed) detectLayout(fh io.ReadSeeker, path string) (layout, error) {
	// Peek the first line.
	var buf [4096]byte
	n, err := fh.Read(buf[:])
	if err != nil && err != io.EOF {
		return layout{}, fmt.Errorf("peek csv: %w", err)
	}
	if _, err := fh.Seek(0, io.SeekStart); err != nil {
		return layout{}, fmt.Errorf("rewind csv: %w", err)
	}
	firstLine := string(buf[:n])
	if i := strings.IndexAny(firstLine, "\r\n"); i >= 0 {
		firstLine = firstLine[:i]
	}

	hasHeader := !headerLooksNumeric(firstLine)
	switch f.cfg.CSV.HasHeader {
	case "true":
		hasHeader = true
	case "false":
		hasHeader = false
	}

	if !hasHeader {
		return layout{
			Format:        config.CSVFormatTrades,
			HasHeader:     false,
			DataStartLine: 0,
			Parse:         parseTradesPositional(columnCount(firstLine)),
		}, nil
	}

	cols := splitCSVLine(firstLine)
	format := f.cfg.CSV.Format
	if format == config.CSVFormatAuto {
		format = inferFormat(cols)
	}
	idx := buildIndex(cols)

	var parse func([]string, string, string) (RawTrade, error)
	switch format {
	case config.CSVFormatAggTrades:
		parse = parseAggTrades(idx)
	default:
		parse = parseTrades(idx)
	}
	return layout{
		Format:        format,
		HasHeader:     true,
		DataStartLine: 1,
		Parse:         parse,
	}, nil
}

func columnCount(line string) int { return len(splitCSVLine(line)) }

func splitCSVLine(line string) []string {
	fields := strings.Split(line, ",")
	for i := range fields {
		fields[i] = strings.Trim(strings.TrimSpace(fields[i]), `"`)
	}
	return fields
}

func headerLooksNumeric(line string) bool {
	first := splitCSVLine(line)
	if len(first) == 0 {
		return false
	}
	_, err := strconv.ParseFloat(first[0], 64)
	return err == nil
}

func inferFormat(cols []string) config.CSVFormat {
	joined := strings.ToLower(strings.Join(cols, ","))
	switch {
	case strings.Contains(joined, "agg_trade_id"), strings.Contains(joined, "aggtradeid"):
		return config.CSVFormatAggTrades
	case strings.Contains(joined, "quoteqty"), strings.Contains(joined, "quote_qty"):
		return config.CSVFormatTrades
	default:
		return config.CSVFormatTrades
	}
}

func buildIndex(cols []string) map[string]int {
	idx := make(map[string]int, len(cols))
	for i, c := range cols {
		key := strings.ToLower(strings.NewReplacer("_", "", "-", "", ".", "", " ", "").Replace(strings.TrimSpace(c)))
		idx[key] = i
	}
	return idx
}

// field looks a column up under several common spellings.
func field(rec []string, idx map[string]int, names ...string) (string, bool) {
	for _, n := range names {
		if i, ok := idx[n]; ok && i < len(rec) {
			return strings.TrimSpace(rec[i]), true
		}
	}
	return "", false
}

// parseTrades handles data.binance.vision daily dumps:
// id,price,qty,quoteQty,time,isBuyerMaker,isBestMatch
func parseTrades(idx map[string]int) func([]string, string, string) (RawTrade, error) {
	return func(rec []string, exchange, symbol string) (RawTrade, error) {
		idStr, ok := field(rec, idx, "id", "tradeid", "trade_id", "aggtradeid", "agg_trade_id", "a")
		if !ok {
			return RawTrade{}, errors.New("missing id column")
		}
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			return RawTrade{}, fmt.Errorf("bad id %q", idStr)
		}
		price, ok := field(rec, idx, "price", "p")
		if !ok {
			return RawTrade{}, errors.New("missing price column")
		}
		qty, ok := field(rec, idx, "qty", "quantity", "q")
		if !ok {
			return RawTrade{}, errors.New("missing qty column")
		}
		quote, _ := field(rec, idx, "quoteqty", "quote_qty", "quotequantity", "quotequantity")
		timeStr, ok := field(rec, idx, "time", "timestamp", "transacttime", "t", "tradetime")
		if !ok {
			return RawTrade{}, errors.New("missing time column")
		}
		ts, err := parseIntTime(timeStr)
		if err != nil {
			return RawTrade{}, fmt.Errorf("bad time %q", timeStr)
		}
		makerStr, _ := field(rec, idx, "isbuyermaker", "m")
		return RawTrade{
			Exchange:      exchange,
			Symbol:        symbol,
			Source:        model.SourceCSV,
			TradeID:       id,
			Price:         price,
			Quantity:      qty,
			QuoteQuantity: quote,
			IsBuyerMaker:  parseBool(makerStr),
			TradeTimeMS:   ts,
			EventTimeMS:   ts,
		}, nil
	}
}

// parseTradesPositional handles headerless dumps. Two generations exist:
//
//	6 columns: id,price,qty,time,isBuyerMaker,isBestMatch
//	7 columns: id,price,qty,quoteQty,time,isBuyerMaker,isBestMatch
func parseTradesPositional(cols int) func([]string, string, string) (RawTrade, error) {
	withQuote := cols >= 7
	return func(rec []string, exchange, symbol string) (RawTrade, error) {
		if len(rec) < 6 {
			return RawTrade{}, fmt.Errorf("expected >= 6 columns, got %d", len(rec))
		}
		id, err := strconv.ParseInt(strings.TrimSpace(rec[0]), 10, 64)
		if err != nil {
			return RawTrade{}, fmt.Errorf("bad id %q", rec[0])
		}

		timeIdx := 3
		makerIdx := 4
		if withQuote {
			timeIdx = 4
			makerIdx = 5
		}
		if len(rec) <= makerIdx {
			return RawTrade{}, fmt.Errorf("expected >= %d columns, got %d", makerIdx+1, len(rec))
		}
		ts, err := strconv.ParseInt(strings.TrimSpace(rec[timeIdx]), 10, 64)
		if err != nil {
			return RawTrade{}, fmt.Errorf("bad time %q", rec[timeIdx])
		}
		var quote string
		if withQuote {
			quote = strings.TrimSpace(rec[3])
		}
		return RawTrade{
			Exchange:      exchange,
			Symbol:        symbol,
			Source:        model.SourceCSV,
			TradeID:       id,
			Price:         strings.TrimSpace(rec[1]),
			Quantity:      strings.TrimSpace(rec[2]),
			QuoteQuantity: quote,
			IsBuyerMaker:  parseBool(strings.TrimSpace(rec[makerIdx])),
			TradeTimeMS:   ts,
			EventTimeMS:   ts,
		}, nil
	}
}

// parseAggTrades handles aggregated-trade dumps:
// aggTradeId,price,quantity,firstTradeId,lastTradeId,transactTime,isBuyerMaker,isBestMatch
func parseAggTrades(idx map[string]int) func([]string, string, string) (RawTrade, error) {
	return func(rec []string, exchange, symbol string) (RawTrade, error) {
		idStr, ok := field(rec, idx, "aggtradeid", "agg_trade_id", "a", "id")
		if !ok {
			return RawTrade{}, errors.New("missing aggTradeId column")
		}
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			return RawTrade{}, fmt.Errorf("bad aggTradeId %q", idStr)
		}
		price, ok := field(rec, idx, "price", "p")
		if !ok {
			return RawTrade{}, errors.New("missing price column")
		}
		qty, ok := field(rec, idx, "quantity", "qty", "q")
		if !ok {
			return RawTrade{}, errors.New("missing quantity column")
		}
		quote, _ := field(rec, idx, "quoteqty", "quote_qty")
		timeStr, ok := field(rec, idx, "transacttime", "timestamp", "time", "t")
		if !ok {
			return RawTrade{}, errors.New("missing transactTime column")
		}
		ts, err := parseIntTime(timeStr)
		if err != nil {
			return RawTrade{}, fmt.Errorf("bad transactTime %q", timeStr)
		}
		makerStr, _ := field(rec, idx, "isbuyermaker", "m")
		return RawTrade{
			Exchange:      exchange,
			Symbol:        symbol,
			Source:        model.SourceCSV,
			TradeID:       id,
			Price:         price,
			Quantity:      qty,
			QuoteQuantity: quote,
			IsBuyerMaker:  parseBool(makerStr),
			TradeTimeMS:   ts,
			EventTimeMS:   ts,
		}, nil
	}
}

// parseIntTime accepts epoch milliseconds, epoch seconds and RFC3339 text,
// because Binance has shipped all three across its different dump generations.
func parseIntTime(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("empty timestamp")
	}
	if v, err := strconv.ParseInt(s, 10, 64); err == nil {
		if v < 1e11 { // seconds
			return v * 1000, nil
		}
		return v, nil
	}
	if v, err := strconv.ParseFloat(s, 64); err == nil {
		if v < 1e11 {
			return int64(v * 1000), nil
		}
		return int64(v), nil
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05.999", "2006-01-02 15:04:05"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UnixMilli(), nil
		}
	}
	return 0, fmt.Errorf("unrecognised timestamp %q", s)
}

func parseBool(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "1", "t", "true", "y", "yes":
		return true
	default:
		return false
	}
}
