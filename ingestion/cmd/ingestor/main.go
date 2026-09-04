// Command ingestor is the market data ingestion service of the toxic order
// flow detector.
//
// Pipeline:
//
//	feed (live websocket | csv replay | synthetic simulator)
//	  -> dispatcher (shards by symbol so per-symbol order is preserved)
//	    -> worker pool (dedup -> normalize -> publish)
//	      -> async producer (batching, retry, dead letter)
//	        -> broker (Kafka | RabbitMQ | stdout)
//
// Run `ingestor -h` for flags and see README.md for the full configuration
// reference.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/config"
	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/feed"
	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/model"
	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/normalize"
	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/observability"
	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/producer"
)

// Build information injected at link time (see Makefile).
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	os.Exit(run())
}

func run() int {
	var (
		envFile     string
		showVersion bool
		checkOnly   bool
	)
	flag.StringVar(&envFile, "env-file", os.Getenv("ENV_FILE"), "path to a .env file (real environment variables win)")
	flag.BoolVar(&showVersion, "version", false, "print build information and exit")
	flag.BoolVar(&checkOnly, "check", false, "validate the configuration, print it, and exit")
	flag.Parse()

	if showVersion {
		fmt.Printf("ingestor %s (commit %s, built %s, schema %s)\n", version, commit, date, model.SchemaVersion)
		return 0
	}

	cfg, err := config.Load(config.LoadOptions{EnvFile: envFile})
	if err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		return 2
	}
	logger := observability.NewLogger(cfg.Service, resolveLogOutput(cfg))

	if checkOnly {
		fmt.Printf("configuration OK\n%s", cfg.String())
		return 0
	}

	app, err := newApp(cfg, logger)
	if err != nil {
		logger.Error("startup failed", "error", err)
		return 1
	}
	return app.Run()
}

// resolveLogOutput keeps the data stream clean: when the broker *is* stdout,
// logs must go to stderr so that `ingestor | jq` sees only trade JSON.
func resolveLogOutput(cfg config.Config) io.Writer {
	switch cfg.Service.LogOutput {
	case "stderr":
		return os.Stderr
	case "stdout":
		return os.Stdout
	default: // auto
		if cfg.Broker.Kind == config.BrokerStdout {
			return os.Stderr
		}
		return os.Stdout
	}
}

// app holds every long lived dependency of the service.
type app struct {
	cfg     config.Config
	log     *slog.Logger
	metrics *observability.Metrics
	server  *observability.Server
	source  feed.Feed
	norm    *normalize.Normalizer
	dedup   *normalize.Dedup
	prod    *producer.AsyncProducer

	wg       sync.WaitGroup
	shutdown sync.Once
}

func newApp(cfg config.Config, logger *slog.Logger) (*app, error) {
	metrics := observability.NewMetrics()
	metrics.SetBuildInfo(version, commit, date, cfg.Service.Environment)

	server := observability.NewServer(cfg.Service.HTTPAddr, metrics.Registry(), logger, map[string]any{
		"service":        cfg.Service.Name,
		"version":        version,
		"commit":         commit,
		"schema_version": model.SchemaVersion,
		"feed_mode":      string(cfg.Feed.Mode),
		"broker":         string(cfg.Broker.Kind),
		"symbols":        cfg.Feed.Symbols,
		"workers":        cfg.Pipeline.Workers,
	})

	sink, err := producer.NewSink(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("broker setup: %w", err)
	}

	source, err := feed.New(cfg, logger, metrics)
	if err != nil {
		return nil, fmt.Errorf("feed setup: %w", err)
	}

	var dedup *normalize.Dedup
	if cfg.Dedup.Enabled {
		dedup = normalize.NewDedup(cfg.Dedup.TTL, cfg.Dedup.MaxKeys)
	}

	return &app{
		cfg:     cfg,
		log:     logger,
		metrics: metrics,
		server:  server,
		source:  source,
		norm:    normalize.New(cfg.Normalize, "", metrics, logger),
		dedup:   dedup,
		prod:    producer.NewAsyncProducer(cfg, sink, logger, metrics),
	}, nil
}

// Run wires the process together: signals, HTTP, producer, then the pipeline.
// It blocks until the feed finishes or a shutdown signal arrives.
func (a *app) Run() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	a.server.Start()
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = a.server.Shutdown(shutCtx)
	}()

	a.log.Info("ingestor starting",
		"version", version, "commit", commit, "schema", model.SchemaVersion,
		"feed", a.cfg.Feed.Mode, "broker", a.cfg.Broker.Kind,
		"symbols", strings.Join(a.cfg.Feed.Symbols, ","), "workers", a.cfg.Pipeline.Workers)

	a.prod.Start(ctx)
	a.server.SetReady(true)
	a.log.Info("ingestor ready", "http", a.cfg.Service.HTTPAddr)

	// Periodic throughput summary in the logs.
	stats := a.startStatsLoop(ctx)
	defer func() {
		close(stats.stop)
		<-stats.done
	}()

	exitCode := a.runPipeline(ctx)

	a.log.Info("flushing producer", "queue_depth", a.prod.QueueDepth())
	a.prod.Close()

	c := a.prod.Counters()
	a.log.Info("ingestor stopped",
		"published", c.Published, "batches", c.Batches, "retries", c.Retries,
		"failed", c.Failed, "dead_lettered", c.DeadLettered, "dropped", c.Dropped,
		"bytes", c.Bytes, "normalized", a.norm.Sequence())
	return exitCode
}

// runPipeline owns the feed, the dispatcher and the worker pool. It returns
// once the stream has been fully drained.
func (a *app) runPipeline(ctx context.Context) int {
	rawCh := make(chan feed.RawTrade, a.cfg.Feed.QueueSize)
	workerCh := make([]chan feed.RawTrade, a.cfg.Pipeline.Workers)
	for i := range workerCh {
		workerCh[i] = make(chan feed.RawTrade, a.cfg.Pipeline.ChannelSize)
		a.wg.Add(1)
		go a.runWorker(i, workerCh[i])
	}

	// Dispatcher: one goroutine shards records by symbol so that all trades of
	// a market flow through the same worker in arrival order.
	dispatchDone := make(chan struct{})
	go func() {
		defer close(dispatchDone)
		defer func() {
			for _, ch := range workerCh {
				close(ch)
			}
		}()
		for raw := range rawCh {
			idx := shardIndex(raw.Symbol, len(workerCh))
			select {
			case workerCh[idx] <- raw:
			case <-ctx.Done():
				// Keep draining what we can on the way out.
				select {
				case workerCh[idx] <- raw:
				default:
				}
			}
		}
	}()

	feedDone := make(chan error, 1)
	go func() { feedDone <- a.source.Run(ctx, rawCh) }()

	exitCode := 0
	select {
	case err := <-feedDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			a.log.Error("feed stopped with error", "error", err)
			a.server.SetReady(false)
			exitCode = 1
		} else {
			a.log.Info("feed finished", "error", err)
		}
	case <-ctx.Done():
		a.log.Info("shutdown signal received, draining pipeline")
		select {
		case err := <-feedDone:
			if err != nil && !errors.Is(err, context.Canceled) {
				a.log.Warn("feed stopped with error during shutdown", "error", err)
			}
		case <-time.After(a.cfg.Pipeline.ShutdownGrace):
			a.log.Warn("feed did not stop within grace period, forcing shutdown",
				"grace", a.cfg.Pipeline.ShutdownGrace)
		}
	}

	// Safe to close: the feed goroutine has returned, so nobody writes to rawCh.
	// Safe to close: the feed goroutine has returned, so nobody writes to rawCh.
	close(rawCh)
	<-dispatchDone
	a.wg.Wait()
	return exitCode
}

// runWorker consumes one shard of the stream: dedup -> normalize -> publish.
func (a *app) runWorker(id int, in <-chan feed.RawTrade) {
	defer a.wg.Done()
	for raw := range in {
		a.handle(raw)
	}
	a.log.Debug("worker stopped", "worker", id)
}

func (a *app) handle(raw feed.RawTrade) {
	exchange := strings.ToLower(strings.TrimSpace(raw.Exchange))
	symbol := model.NormalizeSymbol(raw.Symbol)

	if a.dedup != nil && a.dedup.Seen(exchange, symbol, raw.TradeID) {
		a.metrics.DuplicateTotal(exchange, symbol)
		return
	}

	trade, err := a.norm.Normalize(raw)
	if err != nil {
		var ne *normalize.Error
		if errors.As(err, &ne) {
			a.log.Warn("record rejected",
				"reason", ne.Reason, "detail", ne.Detail,
				"symbol", ne.Raw.Symbol, "trade_id", ne.Raw.TradeID)
			a.deadLetter(ne)
			return
		}
		a.log.Error("unexpected normalization failure", "error", err)
		return
	}

	// A background context: during shutdown we still want queued work to be
	// enqueued so the final flush can deliver it.
	if err := a.prod.Publish(context.Background(), trade); err != nil {
		if errors.Is(err, producer.ErrClosed) {
			a.log.Warn("trade dropped: producer already closed", "event_id", trade.EventID)
			return
		}
		a.log.Error("publish failed", "event_id", trade.EventID, "error", err)
	}
}

// deadLetter routes an unusable record to the DLQ instead of dropping it.
func (a *app) deadLetter(ne *normalize.Error) {
	rec := producer.DLQRecord{
		FailedAtMS: time.Now().UnixMilli(),
		Reason:     ne.Reason,
		Detail:     ne.Detail,
		Exchange:   strings.ToLower(ne.Raw.Exchange),
		Symbol:     model.NormalizeSymbol(ne.Raw.Symbol),
		TradeID:    ne.Raw.TradeID,
		RawPayload: string(ne.Raw.Payload),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.prod.DeadLetter(ctx, rec); err != nil {
		a.log.Error("dead lettering failed", "reason", ne.Reason, "error", err)
	}
}

// statsLoop controls the throughput reporter goroutine.
type statsLoop struct {
	stop chan struct{}
	done chan struct{}
}

// startStatsLoop logs a throughput heartbeat and refreshes gauges.
func (a *app) startStatsLoop(ctx context.Context) statsLoop {
	sl := statsLoop{stop: make(chan struct{}), done: make(chan struct{})}
	interval := a.cfg.Service.StatsEvery
	if interval <= 0 {
		interval = 15 * time.Second
	}
	go func() {
		defer close(sl.done)
		t := time.NewTicker(interval)
		defer t.Stop()
		var lastPublished, lastNormalized uint64
		last := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-sl.stop:
				return
			case <-t.C:
				now := time.Now()
				elapsed := now.Sub(last).Seconds()
				c := a.prod.Counters()
				normalized := a.norm.Sequence()
				ratePub := float64(c.Published-lastPublished) / elapsed
				rateNorm := float64(normalized-lastNormalized) / elapsed
				last, lastPublished, lastNormalized = now, c.Published, normalized

				if a.dedup != nil {
					a.metrics.SetDedupKeys(a.dedup.Len())
				}
				a.metrics.SetQueueDepth(a.prod.QueueDepth())

				a.log.Info("throughput",
					"normalized_per_sec", fmt.Sprintf("%.1f", rateNorm),
					"published_per_sec", fmt.Sprintf("%.1f", ratePub),
					"published_total", c.Published,
					"queue_depth", a.prod.QueueDepth(),
					"failed", c.Failed, "dead_lettered", c.DeadLettered, "dropped", c.Dropped)
			}
		}
	}()
	return sl
}

// shardIndex maps a symbol to a worker, preserving per-symbol ordering.
func shardIndex(symbol string, workers int) int {
	if workers <= 1 {
		return 0
	}
	h := fnv.New32a()
	h.Write([]byte(model.NormalizeSymbol(symbol)))
	return int(h.Sum32() % uint32(workers))
}
