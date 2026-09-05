package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/model"
)

// Metrics is the single place where the ingestor's Prometheus metrics are
// declared. It implements the feed.Stats, normalize.Stats and producer Stats
// interfaces so no package needs to import the client library directly.
type Metrics struct {
	reg *prometheus.Registry

	feedRead        *prometheus.CounterVec
	feedErrors      *prometheus.CounterVec
	feedEvents      *prometheus.CounterVec
	normalized      *prometheus.CounterVec
	normalizeErrors *prometheus.CounterVec
	duplicates      *prometheus.CounterVec
	published       *prometheus.CounterVec
	publishErrors   *prometheus.CounterVec
	publishRetries  *prometheus.CounterVec
	dlq             *prometheus.CounterVec
	dropped         *prometheus.CounterVec

	batchSize      prometheus.Histogram
	publishLatency prometheus.Histogram
	ingestLag      prometheus.Histogram
	queueDepth     prometheus.Gauge
	dedupKeys      prometheus.Gauge
	buildInfo      *prometheus.GaugeVec
	startTime      prometheus.Gauge
}

// NewMetrics registers every collector on a dedicated registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	m := &Metrics{
		reg: reg,
		feedRead: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tofd_ingestor_feed_read_total",
			Help: "Raw trades read from the market data feed.",
		}, []string{"exchange", "symbol", "source"}),
		feedErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tofd_ingestor_feed_errors_total",
			Help: "Feed level failures such as malformed frames or disconnects.",
		}, []string{"feed", "reason"}),
		feedEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tofd_ingestor_feed_events_total",
			Help: "Feed lifecycle events: reconnects, pings, synthetic bursts.",
		}, []string{"feed", "kind"}),
		normalized: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tofd_ingestor_normalized_total",
			Help: "Trades successfully normalized into the canonical schema.",
		}, []string{"exchange", "symbol"}),
		normalizeErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tofd_ingestor_normalize_errors_total",
			Help: "Records rejected by the normalizer, by reason.",
		}, []string{"exchange", "symbol", "reason"}),
		duplicates: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tofd_ingestor_duplicates_total",
			Help: "Records dropped because the same trade id was already ingested.",
		}, []string{"exchange", "symbol"}),
		published: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tofd_ingestor_published_total",
			Help: "Trades successfully handed to the broker.",
		}, []string{"broker", "destination"}),
		publishErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tofd_ingestor_publish_errors_total",
			Help: "Broker batches that failed after all retries.",
		}, []string{"broker", "destination"}),
		publishRetries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tofd_ingestor_publish_retries_total",
			Help: "Retry attempts after a transient broker failure.",
		}, []string{"broker"}),
		dlq: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tofd_ingestor_dead_lettered_total",
			Help: "Records routed to the dead letter destination.",
		}, []string{"broker", "destination"}),
		dropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tofd_ingestor_dropped_total",
			Help: "Records that could not be published or dead lettered.",
		}, []string{"broker"}),
		batchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "tofd_ingestor_batch_size",
			Help:    "Number of messages per broker batch.",
			Buckets: []float64{1, 10, 50, 100, 250, 500, 1000, 2500, 5000},
		}),
		publishLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "tofd_ingestor_publish_latency_seconds",
			Help:    "Broker write latency per batch.",
			Buckets: prometheus.DefBuckets,
		}),
		ingestLag: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "tofd_ingestor_ingest_lag_milliseconds",
			Help:    "Delay between the exchange match time and normalization time.",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000, 30000},
		}),
		queueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "tofd_ingestor_producer_queue_depth",
			Help: "Messages waiting in the async producer queue.",
		}),
		dedupKeys: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "tofd_ingestor_dedup_keys",
			Help: "Approximate number of trade ids tracked by the de-duplicator.",
		}),
		buildInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "tofd_ingestor_build_info",
			Help: "Build metadata, always 1.",
		}, []string{"version", "commit", "date", "environment"}),
		startTime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "tofd_ingestor_start_time_seconds",
			Help: "Unix time at which the process started.",
		}),
	}

	reg.MustRegister(
		m.feedRead, m.feedErrors, m.feedEvents,
		m.normalized, m.normalizeErrors, m.duplicates,
		m.published, m.publishErrors, m.publishRetries, m.dlq, m.dropped,
		m.batchSize, m.publishLatency, m.ingestLag,
		m.queueDepth, m.dedupKeys, m.buildInfo, m.startTime,
	)
	m.startTime.Set(float64(time.Now().Unix()))
	return m
}

// Registry exposes the registry for the /metrics handler.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// SetBuildInfo records version metadata.
func (m *Metrics) SetBuildInfo(version, commit, date, environment string) {
	m.buildInfo.WithLabelValues(version, commit, date, environment).Set(1)
}

// ---- feed.Stats ----

// FeedReadTotal counts a raw record read from a feed.
func (m *Metrics) FeedReadTotal(exchange, symbol string, source model.Source) {
	m.feedRead.WithLabelValues(exchange, symbol, string(source)).Inc()
}

// FeedErrorTotal counts a feed level failure.
func (m *Metrics) FeedErrorTotal(feed, reason string) {
	m.feedErrors.WithLabelValues(feed, reason).Inc()
}

// FeedEventTotal counts a feed lifecycle event (reconnect, ping, burst).
func (m *Metrics) FeedEventTotal(feed, kind string) {
	m.feedEvents.WithLabelValues(feed, kind).Inc()
}

// ---- normalize.Stats ----

// NormalizedTotal counts a successfully normalized trade.
func (m *Metrics) NormalizedTotal(exchange, symbol string) {
	m.normalized.WithLabelValues(exchange, symbol).Inc()
}

// NormalizeErrorTotal counts a rejected record.
func (m *Metrics) NormalizeErrorTotal(exchange, symbol, reason string) {
	m.normalizeErrors.WithLabelValues(exchange, symbol, reason).Inc()
}

// DuplicateTotal counts a suppressed duplicate.
func (m *Metrics) DuplicateTotal(exchange, symbol string) {
	m.duplicates.WithLabelValues(exchange, symbol).Inc()
}

// IngestLagMilliseconds records end-to-end freshness of a trade.
func (m *Metrics) IngestLagMilliseconds(exchange, symbol string, ms float64) {
	m.ingestLag.Observe(ms)
}

// ---- producer metrics ----

// PublishedTotal counts successful message deliveries.
func (m *Metrics) PublishedTotal(broker, destination string, n int) {
	m.published.WithLabelValues(broker, destination).Add(float64(n))
}

// PublishErrorTotal counts a batch that failed permanently.
func (m *Metrics) PublishErrorTotal(broker, destination string) {
	m.publishErrors.WithLabelValues(broker, destination).Inc()
}

// PublishRetryTotal counts a retry attempt.
func (m *Metrics) PublishRetryTotal(broker string) {
	m.publishRetries.WithLabelValues(broker).Inc()
}

// DeadLetteredTotal counts records routed to the dead letter destination.
func (m *Metrics) DeadLetteredTotal(broker, destination string, n int) {
	m.dlq.WithLabelValues(broker, destination).Add(float64(n))
}

// DroppedTotal counts records lost after the DLQ also failed.
func (m *Metrics) DroppedTotal(broker string, n int) {
	m.dropped.WithLabelValues(broker).Add(float64(n))
}

// ObserveBatch records a batch size observation.
func (m *Metrics) ObserveBatch(n int) { m.batchSize.Observe(float64(n)) }

// ObservePublishLatency records how long a broker write took.
func (m *Metrics) ObservePublishLatency(d time.Duration) {
	m.publishLatency.Observe(d.Seconds())
}

// SetQueueDepth reports the current async producer backlog.
func (m *Metrics) SetQueueDepth(n int) { m.queueDepth.Set(float64(n)) }

// SetDedupKeys reports the dedup cache size.
func (m *Metrics) SetDedupKeys(n int) { m.dedupKeys.Set(float64(n)) }
