# Ingestion service — `toxic-order-flow-detector/ingestion`

Go service that pulls raw market data from an exchange (or a simulator), normalizes
it into the team-wide `Trade` schema, and publishes it onto the message bus for the
processing and scoring services.

---

## 1. Where this sits in the pipeline

```
                      ┌──────────────────────── ingestion (this service) ────────────────────────┐
 Binance WS / CSV  →  │ feed → dispatcher → worker pool → async producer → sink │  →  Kafka / RabbitMQ
 synthetic feed       │        (by symbol)   dedup+normalize   batch+retry+DLQ  │        │
                      └─────────────────────────────────────────────────────────┘        │
                                                                                          ▼
                                    processing (Devnath) → scoring (Neha) → dashboard (Adarsh)
```

* **feed** – pluggable source: live Binance websocket, CSV replay, or a seeded simulator.
* **dispatcher** – shards records by symbol (`fnv(symbol) % workers`) so every trade of a
  market flows through the *same* worker. That is what preserves per-symbol ordering
  while still using all cores.
* **worker pool** – de-duplicate → normalize → publish.
* **async producer** – bounded queue, size/linger batching, capped-exponential retry,
  then dead letter. One flusher goroutine keeps ordering intact.
* **sink** – the transport: `kafka`, `rabbitmq`, `stdout` (default) or `discard`.

The published payload is defined once, in [`../shared/trade_schema.json`](../shared/trade_schema.json),
and mirrored by `internal/model.Trade`. A test fails if the two ever drift
(`internal/model/trade_test.go`).

---

## 2. Quick start

**Prerequisites:** Go 1.25+ (`go.mod` pins 1.25 because of the Prometheus client), and
Python 3 only if you want to regenerate the sample data. No broker is needed to run it.

### 2.1 No infrastructure at all (30 seconds)

```bash
cd ingestion
make build                      # -> bin/ingestor
BROKER=stdout FEED_MODE=synthetic SYNTH_TRADES_PER_SEC=50 ./bin/ingestor | jq
```

```json
{
  "event_id": "6a9cbef8-0573-4b14-9c2b-9c24125c86e3",
  "schema_version": "1.0.0",
  "source": "synthetic",
  "exchange": "binance",
  "symbol": "BTCUSDT",
  "trade_id": 4000000001,
  "price": 65000.29,
  "quantity": 0.0450454,
  "quote_quantity": 2927.964063166,
  "side": "SELL",
  "is_buyer_maker": true,
  "trade_time_ms": 1788451622788,
  "event_time_ms": 1788451622788,
  "ingested_at_ms": 1788451622789,
  "sequence": 1,
  "ingestion_host": "ingestor-0"
}
```

With `BROKER=stdout`, logs automatically go to **stderr**, so `ingestor | jq` stays clean.
(`LOG_OUTPUT=auto|stdout|stderr` overrides it.)

### 2.2 Replay the bundled sample data

```bash
CSV_PATH=../data/sample_binance_trades.csv CSV_SPEED=50 ./bin/ingestor | jq -c
```

* `CSV_SPEED=1` replays in real time, `50` is 50× faster, `0` is as fast as the pipeline drains.
* The sample file contains a **planted toxic burst** at rows 12 000–13 200 (≈88 % aggressive
  buys, larger sizes, faster arrivals) — that is the signal the scorer should catch.
  Regenerate it with `make data`, or fetch real data with `scripts/fetch_binance_sample.sh`.

### 2.3 Against the real stack (Kafka)

```bash
docker compose -f infra/docker-compose.yaml up -d kafka

cd ingestion
BROKER=kafka KAFKA_BROKERS=localhost:9092 \
FEED_MODE=csv CSV_PATH=../data/sample_binance_trades.csv CSV_SPEED=50 ./bin/ingestor
```

Then verify what landed on the wire:

```bash
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic trades.normalized --from-beginning --max-messages 3
```

### 2.4 Live market data

```bash
FEED_MODE=live FEED_SYMBOLS=BTCUSDT,ETHUSDT BROKER=kafka ./bin/ingestor
```

Public endpoint, no API key. It reconnects with exponential backoff and can optionally
backfill the gap left by a restart over REST (`BINANCE_BACKFILL=true`).

---

## 3. Configuration

Every setting has a sane default (see `internal/config/config.go` and `.env.example`).
Real environment variables win over `-env-file` values.

```bash
./bin/ingestor -check            # validate + print the resolved config (secrets redacted)
./bin/ingestor -version          # build metadata
./bin/ingestor -env-file .env    # load a .env file
```

| Variable | Default | Purpose |
|---|---|---|
| `FEED_MODE` | `csv` | `live` \| `csv` \| `synthetic` |
| `FEED_SYMBOLS` | `BTCUSDT` | Comma separated; also the fallback symbol for CSV dumps |
| `FEED_QUEUE_SIZE` | `8192` | Backlog between feed and workers |
| `CSV_PATH` / `CSV_SPEED` / `CSV_LOOP` / `CSV_MAX_ROWS` / `CSV_START_ROW` | see `.env.example` | Replay controls |
| `CSV_FORMAT` | `auto` | `auto` \| `binance-trades` \| `binance-aggtrades` (header auto-detected) |
| `SYNTH_*` | — | Simulator: rate, seed, burst cadence/imbalance/size |
| `BINANCE_*` | — | Websocket URLs, stream kind, backoff, REST backfill |
| `PIPELINE_WORKERS` | `4` | Normalize/publish goroutines |
| `DEDUP_ENABLED` / `DEDUP_TTL` / `DEDUP_MAX_KEYS` | `true` / `10m` / `1e6` | Suppress re-delivered trade ids |
| `BROKER` | `stdout` | `kafka` \| `rabbitmq` \| `stdout` \| `discard` |
| `PRODUCER_BATCH_SIZE` / `PRODUCER_LINGER` | `500` / `20ms` | Batching |
| `PRODUCER_QUEUE_SIZE` | `16384` | Bounded backlog; when full, the feed is throttled |
| `PRODUCER_MAX_RETRIES` / `_RETRY_BASE` / `_RETRY_MAX` | `5` / `100ms` / `5s` | Retry policy |
| `KAFKA_*` | see `.env.example` | Brokers, topic, DLQ topic, acks, compression, TLS, SASL |
| `RABBIT_*` | see `.env.example` | URL, exchange, DLX, routing key, confirms |
| `NORMALIZE_*` | — | Range/staleness guards that reject bad data |
| `HTTP_ADDR` | `:9090` | `/metrics` `/healthz` `/readyz` `/` |
| `LOG_LEVEL` / `LOG_FORMAT` / `LOG_OUTPUT` | `info` / `json` / `auto` | Logging |

---

## 4. Output contract 

**Kafka**

| Item | Value |
|---|---|
| Topic | `KAFKA_TOPIC` (default `trades.normalized`) |
| Dead letter topic | `KAFKA_DLQ_TOPIC` (default `trades.normalized.dlq`) |
| Partition key | `exchange:symbol` (hash balancer) |
| Ordering | Total per key (per symbol); no global ordering |
| Headers | `schema_version`, `event_id`, `exchange`, `symbol`, `source`, `side`, `trade_time_ms`, `ingested_at_ms`, `content-type` |

**RabbitMQ**

| Item | Value |
|---|---|
| Exchange | `trades` (topic, durable) |
| Routing key | `trade.<exchange>.<symbol>` |
| Dead letter exchange | `trades.dlx` |
| Delivery | persistent + publisher confirms (at-least-once) |

**Guarantees**

1. **At-least-once.** A batch that times out is retried, so a message can be delivered
   twice after a failover. Consumers must be idempotent — `event_id` (UUIDv4) is the key.
2. **Ordering.** The flusher is single-goroutine and the dispatcher shards by symbol, so
   per-symbol order survives end to end.
3. **Backpressure.** The producer queue is bounded; when it is full the feed blocks.
   Memory stays flat during a broker outage instead of growing until the OOM killer strikes.
4. **No poison pills.** Records that fail normalization are never published; they go to the
   dead letter destination with the original payload (see §7).

**Side semantics** — the field the scorer depends on:

| `is_buyer_maker` | meaning | `side` |
|---|---|---|
| `true` | buyer was resting on the book → seller crossed the spread | `SELL` |
| `false` | seller was resting → buyer crossed the spread | `BUY` |

---

## 5. Metrics and health

`http://localhost:9090`

| Path | Purpose |
|---|---|
| `/metrics` | Prometheus exposition (OpenMetrics) |
| `/healthz` | Liveness — always 200 while the process runs |
| `/readyz` | Readiness — 200 once the pipeline is producing, 503 while starting |
| `/` | JSON summary (version, feed mode, broker, uptime) |

Key series (all prefixed `tofd_ingestor_`):

| Metric | Meaning |
|---|---|
| `feed_read_total{exchange,symbol,source}` | Raw records read from the feed |
| `feed_errors_total{feed,reason}` | Malformed frames, disconnects |
| `feed_events_total{feed,kind}` | Reconnects, pings, synthetic burst start/end |
| `normalized_total{exchange,symbol}` | Successfully normalized trades |
| `normalize_errors_total{exchange,symbol,reason}` | Rejections by reason |
| `duplicates_total{exchange,symbol}` | Suppressed re-deliveries |
| `published_total{broker,destination}` | Messages accepted by the broker |
| `publish_errors_total` / `publish_retries_total` | Delivery problems |
| `dead_lettered_total` / `dropped_total` | Poison records / total loss |
| `ingest_lag_milliseconds` | `ingested_at_ms - trade_time_ms` freshness |
| `producer_queue_depth` | Backlog — watch this during broker outages |
| `build_info{version,commit,date,environment}` | Deployment traceability |

A throughput line is also logged every `STATS_EVERY` (default 15 s).

---

## 6. Layout

```
ingestion/
├── cmd/ingestor/main.go         # wiring: signals, HTTP, producer, pipeline
├── internal/
│   ├── config/                  # env + .env loading, validation, redacted dump
│   ├── feed/
│   │   ├── feed.go              # Feed interface, RawTrade, factory
│   │   ├── binance.go           # live websocket (+ REST backfill, reconnect)
│   │   ├── csv.go               # historical replay, header/format auto-detect
│   │   └── synthetic.go         # seeded simulator with toxic bursts
│   ├── model/                   # Trade: the mirror of shared/trade_schema.json
│   ├── normalize/               # RawTrade -> Trade, plus the de-duplicator
│   ├── observability/           # slog setup, Prometheus metrics, HTTP server
│   └── producer/
│       ├── producer.go          # Message / Sink / DeadLetterer contracts
│       ├── batcher.go           # queue, batching, backoff, dead lettering
│       ├── kafka.go             # segmentio/kafka-go (TLS, SASL, compression)
│       ├── rabbitmq.go          # amqp091-go (topic exchange, confirms)
│       └── stdout.go            # stdout / discard sinks for dev and benchmarks
├── scripts/
│   ├── generate_sample_csv.py   # deterministic sample data with a planted burst
│   └── fetch_binance_sample.sh  # slice a real dump from data.binance.vision
├── Dockerfile                   # multi-stage, static binary, non-root
├── Makefile                     # build / test / race / cover / run / docker
└── .env.example                 # every knob, documented
```

---

## 7. Dead letters

A record is dead lettered (never silently dropped) when it cannot be normalized — bad
price, empty symbol, missing timestamp, out-of-range value, stale trade — or when the
broker rejected it after all retries.

```json
{
  "failed_at_ms": 1788451654147,
  "reason": "bad_price",
  "detail": "price=\"abc\"",
  "exchange": "binance",
  "symbol": "BTCUSDT",
  "trade_id": 2,
  "raw_payload": "2,abc,1.0,1.0,1704153600001,false,true"
}
```

`reason` values: `empty_symbol`, `bad_price`, `bad_quantity`, `bad_quote_quantity`,
`bad_timestamp`, `price_out_of_range`, `quantity_out_of_range`, `stale_trade`,
`invalid_trade`.

---

## 8. Tests

```bash
make test     # unit + end-to-end pipeline tests
make race     # same, under the race detector
make cover    # coverage report -> coverage.html
```

What is covered:

* **Schema contract** – every required property of `shared/trade_schema.json` is emitted
  with the right JSON type.
* **Normalization** – side derivation from `is_buyer_maker`, quote-quantity handling,
  UUID stamping, sequence monotonicity under concurrency, every rejection reason.
* **De-duplication** – detection, TTL expiry, memory bounds, concurrent safety.
* **Feeds** – all three CSV layouts (7-col, 6-col headerless, aggTrades), malformed row
  skipping, `MaxRows`/`StartRow`/loop, replay pacing, context cancellation, synthetic
  determinism per seed, arrival rate, and burst imbalance.
* **Producer** – batch-size and linger flushing, retry-then-succeed, dead lettering after
  retries exhausted, flush-on-close, `ErrClosed` after close, and **backpressure**.
* **Pipeline** – CSV replay and synthetic feed end to end into an in-memory sink,
  including duplicate suppression and dead lettering.

---

## 9. Docker

```bash
make docker                                  # builds tofd/ingestor:dev
docker run --rm -p 9090:9090 \
  -e BROKER=kafka -e KAFKA_BROKERS=kafka:9092 \
  -e FEED_MODE=csv -e CSV_PATH=/data/sample_binance_trades.csv \
  -v "$PWD/../data:/data:ro" \
  tofd/ingestor:dev
```

For the shared compose file, this is the service block (Adarsh — merge/rename as needed):

```yaml
  ingestor:
    build:
      context: ./ingestion
      args: { VERSION: dev }
    image: tofd/ingestor:dev
    container_name: tofd-ingestor
    restart: unless-stopped
    depends_on: [kafka]
    environment:
      BROKER: kafka
      KAFKA_BROKERS: kafka:9092
      KAFKA_TOPIC: trades.normalized
      FEED_MODE: csv                       # or synthetic / live
      CSV_PATH: /data/sample_binance_trades.csv
      CSV_SPEED: "50"
      LOG_FORMAT: json
      STATS_EVERY: 15s
    volumes:
      - ../data:/data:ro
    ports:
      - "9090:9090"
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://127.0.0.1:9090/healthz"]
      interval: 30s
      timeout: 5s
      retries: 3
```

---

## 10. Operational notes

* **Backpressure, not unbounded buffering.** If the broker goes down the ingestor slows
  the feed instead of growing memory; `producer_queue_depth` shows it happening.
* **Graceful shutdown.** `SIGTERM`/`SIGINT` stops the feed, drains the dispatcher and
  workers, flushes queued batches, and only then closes the broker connection.
* **De-duplication is per process.** Restarting loses the cache; the scorer should stay
  idempotent on `event_id` anyway.
* **Secrets.** Kafka SASL passwords are redacted in `-check` output and never logged.
  In production, prefer SASL/SCRAM over PLAIN and enable `KAFKA_TLS_ENABLED`.
* **Clock skew.** `ingest_lag_milliseconds` clamps negative lag to 0 rather than emitting
  nonsense if the exchange clock is ahead of ours.

## 11. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `configuration error: CSV_SPEED must be >= 0` | Negative speed; use `0` for max rate |
| Everything is slow with `BROKER=kafka` | Broker unreachable → retries with backoff. Check `publish_retries_total` and `/readyz` |
| `csv replay pass complete ... rows=0` | Wrong `CSV_PATH` (paths are relative to the working directory) |
| Duplicate alerts downstream | Expected after a retry/failover; de-duplicate on `event_id` |
| `dead lettering failed, records dropped` | The DLQ destination itself is down — create `trades.normalized.dlq` / `trades.dlx` |
| Symbol shows as the file name | Rename the dump to `BTCUSDT-trades-....csv` or set `FEED_SYMBOLS` |
