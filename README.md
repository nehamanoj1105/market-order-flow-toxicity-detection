# Market Order Flow Toxicity Detection

A Docker-based Secure Software Engineering project for detecting potentially toxic market order flow in real time using Order Flow Imbalance (OFI), EWMA-based statistics, and Z-score thresholding.

The system ingests trade data, normalizes it, calculates toxicity indicators, generates alerts, persists alerts and audit records, and provides a web dashboard for monitoring, filtering, configuration, and CSV export.

---

## System Architecture

```text
CSV / Trade Feed
       |
       v
+----------------+
| Go Ingestor    |
+----------------+
       |
       v
 Kafka: trades.raw
       |
       v
+----------------+
| Go Processing  |
| Normalize/Dedup|
+----------------+
       |
       v
 Kafka: trades.normalized
       |
       v
+----------------+
| Python Scorer  |
| OFI + EWMA     |
| Z-score        |
+----------------+
       |
       v
 Kafka: trades.alerts
       |
       +--------------------+
       |                    |
       v                    v
+----------------+   +----------------+
| PostgreSQL /   |   | FastAPI        |
| TimescaleDB    |   | Dashboard      |
+----------------+   +----------------+
       |                    |
       |                    v
       |              WebSocket
       |              Live Alerts
       |
       +--> Alert History
       +--> Audit Logs
       +--> CSV Export
```

## Technology Stack

| Component | Technology | Purpose |
|---|---|---|
| Ingestion | Go | Reads trade data and publishes raw trades |
| Processing | Go | Normalizes and deduplicates trades |
| Scoring | Python | Calculates OFI/EWMA/Z-score and toxicity |
| Messaging | Apache Kafka | Connects pipeline stages |
| Database | PostgreSQL + TimescaleDB | Stores alerts, configurations and audit logs |
| Dashboard | FastAPI + HTML/CSS/JavaScript | Monitoring and administration |
| Authentication | JWT | Protects administrative operations |
| Audit | PostgreSQL + hash chaining | Records important system events |
| Deployment | Docker Compose | Runs the complete system |

## Repository Structure

```
market-order-flow-toxicity-detection/
│
├── dashboard/
│   ├── app/
│   │   ├── api/
│   │   │   ├── routes.py
│   │   │   └── schemas.py
│   │   ├── core/
│   │   │   ├── alert_stream.py
│   │   │   ├── auth.py
│   │   │   ├── config.py
│   │   │   ├── database.py
│   │   │   └── websocket.py
│   │   └── main.py
│   ├── frontend/
│   │   ├── css/dashboard.css
│   │   ├── js/dashboard.js
│   │   └── index.html
│   ├── Dockerfile
│   └── requirements.txt
│
├── data/
│   ├── sample_binance_trades.csv
│   └── synthetic_binance_trades.csv
│
├── infra/
│   ├── docker-compose.yaml
│   └── init.sql
│
├── ingestion/
│   ├── cmd/
│   │   └── ingestor/
│   ├── internal/
│   │   ├── config/
│   │   ├── feed/
│   │   ├── model/
│   │   ├── normalize/
│   │   ├── observability/
│   │   └── producer/
│   ├── scripts/
│   ├── Dockerfile
│   ├── Makefile
│   ├── go.mod
│   └── go.sum
│
├── processing/
│   ├── cmd/
│   │   └── processor/
│   ├── internal/
│   │   ├── consumer/
│   │   └── forwarder/
│   ├── Dockerfile
│   ├── go.mod
│   └── go.sum
│
├── scoring/
│   ├── checkpoint.py
│   ├── config.py
│   ├── registry.py
│   ├── scorer.py
│   ├── service.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── notification_agent/
│   ├── agent.py
│   └── requirements.txt
│
├── shared/
│   └── trade_schema.json
│
├── tools/
│   └── synthetic_alerts.py
│
├── .gitignore
├── infra/docker-compose.yaml
└── README.md
```

## Requirements

- Docker
- Docker Compose
- Git
- Python 3

The project is intended to run using Docker Compose.

## Quick Start

From the project root:

```bash
docker-compose -f infra/docker-compose.yaml up -d
```

Check service status:

```bash
docker-compose -f infra/docker-compose.yaml ps
```

Expected services: `zookeeper`, `kafka`, `postgres`, `ingestor`, `processing`, `scorer`, `dashboard`

The dashboard is available at: **http://localhost:8000**

## Dashboard

Open http://localhost:8000

The dashboard provides:

- **Live Alerts** — Incoming toxicity alerts via WebSocket
- **Alert History** — Historical alerts filterable by trading pair, timestamp range, and Z-score range
- **Configuration** — Admin-authenticated threshold management (Z-score threshold, EWMA alpha, minimum trade count)
- **CSV Export** — Download historical alerts as CSV (requires authentication)

If no alerts match the selected filters: `No records found`

### Default Development Credentials

- Username: `admin`
- Password: `password`

These credentials are intended for local development/demo use only.

## Kafka Topics

| Topic | Purpose |
|---|---|
| `trades.raw` | Raw ingested trades |
| `trades.normalized` | Normalized/deduplicated trades |
| `trades.alerts` | Toxicity alerts |
| `trades.normalized.dlq` | Dead letter queue |

Kafka listeners:
- Docker services: `kafka:9092`
- Host machine: `localhost:29092`

## Synthetic Alert Generation

Test the dashboard without the full pipeline:

```bash
# Stop the live pipeline
docker-compose -f infra/docker-compose.yaml stop ingestor processing scorer

# Generate synthetic alerts
python3 tools/synthetic_alerts.py --brokers localhost:29092 --count 24 --delay 0.5
```

## API Reference

### Authentication

```bash
curl -X POST http://localhost:8000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"password"}'
```

### Alert History

```bash
# Basic query
curl "http://localhost:8000/api/alerts/history?limit=50&offset=0"

# Filter by symbol
curl "http://localhost:8000/api/alerts/history?symbol=BTCUSDT&limit=50&offset=0"

# Filter by Z-score range
curl "http://localhost:8000/api/alerts/history?min_z_score=4&max_z_score=6&limit=50&offset=0"
```

Supports: `from_time`, `to_time`, `min_z_score`, `max_z_score`, `symbol`, `limit`, `offset`

### Configuration

```bash
# Retrieve
curl http://localhost:8000/api/config -H "Authorization: Bearer <TOKEN>"

# Update
curl -X POST http://localhost:8000/api/config \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"symbol":"BTCUSDT","z_threshold":3.5,"ewma_alpha":0.1,"min_trades":10}'
```

### Audit Logs

```bash
curl -s "http://localhost:8000/api/audit?limit=100"
```

Audit entries use hash chaining. Event types: `ALERT`, `CONFIG_CHANGE`

### CSV Export

```bash
curl -L "http://localhost:8000/api/export" -H "Authorization: Bearer <TOKEN>" -o toxicity_alerts.csv
curl -L "http://localhost:8000/api/export?symbol=BTCUSDT" -H "Authorization: Bearer <TOKEN>" -o btcusdt_alerts.csv
```

## Database

PostgreSQL/TimescaleDB is exposed on `localhost:5432`.

| Setting | Value |
|---|---|
| Database | `toxicflow` |
| Username | `postgres` |
| Password | `devpassword` |

Main tables: `alerts`, `symbol_configs`, `audit_logs`

Schema: `infra/init.sql`

## Useful Docker Commands

```bash
# View all services
docker-compose -f infra/docker-compose.yaml ps

# Service logs
docker-compose -f infra/docker-compose.yaml logs dashboard --tail=100
docker-compose -f infra/docker-compose.yaml logs scorer --tail=100

# Restart dashboard
docker-compose -f infra/docker-compose.yaml restart dashboard

# Stop live pipeline only
docker-compose -f infra/docker-compose.yaml stop ingestor processing scorer

# Start infra + dashboard only
docker-compose -f infra/docker-compose.yaml up -d zookeeper kafka postgres dashboard

# Stop everything (preserves data)
docker-compose -f infra/docker-compose.yaml down
```

> **Warning**: Do not use `docker-compose down -v` unless database volumes are intentionally meant to be deleted.

## Development Notes

The dashboard source is bind-mounted in Docker Compose (`../dashboard:/app`), so Python/HTML/CSS/JavaScript changes normally require only:

```bash
docker-compose -f infra/docker-compose.yaml restart dashboard
```

Changes to Dockerfiles or dependencies require rebuilding:

```bash
docker-compose -f infra/docker-compose.yaml build dashboard
docker-compose -f infra/docker-compose.yaml up -d dashboard
```

## Security Features

- JWT authentication for admin operations
- Protected configuration and CSV export endpoints
- Input validation using Pydantic
- Hash-chained audit logging
- Dockerized service isolation
- No credentials stored in the Git repository through `.env` files

Development credentials in Docker Compose are for local demonstration only.
