# Market Order-flow Toxicity Detection
Market Order Flow Toxicity Detection is a real-time software for detecting toxic order flows in cryptocurrency markets. The system consumes cryptocurrency trade evnts, normalizes, and processes them. Then the order flow toxicity score is calculated and alerts are generated when the observed score exceeds configured threshold.

## Technology Stack
* Go for market-data ingestion and processing
* Kafka for asynchronous event streaming
* Python for toxicity scoring
* PostgreSQL for persistent state and checkpoints
* FastAPI for the dahsboard backend
* Docker for local development and deployment
* GitHub Actions for CI/CD and automated testing

## Repository Structure
market-order-flow-toxicity-detection/
│
├── data/
│   ├── BTCUSDT-trades-2026-09-04.csv
│   ├── sample_binance_trades.csv
│   └── synthetic_binance_trades.csv
│
├── infra/
│   └── docker-compose.yaml
│
├── ingestion/
│   ├── bin/
│   │   └── ingestor
│   │
│   ├── cmd/
│   │   └── ingestor/
│   │       ├── main.go
│   │       └── main_test.go
│   │
│   ├── internal/
│   │   ├── config/
│   │   │   ├── config.go
│   │   │   ├── config_test.go
│   │   │   └── envfile.go
│   │   │
│   │   ├── feed/
│   │   │   ├── binance.go
│   │   │   ├── csv.go
│   │   │   ├── csv_test.go
│   │   │   ├── feed.go
│   │   │   ├── helpers_test.go
│   │   │   ├── synthetic.go
│   │   │   └── synthetic_test.go
│   │   │
│   │   ├── model/
│   │   │   ├── itoa.go
│   │   │   ├── itoa_test.go
│   │   │   ├── trade.go
│   │   │   └── trade_test.go
│   │   │
│   │   ├── normalize/
│   │   │   ├── dedup.go
│   │   │   ├── dedup_test.go
│   │   │   ├── normalize.go
│   │   │   └── normalize_test.go
│   │   │
│   │   ├── observability/
│   │   │   ├── logging.go
│   │   │   ├── metrics.go
│   │   │   ├── server.go
│   │   │   └── server_test.go
│   │   │
│   │   └── producer/
│   │       ├── batcher.go
│   │       ├── batcher_test.go
│   │       ├── factory.go
│   │       ├── kafka.go
│   │       ├── producer.go
│   │       ├── rabbitmq.go
│   │       ├── stdout.go
│   │       └── transports_test.go
│   │
│   ├── scripts/
│   │   ├── fetch_binance_sample.sh
│   │   └── generate_sample_csv.py
│   │
│   ├── Dockerfile
│   ├── .dockerignore
│   ├── .env.example
│   ├── go.mod
│   ├── go.sum
│   ├── Makefile
│   └── README.md
│
├── processing/
│   ├── cmd/
│   │   └── processor/
│   │       └── main.go
│   │
│   ├── internal/
│   │   ├── consumer/
│   │   │   ├── consumer.go
│   │   │   └── consumer_test.go
│   │   │
│   │   └── forwarder/
│   │       ├── forwarder.go
│   │       └── forwarder_test.go
│   │
│   ├── Dockerfile
│   ├── go.mod
│   └── go.sum
│
├── scoring/
│   ├── __init__.py
│   ├── checkpoint.py
│   ├── config.py
│   ├── registry.py
│   ├── scorer.py
│   ├── service.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── shared/
│   └── trade_schema.json
│
├── .gitignore
└── README.md

## Repository Setup

1. Building the services
```
docker compose -f infra/docker-compose.yaml build
```

2. Starting the infrastructure
```
docker compose -f infra/docker-compose.yaml build
```

3. Kafka Verification
```
docker compose -f infra/docker-compose.yaml exec kafka \
  kafka-topics --bootstrap-server kafka:9092 --list
```


