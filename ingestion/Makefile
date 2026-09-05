# Makefile for the ingestion service (toxic order flow detector)
#
#   make help        – list targets
#   make build       – compile ./bin/ingestor
#   make test        – unit + integration tests
#   make race        – tests under the race detector
#   make run-csv     – replay data/sample_binance_trades.csv to stdout
#   make run-synth   – synthetic feed to stdout (no infrastructure needed)
#   make kafka       – replay into Kafka (needs infra/docker-compose.yaml up)

SHELL := /bin/bash
BIN_DIR ?= bin
BINARY  ?= $(BIN_DIR)/ingestor
IMAGE   ?= tofd/ingestor
TAG     ?= dev

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE)

export GOFLAGS ?= -mod=mod

.PHONY: help build test race cover vet fmt lint run-csv run-synth kafka rabbit \
        docker docker-run data clean tidy

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

## Build --------------------------------------------------------------------
build: ## Compile the binary
	@mkdir -p $(BIN_DIR)
	go build -trimpath -ldflags "$(LDFLAGS)" -o $(BINARY) ./cmd/ingestor
	@echo "built $(BINARY) ($(VERSION))"

tidy: ## Sync go.mod/go.sum
	go mod tidy

## Test ---------------------------------------------------------------------
test: ## Run all tests
	go test ./...

race: ## Run tests under the race detector
	go test -race -timeout 300s ./...

cover: ## Test with coverage, opens coverage.html
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -n 1
	go tool cover -html=coverage.out -o coverage.html
	@echo "coverage.html written"

vet: ## go vet
	go vet ./...

fmt: ## gofmt (rewrites files)
	gofmt -w .

lint: vet ## vet + gofmt check
	@test -z "$$(gofmt -l .)" || { echo "gofmt needed on:"; gofmt -l .; exit 1; }

## Run ----------------------------------------------------------------------
run-csv: ## Replay the sample CSV to stdout (no broker needed)
	go run ./cmd/ingestor -env-file .env.example || \
	FEED_MODE=csv CSV_PATH=../data/sample_binance_trades.csv CSV_SPEED=50 BROKER=stdout \
		STATS_EVERY=5s go run ./cmd/ingestor

run-synth: ## Synthetic feed to stdout, 200 trades/s with toxic bursts
	FEED_MODE=synthetic SYNTH_TRADES_PER_SEC=200 BROKER=stdout \
	SYNTH_BURST_EVERY=30s SYNTH_BURST_DURATION=8s STATS_EVERY=5s \
		go run ./cmd/ingestor

kafka: ## Replay into Kafka on localhost:9092
	FEED_MODE=csv CSV_PATH=../data/sample_binance_trades.csv CSV_SPEED=50 \
	BROKER=kafka KAFKA_BROKERS=localhost:9092 STATS_EVERY=5s \
		go run ./cmd/ingestor

rabbit: ## Replay into RabbitMQ on localhost:5672
	FEED_MODE=synthetic SYNTH_TRADES_PER_SEC=50 BROKER=rabbitmq \
	RABBIT_URL=amqp://guest:guest@localhost:5672/ STATS_EVERY=5s \
		go run ./cmd/ingestor

check: ## Validate the current configuration and print it
	go run ./cmd/ingestor -check

data: ## Regenerate the sample CSV (deterministic)
	python3 scripts/generate_sample_csv.py --rows 20000 \
		--out ../data/sample_binance_trades.csv

## Docker -------------------------------------------------------------------
docker: ## Build the container image
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg DATE=$(DATE) \
		-t $(IMAGE):$(TAG) .

docker-run: docker ## Run the container with the CSV feed to stdout
	docker run --rm -p 9090:9090 \
		-e BROKER=stdout -e FEED_MODE=synthetic -e SYNTH_TRADES_PER_SEC=50 \
		$(IMAGE):$(TAG)

clean: ## Remove build artefacts
	rm -rf $(BIN_DIR) coverage.out coverage.html
