# ---------------------------------------------------------------------------
# Stage 1: build. Pinned, slim image; the module cache is mounted so repeated
# local builds stay fast.
# ---------------------------------------------------------------------------
FROM golang:1.25-alpine AS build

ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown

WORKDIR /src

# Dependencies first: this layer is cached unless go.mod/go.sum change.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY cmd/ ./cmd/
COPY internal/ ./internal/

# CGO disabled keeps the binary static so it runs on distroless/alpine with no
# libc surprises. -trimpath removes local paths from panic stack traces.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${DATE}" \
    -o /out/ingestor ./cmd/ingestor

# ---------------------------------------------------------------------------
# Stage 2: runtime. Minimal image, non-root user, no shell, healthchecked.
# ---------------------------------------------------------------------------
FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata wget \
    && addgroup -g 10001 -S ingestor \
    && adduser -u 10001 -S -G ingestor ingestor

COPY --from=build /out/ingestor /usr/local/bin/ingestor

USER 10001:10001
WORKDIR /home/ingestor

# Metrics / health endpoints.
EXPOSE 9090

ENV LOG_FORMAT=json \
    LOG_LEVEL=info \
    HTTP_ADDR=:9090 \
    BROKER=kafka \
    FEED_MODE=csv \
    CSV_PATH=/data/sample_binance_trades.csv

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- http://127.0.0.1:9090/healthz || exit 1

ENTRYPOINT ["/usr/local/bin/ingestor"]
