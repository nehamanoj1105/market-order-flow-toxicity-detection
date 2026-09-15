FROM golang:1.24 AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o processor ./cmd/processor

FROM debian:bookworm-slim
COPY --from=build /app/processor /usr/local/bin/processor
ENTRYPOINT ["processor"]
