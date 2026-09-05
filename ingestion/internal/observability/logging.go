// Package observability wires structured logging, Prometheus metrics and the
// health/metrics HTTP server. It is intentionally dependency-light: log/slog
// from the standard library plus the official Prometheus client.
package observability

import (
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/config"
)

// NewLogger builds the service logger writing to out. JSON is the default so
// that logs from every container can be shipped to the dashboard's log store
// without parsing.
func NewLogger(cfg config.ServiceConfig, out io.Writer) *slog.Logger {
	if out == nil {
		out = os.Stdout
	}
	level := parseLevel(cfg.LogLevel)
	opts := &slog.HandlerOptions{
		Level: level,
		// Source locations are only worth their cost while debugging.
		AddSource: level == slog.LevelDebug,
	}

	var handler slog.Handler
	if strings.EqualFold(cfg.LogFormat, "text") {
		handler = slog.NewTextHandler(out, opts)
	} else {
		handler = slog.NewJSONHandler(out, opts)
	}

	logger := slog.New(handler).With(
		slog.String("service", cfg.Name),
		slog.String("env", cfg.Environment),
	)
	slog.SetDefault(logger)
	return logger
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// LogFatal is a small helper so main can log and exit in one call.
func LogFatal(logger *slog.Logger, msg string, err error) {
	logger.Error(msg, "error", err)
	os.Exit(1)
}
