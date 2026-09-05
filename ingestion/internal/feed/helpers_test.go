package feed

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nehamanoj1105/market-order-flow-toxicity-detection/ingestion/internal/config"
)

// testLogger keeps the test output readable.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// csvTestConfig builds a minimal configuration for CSV replay tests.
func csvTestConfig(path string) config.Config {
	return config.Config{
		Service: config.ServiceConfig{Name: "test", LogLevel: "error", LogFormat: "text"},
		Feed: config.FeedConfig{
			Mode:      config.FeedCSV,
			Symbols:   []string{"BTCUSDT"},
			Exchange:  "binance",
			QueueSize: 16,
		},
		CSV: config.CSVConfig{
			Path:      path,
			Format:    config.CSVFormatAuto,
			HasHeader: "auto",
			Speed:     0, // as fast as possible
		},
	}
}

// writeTempFile writes content into the test's temporary directory.
func writeTempFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

// collect drains a feed until it finishes, ctx is cancelled, or timeout expires.
func collect(t *testing.T, f Feed, timeout time.Duration) []RawTrade {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	out := make(chan RawTrade, 1024)
	errCh := make(chan error, 1)
	go func() { errCh <- f.Run(ctx, out) }()

	got := make([]RawTrade, 0, 16)
	for {
		select {
		case rt := <-out:
			got = append(got, rt)
		case err := <-errCh:
			if err != nil && ctx.Err() == nil {
				t.Fatalf("feed returned error: %v", err)
			}
			// Drain whatever is still buffered.
			for {
				select {
				case rt := <-out:
					got = append(got, rt)
				default:
					return got
				}
			}
		case <-time.After(timeout):
			cancel()
			t.Fatalf("feed did not finish within %s", timeout)
		}
	}
}
