package observability

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server exposes /metrics, /healthz, /readyz and a small JSON index.
//
// /healthz answers "is the process alive" (used by container orchestrators for
// restarts). /readyz answers "is the ingestor actually producing" (used to pull
// a pod out of rotation while it reconnects to the broker).
type Server struct {
	addr     string
	registry *prometheus.Registry
	log      *slog.Logger
	info     map[string]any
	ready    atomic.Bool
	srv      *http.Server
	started  time.Time
}

// NewServer builds (but does not start) the observability server.
func NewServer(addr string, registry *prometheus.Registry, log *slog.Logger, info map[string]any) *Server {
	if addr == "" {
		addr = ":9090"
	}
	s := &Server{addr: addr, registry: registry, log: log, info: info, started: time.Now()}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
		Registry:          registry,
	}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "uptime_seconds": s.uptime()})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if s.ready.Load() {
			writeJSON(w, http.StatusOK, map[string]any{"status": "ready", "uptime_seconds": s.uptime()})
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "starting", "uptime_seconds": s.uptime()})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		out := map[string]any{"uptime_seconds": s.uptime(), "ready": s.ready.Load()}
		for k, v := range s.info {
			out[k] = v
		}
		writeJSON(w, http.StatusOK, out)
	})

	s.srv = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      20 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return s
}

// Start binds the listener in the background.
func (s *Server) Start() {
	go func() {
		s.log.Info("observability http server listening",
			"addr", s.addr, "endpoints", "/metrics /healthz /readyz /")
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.log.Error("observability server stopped", "error", err)
		}
	}()
}

// SetReady flips readiness, which drives /readyz.
func (s *Server) SetReady(ready bool) { s.ready.Store(ready) }

// Shutdown gracefully stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

func (s *Server) uptime() float64 { return time.Since(s.started).Seconds() }

func writeJSON(w http.ResponseWriter, code int, body map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}
