// Package httpserver serves the single /healthz endpoint.
package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/a1ekseev/chat-analyze/internal/logx"
)

const (
	pingTimeout        = 2 * time.Second
	healthcheckTimeout = 5 * time.Second
)

// Pinger checks database connectivity (pgxpool.Pool satisfies it).
type Pinger interface {
	Ping(ctx context.Context) error
}

// ReadyFunc reports whether the Telegram client is connected and authorized.
type ReadyFunc func() bool

// NewHealth returns a handler serving GET /healthz: 200 {"status":"ok"} when
// the database answers a ping within 2s and Telegram is ready, otherwise 503
// with per-check details. Only failures are logged.
func NewHealth(db Pinger, tgReady ReadyFunc, logger *slog.Logger) http.Handler {
	log := logger.With(logx.Component("http"))
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), pingTimeout)
		defer cancel()

		dbState, tgState := "ok", "ok"
		if err := db.Ping(ctx); err != nil {
			dbState = "error"
		}
		if !tgReady() {
			tgState = "not_ready"
		}

		w.Header().Set("Content-Type", "application/json")
		if dbState == "ok" && tgState == "ok" {
			writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
			return
		}
		log.WarnContext(r.Context(), "health check failed",
			slog.String("db", dbState), slog.String("telegram", tgState))
		writeJSON(w, http.StatusServiceUnavailable,
			map[string]string{"status": "fail", "db": dbState, "telegram": tgState})
	})
	return mux
}

func writeJSON(w http.ResponseWriter, code int, body map[string]string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

// HealthcheckCmd probes http://127.0.0.1:<port of addr>/healthz and returns
// the process exit code: 0 when healthy, 1 otherwise.
func HealthcheckCmd(ctx context.Context, addr string) int {
	base, err := healthcheckURL(addr)
	if err != nil {
		return 1
	}
	return healthcheck(ctx, base)
}

func healthcheckURL(addr string) (string, error) {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("parse http addr: %w", err)
	}
	if port == "" {
		return "", errors.New("parse http addr: empty port")
	}
	return "http://" + net.JoinHostPort("127.0.0.1", port), nil
}

func healthcheck(ctx context.Context, baseURL string) int {
	ctx, cancel := context.WithTimeout(ctx, healthcheckTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/healthz", http.NoBody)
	if err != nil {
		return 1
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
