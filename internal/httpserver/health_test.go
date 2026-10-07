package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/a1ekseev/chat-analyze/internal/logx"
)

type fakePinger struct{ err error }

func (f fakePinger) Ping(context.Context) error { return f.err }

func ready(v bool) ReadyFunc { return func() bool { return v } }

func doHealth(t *testing.T, h http.Handler, method string) (*httptest.ResponseRecorder, map[string]string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), method, "/healthz", http.NoBody))
	var body map[string]string
	if rec.Code != http.StatusMethodNotAllowed {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	}
	return rec, body
}

func TestHealth_OK(t *testing.T) {
	var logs bytes.Buffer
	h := NewHealth(fakePinger{}, ready(true), logx.New(&logs, slog.LevelDebug, "json"))

	rec, body := doHealth(t, h, http.MethodGet)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, map[string]string{"status": "ok"}, body)
	assert.Empty(t, logs.String(), "successful checks are not logged")
}

func TestHealth_DBDown(t *testing.T) {
	var logs bytes.Buffer
	h := NewHealth(fakePinger{err: errors.New("CANARY-db")}, ready(true), logx.New(&logs, slog.LevelDebug, "json"))

	rec, body := doHealth(t, h, http.MethodGet)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, map[string]string{"status": "fail", "db": "error", "telegram": "ok"}, body)
	assert.Contains(t, logs.String(), `"msg":"health check failed"`)
	assert.Contains(t, logs.String(), `"component":"http"`)
	assert.Contains(t, logs.String(), `"db":"error"`)
	assert.NotContains(t, logs.String(), "CANARY")
}

func TestHealth_TelegramNotReady(t *testing.T) {
	var logs bytes.Buffer
	h := NewHealth(fakePinger{}, ready(false), logx.New(&logs, slog.LevelDebug, "json"))

	rec, body := doHealth(t, h, http.MethodGet)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, map[string]string{"status": "fail", "db": "ok", "telegram": "not_ready"}, body)
	assert.Contains(t, logs.String(), `"telegram":"not_ready"`)
}

func TestHealth_MethodNotAllowed(t *testing.T) {
	h := NewHealth(fakePinger{}, ready(true), logx.New(&bytes.Buffer{}, slog.LevelDebug, "json"))

	rec, _ := doHealth(t, h, http.MethodPost)

	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHealth_UnknownPath(t *testing.T) {
	h := NewHealth(fakePinger{}, ready(true), logx.New(&bytes.Buffer{}, slog.LevelDebug, "json"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/metrics", http.NoBody))
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHealthcheckCmd(t *testing.T) {
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(status)
	}))
	defer srv.Close()

	assert.Equal(t, 0, healthcheck(context.Background(), srv.URL))
	status = http.StatusServiceUnavailable
	assert.Equal(t, 1, healthcheck(context.Background(), srv.URL))
	srv.Close()
	assert.Equal(t, 1, healthcheck(context.Background(), srv.URL), "connection refused")
}

func TestHealthcheckURL(t *testing.T) {
	tests := map[string]string{
		":8080":          "http://127.0.0.1:8080",
		"0.0.0.0:9000":   "http://127.0.0.1:9000",
		"localhost:7000": "http://127.0.0.1:7000",
		"[::]:8081":      "http://127.0.0.1:8081",
	}
	for addr, want := range tests {
		got, err := healthcheckURL(addr)
		require.NoError(t, err, addr)
		assert.Equal(t, want, got, addr)
	}
	_, err := healthcheckURL("host:")
	require.Error(t, err)
	_, err = healthcheckURL("nonsense")
	require.Error(t, err)
	assert.Equal(t, 1, HealthcheckCmd(context.Background(), "nonsense"))
}
