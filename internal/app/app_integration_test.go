//go:build integration

package app

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/a1ekseev/chat-analyze/internal/config"
	"github.com/a1ekseev/chat-analyze/internal/logx"
	"github.com/a1ekseev/chat-analyze/internal/pgtest"
	"github.com/a1ekseev/chat-analyze/internal/telegram"
)

func TestRun_NoSessionExitsWithErrNoSession(t *testing.T) {
	cfg, err := config.LoadFrom(map[string]string{
		"DATABASE_URL": pgtest.DSN(t),
		"TG_API_ID":    "12345",
		"TG_API_HASH":  "0123456789abcdef0123456789abcdef",
		"HTTP_ADDR":    "127.0.0.1:0",
		"WORKERS":      "2",
	})
	require.NoError(t, err)
	var buf bytes.Buffer
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	start := time.Now()
	err = Run(ctx, cfg, logx.New(&buf, slog.LevelInfo, "json"))
	require.ErrorIs(t, err, telegram.ErrNoSession) // errors.Is, as main and orchestration rely on it
	require.Less(t, time.Since(start), 5*time.Second)
	require.NoError(t, ctx.Err(), "Run stopped on its own")
	require.Contains(t, buf.String(), `"msg":"telegram auth required"`)
	require.Contains(t, buf.String(), `"msg":"shutdown complete","component":"app","reason":"error"`)
}
