//go:build integration

package telegram_test

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/stretchr/testify/require"

	"github.com/a1ekseev/chat-analyze/internal/logx"
	"github.com/a1ekseev/chat-analyze/internal/pgtest"
	"github.com/a1ekseev/chat-analyze/internal/telegram"
)

// Without a stored session Run fails before connecting: no network, no new
// auth key on the Telegram side, no unauthorized session written.
func TestClient_RunWithoutSessionFailsFast(t *testing.T) {
	pool := pgtest.New(t)
	var buf bytes.Buffer
	c := telegram.NewClient(telegram.ClientConfig{
		APIID: 12345, APIHash: "0123456789abcdef0123456789abcdef",
		Pool: pool, SessionName: "default",
		Logger: logx.New(&buf, slog.LevelDebug, "json"), GotdLogLevel: slog.LevelDebug,
	})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	registered := false
	start := time.Now()
	err := c.Run(ctx, func(*tg.UpdateDispatcher) { registered = true })
	require.ErrorIs(t, err, telegram.ErrNoSession)
	require.Less(t, time.Since(start), time.Second)
	require.False(t, registered, "nothing is wired without a session")
	require.False(t, c.Ready())
	require.Contains(t, buf.String(), `"msg":"telegram auth required","component":"telegram"`)

	var n int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM tg_session`).Scan(&n))
	require.Zero(t, n, "no session row is created")
}

func TestClient_RunSessionLoadErrorIsNotNoSession(t *testing.T) {
	pool := pgtest.New(t)
	_, err := pool.Exec(t.Context(), `DROP TABLE tg_session`)
	require.NoError(t, err)
	c := telegram.NewClient(telegram.ClientConfig{
		APIID: 12345, APIHash: "0123456789abcdef0123456789abcdef", Pool: pool, SessionName: "default",
		Logger: logx.New(&bytes.Buffer{}, slog.LevelDebug, "json"),
	})
	err = c.Run(t.Context(), nil)
	require.Error(t, err)
	require.NotErrorIs(t, err, telegram.ErrNoSession)
}
