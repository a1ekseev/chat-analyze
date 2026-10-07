//go:build integration

package app

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/a1ekseev/chat-analyze/internal/logx"
	"github.com/a1ekseev/chat-analyze/internal/pgtest"
	"github.com/a1ekseev/chat-analyze/internal/queue"
)

func TestMaintenance_ReapsAndCleans(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	q := queue.New(pool, 5)

	expired, err := q.Enqueue(ctx, "k", "expired", map[string]int{})
	require.NoError(t, err)
	oldDone, err := q.Enqueue(ctx, "k", "old-done", map[string]int{})
	require.NoError(t, err)
	freshDone, err := q.Enqueue(ctx, "k", "fresh-done", map[string]int{})
	require.NoError(t, err)
	oldDead, err := q.Enqueue(ctx, "k", "old-dead", map[string]int{})
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE jobs SET status = 'dead', updated_at = now() - interval '31 days' WHERE id = $1`, oldDead.ID)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `UPDATE jobs SET status = 'running', locked_until = now() - interval '1 minute' WHERE id = $1`, expired.ID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE jobs SET status = 'done', updated_at = now() - interval '10 days' WHERE id = $1`, oldDone.ID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE jobs SET status = 'done', updated_at = now() WHERE id = $1`, freshDone.ID)
	require.NoError(t, err)

	var logs bytes.Buffer
	maintenanceTick(ctx, q, queue.Retention{Done: 168 * time.Hour, Dead: 720 * time.Hour}, logx.New(&logs, slog.LevelDebug, "json"))

	var status string
	require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM jobs WHERE id = $1`, expired.ID).Scan(&status))
	assert.Equal(t, "pending", status)

	var n int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE id = $1`, oldDone.ID).Scan(&n))
	assert.Equal(t, 0, n, "old done job deleted")
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE id = $1`, freshDone.ID).Scan(&n))
	assert.Equal(t, 1, n, "fresh done job kept")
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE id = $1`, oldDead.ID).Scan(&n))
	assert.Equal(t, 0, n, "dead job older than the dead retention deleted")

	out := logs.String()
	assert.Contains(t, out, `"msg":"jobs reaped"`)
	assert.Contains(t, out, `"component":"maintenance"`)
	assert.Contains(t, out, `"count":1`)
}

func TestMaintenance_NoReapNoWarn(t *testing.T) {
	pool := pgtest.New(t)
	var logs bytes.Buffer
	maintenanceTick(context.Background(), queue.New(pool, 5), queue.Retention{Done: time.Hour, Dead: time.Hour}, logx.New(&logs, slog.LevelInfo, "json"))
	assert.NotContains(t, logs.String(), "jobs reaped")
}

func TestMaintenance_StatsTick(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	q := queue.New(pool, 5)
	_, err := q.Enqueue(ctx, "k", "a", map[string]int{})
	require.NoError(t, err)

	var logs bytes.Buffer
	statsTick(ctx, q, logx.New(&logs, slog.LevelInfo, "json"))
	assert.Contains(t, logs.String(), `"msg":"queue stats"`)
	assert.Contains(t, logs.String(), `"pending":1`)
}

func TestRunMaintenance_StopsOnCancel(t *testing.T) {
	pool := pgtest.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- RunMaintenance(ctx, queue.New(pool, 5), queue.Retention{Done: time.Hour, Dead: time.Hour}, logx.New(&bytes.Buffer{}, slog.LevelInfo, "json"))
	}()
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("RunMaintenance did not stop")
	}
}
