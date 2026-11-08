package app

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/a1ekseev/chat-analyze/internal/jobs"
	"github.com/a1ekseev/chat-analyze/internal/queue"
)

type countEnq struct{ n atomic.Int32 }

func (c *countEnq) EnqueueIdle(_ context.Context, kind, key string, _ any) (queue.EnqueueResult, error) {
	if kind == jobs.KindSyncDialogs && key == jobs.SyncDialogsDedupKey {
		c.n.Add(1)
	}
	return queue.EnqueueResult{}, nil
}

// Nothing is enqueued before Telegram is ready; then at once and every interval.
func TestRunDialogSync(t *testing.T) {
	q := &countEnq{}
	var ready atomic.Bool
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- RunDialogSync(ctx, q, 50*time.Millisecond, ready.Load, slog.New(slog.DiscardHandler)) }()
	time.Sleep(100 * time.Millisecond)
	require.Zero(t, q.n.Load())
	ready.Store(true)
	require.Eventually(t, func() bool { return q.n.Load() >= 3 }, 5*time.Second, 10*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
}
