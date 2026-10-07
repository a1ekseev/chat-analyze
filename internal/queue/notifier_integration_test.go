//go:build integration

package queue_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/a1ekseev/chat-analyze/internal/pgtest"
	"github.com/a1ekseev/chat-analyze/internal/queue"
)

func TestNotifier_WakesOnEnqueue(t *testing.T) {
	pool := pgtest.New(t)
	q := queue.New(pool, 5)
	n := queue.NewNotifier(pool, slog.New(slog.DiscardHandler))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel) // stop Run even on Fatal, so pool.Close does not block
	done := make(chan error, 1)
	go func() { done <- n.Run(ctx) }()

	// The notifier signals once right after LISTEN is active; wait for it so
	// that any later signal can only come from NOTIFY.
	select {
	case <-n.C():
	case <-time.After(5 * time.Second):
		t.Fatal("notifier did not start listening")
	}

	_, err := q.Enqueue(context.Background(), "k", "d", 1)
	require.NoError(t, err)
	select {
	case <-n.C():
	case <-time.After(2 * time.Second):
		t.Fatal("no notification received after enqueue")
	}

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
