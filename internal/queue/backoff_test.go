package queue_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/a1ekseev/chat-analyze/internal/queue"
)

func TestBackoff(t *testing.T) {
	require.Equal(t, time.Second, queue.Backoff(1))
	require.Equal(t, 2*time.Second, queue.Backoff(2))
	require.Equal(t, 16*time.Second, queue.Backoff(5))
	require.Equal(t, 10*time.Minute, queue.Backoff(11))
	require.Equal(t, 10*time.Minute, queue.Backoff(100))
	require.Equal(t, time.Second, queue.Backoff(0))
	require.Equal(t, time.Second, queue.Backoff(-3))
}
