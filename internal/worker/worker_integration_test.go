//go:build integration

package worker_test

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/a1ekseev/chat-analyze/internal/jobs"
	"github.com/a1ekseev/chat-analyze/internal/message"
	"github.com/a1ekseev/chat-analyze/internal/pgtest"
	"github.com/a1ekseev/chat-analyze/internal/queue"
	"github.com/a1ekseev/chat-analyze/internal/worker"
)

type okReader struct{ n int }

func (r *okReader) MarkRead(context.Context, int64, jobs.ReadTarget, int) error { r.n++; return nil }

func TestWorker_ProcessesJobEndToEnd(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	q := queue.New(pool, 5)
	reader := &okReader{}
	var buf bytes.Buffer
	w := worker.New(q, message.NewRepository(pool), reader,
		worker.Config{MarkRead: true, Lease: time.Minute, PollInterval: 10 * time.Millisecond},
		slog.New(slog.NewJSONHandler(&buf, nil)))

	m := message.Message{
		ChatID: -1001, MessageID: 42, ChatType: message.ChatTypeGroup,
		Text: new("hello"), CreatedAt: time.Now().UTC(), TopicID: new(7), TopicTitle: new("T"),
	}
	res, err := q.Enqueue(ctx, jobs.KindSaveMessage, jobs.DedupKey(m.ChatID, m.MessageID),
		jobs.SaveMessage{Message: m})
	require.NoError(t, err)

	did, err := w.RunOnce(ctx)
	require.NoError(t, err)
	require.True(t, did)
	require.Equal(t, 1, reader.n)

	var text string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT message FROM messages WHERE chat_id = $1 AND message_id = $2`, m.ChatID, m.MessageID).Scan(&text))
	require.Equal(t, "hello", text)
	var status string
	require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM jobs WHERE id = $1`, res.ID).Scan(&status))
	require.Equal(t, "done", status)

	did, err = w.RunOnce(ctx)
	require.NoError(t, err)
	require.False(t, did)
}
