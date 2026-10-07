//go:build integration

package message_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/a1ekseev/chat-analyze/internal/message"
	"github.com/a1ekseev/chat-analyze/internal/pgtest"
)

func base() message.Message {
	return message.Message{
		ChatID:    -100123,
		MessageID: 1,
		ChatType:  message.ChatTypeChannel,
		Text:      new("a"),
		Views:     5,
		Entities:  []byte(`[{"type":"bold"}]`),
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func TestUpsert_InsertAndEdit(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	r := message.NewRepository(pool)

	m := base()
	require.NoError(t, r.Upsert(ctx, m))

	t1 := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	m.Text, m.UpdatedAt, m.Views = new("b"), &t1, 3
	require.NoError(t, r.Upsert(ctx, m))

	var text string
	var upd time.Time
	var views int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT message, updated_at, views FROM messages WHERE chat_id=$1 AND message_id=$2`,
		m.ChatID, m.MessageID).Scan(&text, &upd, &views))
	require.Equal(t, "b", text)
	require.True(t, t1.Equal(upd))
	require.Equal(t, 5, views)
}

func TestUpsert_OlderEditDoesNotOverwrite(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	r := message.NewRepository(pool)

	t1 := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)
	m := base()
	m.Text, m.UpdatedAt = new("new"), &t2
	require.NoError(t, r.Upsert(ctx, m))
	m.Text, m.UpdatedAt = new("old"), &t1
	require.NoError(t, r.Upsert(ctx, m))

	var text string
	require.NoError(t, pool.QueryRow(ctx, `SELECT message FROM messages`).Scan(&text))
	require.Equal(t, "new", text)
}

func TestUpsert_ChatTypeCheck(t *testing.T) {
	pool := pgtest.New(t)
	r := message.NewRepository(pool)

	m := base()
	m.ChatType = "bogus"
	err := r.Upsert(context.Background(), m)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "23514", pgErr.Code)
}

func TestUpdateViews_Monotonic(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	r := message.NewRepository(pool)
	require.NoError(t, r.Upsert(ctx, base()))

	require.NoError(t, r.UpdateViews(ctx, -100123, 1, 10))
	require.NoError(t, r.UpdateViews(ctx, -100123, 1, 7))
	require.NoError(t, r.UpdateViews(ctx, -100123, 999, 7)) // missing row: no-op

	var views int
	require.NoError(t, pool.QueryRow(ctx, `SELECT views FROM messages`).Scan(&views))
	require.Equal(t, 10, views)
}

func TestUpsertTopic_RenameKeepsLatest(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	r := message.NewRepository(pool)

	require.NoError(t, r.UpsertTopic(ctx, -100123, 5, "first"))
	require.NoError(t, r.UpsertTopic(ctx, -100123, 5, "second"))

	var title string
	var n int
	require.NoError(t, pool.QueryRow(ctx, `SELECT title, count(*) OVER () FROM tg_forum_topics`).Scan(&title, &n))
	require.Equal(t, "second", title)
	require.Equal(t, 1, n)
}

func TestLastMessageID_EmptyAndMax(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	r := message.NewRepository(pool)

	_, ok, err := r.LastMessageID(ctx, -100123)
	require.NoError(t, err)
	require.False(t, ok)

	for _, id := range []int{3, 9, 4} {
		m := base()
		m.MessageID = id
		require.NoError(t, r.Upsert(ctx, m))
	}
	id, ok, err := r.LastMessageID(ctx, -100123)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 9, id)
}

func TestUpsert_OriginalAfterEditDoesNotOverwrite(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	r := message.NewRepository(pool)

	edited := time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC)
	m := base()
	m.Text, m.UpdatedAt = new("EDITED"), &edited
	require.NoError(t, r.Upsert(ctx, m))
	m.Text, m.UpdatedAt = new("ORIGINAL"), nil // the unedited version processed late
	require.NoError(t, r.Upsert(ctx, m))

	var text string
	var upd time.Time
	require.NoError(t, pool.QueryRow(ctx, `SELECT message, updated_at FROM messages`).Scan(&text, &upd))
	require.Equal(t, "EDITED", text)
	require.True(t, edited.Equal(upd))
}

func TestUpsert_RepeatedOriginalStillApplies(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	r := message.NewRepository(pool)

	m := base()
	require.NoError(t, r.Upsert(ctx, m))
	m.Views = 9
	require.NoError(t, r.Upsert(ctx, m)) // duplicate delivery of an unedited message

	var views int
	require.NoError(t, pool.QueryRow(ctx, `SELECT views FROM messages`).Scan(&views))
	require.Equal(t, 9, views)
}

func TestUpsert_NullChatNameKeepsKnown(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	r := message.NewRepository(pool)

	m := base()
	m.ChatName, m.ChatUsername = new("Known"), new("known_user")
	require.NoError(t, r.Upsert(ctx, m))

	t1 := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	m.ChatName, m.ChatUsername, m.UpdatedAt, m.Text = nil, nil, &t1, new("b")
	require.NoError(t, r.Upsert(ctx, m))

	var name, username *string
	require.NoError(t, pool.QueryRow(ctx, `SELECT chat_name, chat_username FROM messages`).Scan(&name, &username))
	require.NotNil(t, name)
	require.Equal(t, "Known", *name)
	require.NotNil(t, username)
	require.Equal(t, "known_user", *username)

	m.ChatName = new("Renamed")
	t2 := t1.Add(time.Hour)
	m.UpdatedAt = &t2
	require.NoError(t, r.Upsert(ctx, m))
	require.NoError(t, pool.QueryRow(ctx, `SELECT chat_name FROM messages`).Scan(&name))
	require.Equal(t, "Renamed", *name)
}
