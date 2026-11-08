//go:build integration

package db_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/a1ekseev/chat-analyze/internal/db"
	"github.com/a1ekseev/chat-analyze/internal/pgtest"
)

func TestMigrate_UpDownUp(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t) // already migrated up

	v, err := db.Migrate(ctx, pool, "status")
	require.NoError(t, err)
	require.EqualValues(t, 4, v)

	for range 4 {
		_, err = db.Migrate(ctx, pool, "down")
		require.NoError(t, err)
	}
	v, err = db.Migrate(ctx, pool, "status")
	require.NoError(t, err)
	require.EqualValues(t, 0, v)

	v, err = db.Migrate(ctx, pool, "up")
	require.NoError(t, err)
	require.EqualValues(t, 4, v)

	var reg *string
	require.NoError(t, pool.QueryRow(ctx, `SELECT to_regclass('messages')::text`).Scan(&reg))
	require.NotNil(t, reg)
}

// Queued legacy backfill_channel jobs become backfill_history jobs keyed by
// the marked chat id, and back.
func TestMigrate_BackfillHistory(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	_, err := db.Migrate(ctx, pool, "down")
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO jobs (kind, dedup_key, payload, max_attempts)
		VALUES ('backfill_channel', '5', '{"channel_id":5,"min_id":10}', 3)`)
	require.NoError(t, err)

	_, err = db.Migrate(ctx, pool, "up")
	require.NoError(t, err)
	var kind, key, payload string
	require.NoError(t, pool.QueryRow(ctx, `SELECT kind, dedup_key, payload::text FROM jobs`).Scan(&kind, &key, &payload))
	require.Equal(t, "backfill_history", kind)
	require.Equal(t, "-1000000000005", key)
	require.JSONEq(t, `{"chat_id":-1000000000005,"min_id":10}`, payload)

	_, err = db.Migrate(ctx, pool, "down")
	require.NoError(t, err)
	require.NoError(t, pool.QueryRow(ctx, `SELECT kind, dedup_key, payload::text FROM jobs`).Scan(&kind, &key, &payload))
	require.Equal(t, "backfill_channel", kind)
	require.Equal(t, "5", key)
	require.JSONEq(t, `{"channel_id":5,"min_id":10}`, payload)
}

func TestMigrate_UnknownCommand(t *testing.T) {
	pool := pgtest.New(t)
	_, err := db.Migrate(context.Background(), pool, "sideways")
	require.Error(t, err)
}
