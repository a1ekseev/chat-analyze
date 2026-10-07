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
	require.EqualValues(t, 3, v)

	for range 3 {
		_, err = db.Migrate(ctx, pool, "down")
		require.NoError(t, err)
	}
	v, err = db.Migrate(ctx, pool, "status")
	require.NoError(t, err)
	require.EqualValues(t, 0, v)

	v, err = db.Migrate(ctx, pool, "up")
	require.NoError(t, err)
	require.EqualValues(t, 3, v)

	var reg *string
	require.NoError(t, pool.QueryRow(ctx, `SELECT to_regclass('messages')::text`).Scan(&reg))
	require.NotNil(t, reg)
}

func TestMigrate_UnknownCommand(t *testing.T) {
	pool := pgtest.New(t)
	_, err := db.Migrate(context.Background(), pool, "sideways")
	require.Error(t, err)
}
