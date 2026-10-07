//go:build integration

package telegram_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/a1ekseev/chat-analyze/internal/pgtest"
	"github.com/a1ekseev/chat-analyze/internal/telegram"
)

func TestReadMarks_Monotonic(t *testing.T) {
	ctx := context.Background()
	s := telegram.NewReadMarks(pgtest.New(t))

	_, ok, err := s.Get(ctx, -100, 0)
	require.NoError(t, err)
	require.False(t, ok)

	require.NoError(t, s.Set(ctx, -100, 0, 10))
	require.NoError(t, s.Set(ctx, -100, 0, 5))
	got, ok, err := s.Get(ctx, -100, 0)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 10, got)

	require.NoError(t, s.Set(ctx, -100, 0, 12))
	got, _, err = s.Get(ctx, -100, 0)
	require.NoError(t, err)
	require.Equal(t, 12, got)

	_, ok, err = s.Get(ctx, -100, 7)
	require.NoError(t, err)
	require.False(t, ok)
}
