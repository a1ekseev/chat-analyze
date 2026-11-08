//go:build integration

package telegram_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/a1ekseev/chat-analyze/internal/jobs"
	"github.com/a1ekseev/chat-analyze/internal/pgtest"
	"github.com/a1ekseev/chat-analyze/internal/telegram"
)

const selfID = int64(99)

func newCache(t *testing.T) *telegram.PeerCache {
	t.Helper()
	return telegram.NewPeerCache(pgtest.New(t))
}

func TestPeerCache_SaveGet(t *testing.T) {
	ctx := context.Background()
	c := newCache(t)
	ps := map[int64]telegram.PeerInfo{
		1:              {ChatID: 1, Kind: jobs.PeerUser, ID: 1, Name: "Ann", Username: "ann", AccessHash: 11, IsSelf: true},
		-2:             {ChatID: -2, Kind: jobs.PeerChat, ID: 2, Name: "Grp"},
		-1000000000003: {ChatID: -1000000000003, Kind: jobs.PeerChannel, ID: 3, Name: "Ch", IsBroadcast: true, IsForum: true, AccessHash: 33},
	}
	require.NoError(t, c.Save(ctx, selfID, ps))

	for id, want := range ps {
		got, ok, err := c.Get(ctx, selfID, id)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, want, got)
	}
	_, ok, err := c.Get(ctx, selfID, 12345)
	require.NoError(t, err)
	require.False(t, ok)

	// non-min update overwrites known values and flags.
	require.NoError(t, c.Save(ctx, selfID, map[int64]telegram.PeerInfo{
		1: {ChatID: 1, Kind: jobs.PeerUser, ID: 1, Name: "Ann2", Username: "ann2", AccessHash: 11},
	}))
	got, _, err := c.Get(ctx, selfID, 1)
	require.NoError(t, err)
	require.Equal(t, "Ann2", got.Name)
	require.Equal(t, "ann2", got.Username)
	require.False(t, got.IsSelf)
}

func TestPeerCache_EmptyDoesNotBlank(t *testing.T) {
	ctx := context.Background()
	c := newCache(t)
	require.NoError(t, c.Save(ctx, selfID, map[int64]telegram.PeerInfo{
		1: {ChatID: 1, Kind: jobs.PeerUser, ID: 1, Name: "A", Username: "u"},
	}))
	require.NoError(t, c.Save(ctx, selfID, map[int64]telegram.PeerInfo{
		1: {ChatID: 1, Kind: jobs.PeerUser, ID: 1},
	}))
	got, ok, err := c.Get(ctx, selfID, 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "A", got.Name)
	require.Equal(t, "u", got.Username)
}

func TestPeerCache_MinDoesNotOverwrite(t *testing.T) {
	ctx := context.Background()
	c := newCache(t)
	full := telegram.PeerInfo{ChatID: 1, Kind: jobs.PeerUser, ID: 1, Name: "A", Username: "a", AccessHash: 5}
	require.NoError(t, c.Save(ctx, selfID, map[int64]telegram.PeerInfo{1: full}))

	minPeer := telegram.PeerInfo{ChatID: 1, Kind: jobs.PeerUser, ID: 1, Name: "B", Username: "b", AccessHash: 6, IsMin: true}
	require.NoError(t, c.Save(ctx, selfID, map[int64]telegram.PeerInfo{1: minPeer}))
	got, ok, err := c.Get(ctx, selfID, 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, full, got)

	// a min entity with no existing row is inserted, without storing its hash.
	min2 := telegram.PeerInfo{ChatID: 2, Kind: jobs.PeerUser, ID: 2, Name: "M", AccessHash: 7, IsMin: true}
	require.NoError(t, c.Save(ctx, selfID, map[int64]telegram.PeerInfo{2: min2}))
	got, ok, err = c.Get(ctx, selfID, 2)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "M", got.Name)
	require.Equal(t, int64(0), got.AccessHash)
}

func TestPeerCache_AccessHashStored(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	st := telegram.NewStateStorage(pool)
	c := telegram.NewPeerCache(pool)
	require.NoError(t, c.Save(ctx, selfID, map[int64]telegram.PeerInfo{
		1:              {ChatID: 1, Kind: jobs.PeerUser, ID: 1, AccessHash: 11},
		-1000000000003: {ChatID: -1000000000003, Kind: jobs.PeerChannel, ID: 3, AccessHash: 33},
		-2:             {ChatID: -2, Kind: jobs.PeerChat, ID: 2, AccessHash: 22},
		4:              {ChatID: 4, Kind: jobs.PeerUser, ID: 4},
	}))
	h, ok, err := st.GetUserAccessHash(ctx, selfID, 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(11), h)
	h, ok, err = st.GetChannelAccessHash(ctx, selfID, 3)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(33), h)
	_, ok, err = st.GetUserAccessHash(ctx, selfID, 4)
	require.NoError(t, err)
	require.False(t, ok)
}

// A hash stored by updates.Manager (no tg_peers row yet) is returned once a
// min entity of the same channel is saved.
func TestPeerCache_MinPeerGetsManagerStoredHash(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	require.NoError(t, telegram.NewStateStorage(pool).SetChannelAccessHash(ctx, selfID, 5, 88))
	c := telegram.NewPeerCache(pool)
	minCh := telegram.PeerInfo{ChatID: -1000000000005, Kind: jobs.PeerChannel, ID: 5, Name: "T", AccessHash: 66, IsMin: true}
	require.NoError(t, c.Save(ctx, selfID, map[int64]telegram.PeerInfo{minCh.ChatID: minCh}))
	got, ok, err := c.Get(ctx, selfID, minCh.ChatID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(88), got.AccessHash)
	require.False(t, got.IsMin)
}
