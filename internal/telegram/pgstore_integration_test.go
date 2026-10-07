//go:build integration

package telegram_test

import (
	"context"
	"errors"
	"testing"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/telegram/updates"
	"github.com/stretchr/testify/require"

	"github.com/a1ekseev/chat-analyze/internal/pgtest"
	"github.com/a1ekseev/chat-analyze/internal/telegram"
)

func TestSessionStorage_RoundTrip(t *testing.T) {
	ctx := context.Background()
	s := telegram.NewSessionStorage(pgtest.New(t), "main")

	_, err := s.LoadSession(ctx)
	require.ErrorIs(t, err, session.ErrNotFound)

	require.NoError(t, s.StoreSession(ctx, []byte("one")))
	got, err := s.LoadSession(ctx)
	require.NoError(t, err)
	require.Equal(t, []byte("one"), got)

	require.NoError(t, s.StoreSession(ctx, []byte("two")))
	got, err = s.LoadSession(ctx)
	require.NoError(t, err)
	require.Equal(t, []byte("two"), got)
}

func TestStateStorage_SetWithoutStateFails(t *testing.T) {
	ctx := context.Background()
	s := telegram.NewStateStorage(pgtest.New(t))
	require.Error(t, s.SetPts(ctx, 1, 5))
	require.Error(t, s.SetQts(ctx, 1, 5))
	require.Error(t, s.SetDate(ctx, 1, 5))
	require.Error(t, s.SetSeq(ctx, 1, 5))
	require.Error(t, s.SetDateSeq(ctx, 1, 5, 6))
	_, found, err := s.GetState(ctx, 1)
	require.NoError(t, err)
	require.False(t, found)
}

func TestStateStorage_StateAndChannels(t *testing.T) {
	ctx := context.Background()
	s := telegram.NewStateStorage(pgtest.New(t))

	require.NoError(t, s.SetState(ctx, 1, updates.State{Pts: 1, Qts: 2, Date: 3, Seq: 4}))
	st, found, err := s.GetState(ctx, 1)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, updates.State{Pts: 1, Qts: 2, Date: 3, Seq: 4}, st)

	require.NoError(t, s.SetPts(ctx, 1, 10))
	require.NoError(t, s.SetQts(ctx, 1, 20))
	require.NoError(t, s.SetDate(ctx, 1, 30))
	require.NoError(t, s.SetSeq(ctx, 1, 40))
	st, _, _ = s.GetState(ctx, 1)
	require.Equal(t, updates.State{Pts: 10, Qts: 20, Date: 30, Seq: 40}, st)
	require.NoError(t, s.SetDateSeq(ctx, 1, 31, 41))
	st, _, _ = s.GetState(ctx, 1)
	require.Equal(t, updates.State{Pts: 10, Qts: 20, Date: 31, Seq: 41}, st)

	_, found, err = s.GetChannelPts(ctx, 1, 100)
	require.NoError(t, err)
	require.False(t, found)
	require.NoError(t, s.SetChannelPts(ctx, 1, 100, 7))
	require.NoError(t, s.SetChannelPts(ctx, 1, 200, 8))
	require.NoError(t, s.SetChannelPts(ctx, 1, 200, 9))
	require.NoError(t, s.SetChannelPts(ctx, 2, 300, 1))
	pts, found, err := s.GetChannelPts(ctx, 1, 200)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 9, pts)

	seen := map[int64]int{}
	require.NoError(t, s.ForEachChannels(ctx, 1, func(_ context.Context, id int64, pts int) error {
		seen[id] = pts
		return nil
	}))
	require.Equal(t, map[int64]int{100: 7, 200: 9}, seen)

	boom := errors.New("boom")
	require.ErrorIs(t, s.ForEachChannels(ctx, 1, func(context.Context, int64, int) error { return boom }), boom)
}

func TestStateStorage_AccessHashes(t *testing.T) {
	ctx := context.Background()
	s := telegram.NewStateStorage(pgtest.New(t))

	_, found, err := s.GetChannelAccessHash(ctx, 1, 5)
	require.NoError(t, err)
	require.False(t, found)

	require.NoError(t, s.SetChannelAccessHash(ctx, 1, 5, 111))
	require.NoError(t, s.SetUserAccessHash(ctx, 1, 5, 222))
	h, found, err := s.GetChannelAccessHash(ctx, 1, 5)
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, 111, h)
	h, found, err = s.GetUserAccessHash(ctx, 1, 5)
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, 222, h)

	require.NoError(t, s.SetUserAccessHash(ctx, 1, 5, 333))
	h, _, _ = s.GetUserAccessHash(ctx, 1, 5)
	require.EqualValues(t, 333, h)
	h, _, _ = s.GetChannelAccessHash(ctx, 1, 5)
	require.EqualValues(t, 111, h)
}

func TestPeerKV_SaveFind(t *testing.T) {
	ctx := context.Background()
	kv := telegram.NewPeerKV(pgtest.New(t))
	k := peers.Key{Prefix: "channel_", ID: 77}

	_, found, err := kv.Find(ctx, k)
	require.NoError(t, err)
	require.False(t, found)

	require.NoError(t, kv.Save(ctx, k, peers.Value{AccessHash: 5}))
	v, found, err := kv.Find(ctx, k)
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, 5, v.AccessHash)

	require.NoError(t, kv.Save(ctx, k, peers.Value{AccessHash: 6}))
	v, _, _ = kv.Find(ctx, k)
	require.EqualValues(t, 6, v.AccessHash)

	_, found, _ = kv.Find(ctx, peers.Key{Prefix: "users_", ID: 77})
	require.False(t, found)
}

func TestPeerKV_PhoneAndContactsHash(t *testing.T) {
	ctx := context.Background()
	kv := telegram.NewPeerKV(pgtest.New(t))

	h, err := kv.GetContactsHash(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 0, h)
	require.NoError(t, kv.SaveContactsHash(ctx, -99))
	require.NoError(t, kv.SaveContactsHash(ctx, 123))
	h, err = kv.GetContactsHash(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 123, h)

	k := peers.Key{Prefix: "users_", ID: 9}
	_, _, found, err := kv.FindPhone(ctx, "+100")
	require.NoError(t, err)
	require.False(t, found)

	// Phone saved for a peer without a saved value: not found.
	require.NoError(t, kv.SavePhone(ctx, "+100", k))
	_, _, found, err = kv.FindPhone(ctx, "+100")
	require.NoError(t, err)
	require.False(t, found)

	// Value is read at lookup time, not at SavePhone time.
	require.NoError(t, kv.Save(ctx, k, peers.Value{AccessHash: 5}))
	require.NoError(t, kv.Save(ctx, k, peers.Value{AccessHash: 6}))
	gk, gv, found, err := kv.FindPhone(ctx, "+100")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, k, gk)
	require.EqualValues(t, 6, gv.AccessHash)
}

func TestStateStorage_SetStateClearsChannelPts(t *testing.T) {
	ctx := context.Background()
	s := telegram.NewStateStorage(pgtest.New(t))
	require.NoError(t, s.SetState(ctx, 1, updates.State{Pts: 1}))
	require.NoError(t, s.SetChannelPts(ctx, 1, 100, 7))
	require.NoError(t, s.SetChannelPts(ctx, 2, 100, 8))
	require.NoError(t, s.SetState(ctx, 1, updates.State{Pts: 2}))
	_, found, err := s.GetChannelPts(ctx, 1, 100)
	require.NoError(t, err)
	require.False(t, found)
	pts, found, _ := s.GetChannelPts(ctx, 2, 100)
	require.True(t, found)
	require.Equal(t, 8, pts)
}

func TestSessionStorage_EmptyBlobIsNotFound(t *testing.T) {
	ctx := context.Background()
	s := telegram.NewSessionStorage(pgtest.New(t), "main")
	require.NoError(t, s.StoreSession(ctx, []byte{}))
	_, err := s.LoadSession(ctx)
	require.ErrorIs(t, err, session.ErrNotFound)
}
