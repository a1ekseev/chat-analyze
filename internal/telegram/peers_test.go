package telegram_test

import (
	"testing"

	"github.com/gotd/td/tg"
	"github.com/stretchr/testify/require"

	"github.com/a1ekseev/chat-analyze/internal/jobs"
	"github.com/a1ekseev/chat-analyze/internal/telegram"
)

func TestPeerFromUser(t *testing.T) {
	p := telegram.PeerFromUser(&tg.User{ID: 7, FirstName: " Ann ", LastName: "Lee", Username: "ann", AccessHash: 5, Self: true})
	require.Equal(t, jobs.PeerUser, p.Kind)
	require.Equal(t, int64(7), p.ChatID)
	require.Equal(t, "Ann  Lee", p.Name)
	require.Equal(t, "ann", p.Username)
	require.True(t, p.IsSelf)
	require.False(t, p.IsMin)

	p = telegram.PeerFromUser(&tg.User{ID: 8, FirstName: "Bob"})
	require.Equal(t, "Bob", p.Name)

	p = telegram.PeerFromUser(&tg.User{ID: 9, Username: "nick"})
	require.Equal(t, "nick", p.Name)

	p = telegram.PeerFromUser(&tg.User{ID: 10, Deleted: true})
	require.Empty(t, p.Name)
	require.Empty(t, p.Username)

	p = telegram.PeerFromUser(&tg.User{ID: 11, Min: true, Usernames: []tg.Username{
		{Username: "old", Active: false}, {Username: "new", Active: true},
	}})
	require.Equal(t, "new", p.Username)
	require.Equal(t, "new", p.Name)
	require.True(t, p.IsMin)
}

func TestPeerFromChat(t *testing.T) {
	p := telegram.PeerFromChat(&tg.Chat{ID: 55, Title: "Group"})
	require.Equal(t, int64(-55), p.ChatID)
	require.Equal(t, jobs.PeerChat, p.Kind)
	require.Equal(t, int64(55), p.ID)
	require.Equal(t, "Group", p.Name)
}

func TestPeerFromChannel_MarkedID(t *testing.T) {
	p := telegram.PeerFromChannel(&tg.Channel{
		ID: 4001, Title: "News", Broadcast: true, Forum: true, AccessHash: 9, Min: true,
		Usernames: []tg.Username{{Username: "news", Active: true}},
	})
	require.Equal(t, int64(-1000000004001), p.ChatID)
	require.Equal(t, jobs.PeerChannel, p.Kind)
	require.Equal(t, int64(4001), p.ID)
	require.Equal(t, "News", p.Name)
	require.Equal(t, "news", p.Username)
	require.True(t, p.IsBroadcast)
	require.True(t, p.IsForum)
	require.True(t, p.IsMin)
	require.Equal(t, int64(9), p.AccessHash)
}

func TestPeersFromEntities(t *testing.T) {
	m := telegram.PeersFromEntities(
		map[int64]*tg.User{1: {ID: 1, FirstName: "A"}},
		map[int64]*tg.Chat{2: {ID: 2, Title: "C"}},
		map[int64]*tg.Channel{3: {ID: 3, Title: "D"}},
	)
	require.Len(t, m, 3)
	require.Equal(t, "A", m[1].Name)
	require.Equal(t, "C", m[-2].Name)
	require.Equal(t, "D", m[-1000000000003].Name)
}

func TestPeerCache_Self(t *testing.T) {
	c := telegram.NewPeerCache(nil)
	require.Equal(t, telegram.PeerInfo{}, c.Self())
	c.SetSelf(&tg.User{ID: 5, FirstName: "Me", Self: true})
	require.Equal(t, int64(5), c.Self().ID)
	require.Equal(t, "Me", c.Self().Name)
}
