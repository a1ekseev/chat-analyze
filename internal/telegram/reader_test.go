package telegram_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/a1ekseev/chat-analyze/internal/jobs"
	"github.com/a1ekseev/chat-analyze/internal/telegram"
	"github.com/a1ekseev/chat-analyze/internal/telegram/mocks"
)

type memMarks struct{ m map[[2]int64]int }

func newMemMarks() *memMarks { return &memMarks{m: map[[2]int64]int{}} }

func (s *memMarks) Get(_ context.Context, chatID int64, threadID int) (int, bool, error) {
	v, ok := s.m[[2]int64{chatID, int64(threadID)}]
	return v, ok, nil
}

func (s *memMarks) Set(_ context.Context, chatID int64, threadID, maxID int) error {
	k := [2]int64{chatID, int64(threadID)}
	if maxID > s.m[k] {
		s.m[k] = maxID
	}
	return nil
}

func newReader(t *testing.T) (*telegram.Reader, *mocks.MockAPI, *memMarks) {
	t.Helper()
	api := mocks.NewMockAPI(gomock.NewController(t))
	marks := newMemMarks()
	return telegram.NewReader(api, marks, slog.New(slog.DiscardHandler)), api, marks
}

func TestMarkRead_User(t *testing.T) {
	r, api, marks := newReader(t)
	api.EXPECT().MessagesReadHistory(gomock.Any(), &tg.MessagesReadHistoryRequest{
		Peer: &tg.InputPeerUser{UserID: 2001, AccessHash: 99}, MaxID: 5,
	}).Return(&tg.MessagesAffectedMessages{}, nil)
	require.NoError(t, r.MarkRead(context.Background(), 2001, jobs.ReadTarget{PeerKind: jobs.PeerUser, PeerID: 2001, AccessHash: 99}, 5))
	require.Equal(t, 5, marks.m[[2]int64{2001, 0}])
}

func TestMarkRead_Self(t *testing.T) {
	r, api, _ := newReader(t)
	api.EXPECT().MessagesReadHistory(gomock.Any(), &tg.MessagesReadHistoryRequest{
		Peer: &tg.InputPeerSelf{}, MaxID: 5,
	}).Return(&tg.MessagesAffectedMessages{}, nil)
	require.NoError(t, r.MarkRead(context.Background(), 1, jobs.ReadTarget{PeerKind: jobs.PeerSelf, PeerID: 1}, 5))
}

func TestMarkRead_Chat(t *testing.T) {
	r, api, _ := newReader(t)
	api.EXPECT().MessagesReadHistory(gomock.Any(), &tg.MessagesReadHistoryRequest{
		Peer: &tg.InputPeerChat{ChatID: 3001}, MaxID: 5,
	}).Return(&tg.MessagesAffectedMessages{}, nil)
	require.NoError(t, r.MarkRead(context.Background(), -3001, jobs.ReadTarget{PeerKind: jobs.PeerChat, PeerID: 3001}, 5))
}

func TestMarkRead_Channel(t *testing.T) {
	r, api, marks := newReader(t)
	api.EXPECT().ChannelsReadHistory(gomock.Any(), &tg.ChannelsReadHistoryRequest{
		Channel: &tg.InputChannel{ChannelID: 4001, AccessHash: 77}, MaxID: 5,
	}).Return(true, nil)
	require.NoError(t, r.MarkRead(context.Background(), -1004001, jobs.ReadTarget{PeerKind: jobs.PeerChannel, PeerID: 4001, AccessHash: 77}, 5))
	require.Equal(t, 5, marks.m[[2]int64{-1004001, 0}])
}

func TestMarkRead_ForumTopic(t *testing.T) {
	r, api, marks := newReader(t)
	gomock.InOrder(
		api.EXPECT().ChannelsReadHistory(gomock.Any(), &tg.ChannelsReadHistoryRequest{
			Channel: &tg.InputChannel{ChannelID: 4001, AccessHash: 77}, MaxID: 5,
		}).Return(true, nil),
		api.EXPECT().MessagesReadDiscussion(gomock.Any(), &tg.MessagesReadDiscussionRequest{
			Peer: &tg.InputPeerChannel{ChannelID: 4001, AccessHash: 77}, MsgID: 200, ReadMaxID: 5,
		}).Return(true, nil),
	)
	tgt := jobs.ReadTarget{PeerKind: jobs.PeerChannel, PeerID: 4001, AccessHash: 77, ThreadID: 200}
	require.NoError(t, r.MarkRead(context.Background(), -1004001, tgt, 5))
	require.Equal(t, 5, marks.m[[2]int64{-1004001, 0}])
	require.Equal(t, 5, marks.m[[2]int64{-1004001, 200}])
}

func TestMarkRead_ForumTopic_FirstFailsSkipsSecond(t *testing.T) {
	r, api, marks := newReader(t)
	api.EXPECT().ChannelsReadHistory(gomock.Any(), gomock.Any()).Return(false, tgerr.New(500, "INTERNAL"))
	tgt := jobs.ReadTarget{PeerKind: jobs.PeerChannel, PeerID: 4001, AccessHash: 77, ThreadID: 200}
	require.Error(t, r.MarkRead(context.Background(), -1004001, tgt, 5))
	require.Empty(t, marks.m)
}

func TestMarkRead_ForumTopic_ChatAlreadyReadStillReadsDiscussion(t *testing.T) {
	r, api, marks := newReader(t)
	marks.m[[2]int64{-1004001, 0}] = 10
	api.EXPECT().MessagesReadDiscussion(gomock.Any(), &tg.MessagesReadDiscussionRequest{
		Peer: &tg.InputPeerChannel{ChannelID: 4001, AccessHash: 77}, MsgID: 200, ReadMaxID: 5,
	}).Return(true, nil)
	tgt := jobs.ReadTarget{PeerKind: jobs.PeerChannel, PeerID: 4001, AccessHash: 77, ThreadID: 200}
	require.NoError(t, r.MarkRead(context.Background(), -1004001, tgt, 5))
	require.Equal(t, 5, marks.m[[2]int64{-1004001, 200}])
}

func TestMarkRead_SkipsMinAndNoHash(t *testing.T) {
	r, _, marks := newReader(t) // no expectations: any API call fails the test
	ctx := context.Background()
	for _, tgt := range []jobs.ReadTarget{
		{PeerKind: jobs.PeerUser, PeerID: 1, AccessHash: 9, IsMin: true},
		{PeerKind: jobs.PeerChat, PeerID: 1, IsMin: true},
		{PeerKind: jobs.PeerUser, PeerID: 1},
		{PeerKind: jobs.PeerChannel, PeerID: 1},
	} {
		require.NoError(t, r.MarkRead(ctx, 1, tgt, 5))
	}
	require.Empty(t, marks.m)
}

func TestMarkRead_AlreadyRead(t *testing.T) {
	r, _, marks := newReader(t)
	marks.m[[2]int64{2001, 0}] = 10
	require.NoError(t, r.MarkRead(context.Background(), 2001, jobs.ReadTarget{PeerKind: jobs.PeerUser, PeerID: 2001, AccessHash: 99}, 5))
	require.NoError(t, r.MarkRead(context.Background(), 2001, jobs.ReadTarget{PeerKind: jobs.PeerUser, PeerID: 2001, AccessHash: 99}, 10))
}

func TestMarkRead_UnknownKind(t *testing.T) {
	r, _, _ := newReader(t)
	require.Error(t, r.MarkRead(context.Background(), 1, jobs.ReadTarget{PeerKind: "bogus"}, 5))
}

func TestMarkRead_FloodWaitPropagates(t *testing.T) {
	r, api, marks := newReader(t)
	api.EXPECT().MessagesReadHistory(gomock.Any(), gomock.Any()).Return(nil, tgerr.New(420, "FLOOD_WAIT_5"))
	err := r.MarkRead(context.Background(), 2001, jobs.ReadTarget{PeerKind: jobs.PeerUser, PeerID: 2001, AccessHash: 99}, 5)
	d, ok := tgerr.AsFloodWait(err)
	require.True(t, ok)
	require.Equal(t, 5*time.Second, d)
	require.Empty(t, marks.m)
}
