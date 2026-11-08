package telegram_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/a1ekseev/chat-analyze/internal/jobs"
	"github.com/a1ekseev/chat-analyze/internal/telegram"
	"github.com/a1ekseev/chat-analyze/internal/telegram/mocks"
)

const (
	userA    = int64(10) // private chat
	groupRaw = int64(20) // basic group: marked -20
	chanRaw  = int64(30) // supergroup / forum
	chanMark = int64(-1000000000030)
)

type mark struct {
	chat   int64
	kind   string
	hash   int64
	thread int
	max    int
}

type fakeReader struct {
	mu    sync.Mutex
	marks []mark
}

func (r *fakeReader) MarkRead(_ context.Context, chatID int64, t jobs.ReadTarget, maxID int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.marks = append(r.marks, mark{chatID, t.PeerKind, t.AccessHash, t.ThreadID, maxID})
	return nil
}

// chatRepo stores the last message id per chat.
type chatRepo struct{ last map[int64]int }

func (r *chatRepo) UpdateViews(context.Context, int64, int, int) error { return nil }

func (r *chatRepo) LastMessageID(_ context.Context, chatID int64) (int, bool, error) {
	id, ok := r.last[chatID]
	return id, ok, nil
}

type syncEnv struct {
	api    *mocks.MockAPI
	l      *telegram.Listener
	s      *telegram.DialogSync
	q      *enq
	repo   *chatRepo
	peers  *fakePeers
	reader *fakeReader
	buf    *lockedBuf
	sleeps *[]time.Duration
}

func newSync(t *testing.T, markRead bool) syncEnv {
	t.Helper()
	api := mocks.NewMockAPI(gomock.NewController(t))
	e := syncEnv{
		api: api, q: &enq{}, repo: &chatRepo{last: map[int64]int{}}, peers: &fakePeers{},
		reader: &fakeReader{}, buf: &lockedBuf{},
	}
	e.l = telegram.NewListener(e.q, e.repo, e.peers, api,
		slog.New(slog.NewTextHandler(e.buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	e.sleeps = fakeSleep(e.l)
	telegram.SetBackfillPagePause(e.l, time.Second)
	e.s = telegram.NewDialogSync(e.l, e.reader, markRead)
	return e
}

func user(id int64) *tg.User { return &tg.User{ID: id, AccessHash: id * 100, FirstName: "secret name"} }

func supergroup(forum bool) *tg.Channel {
	return &tg.Channel{ID: chanRaw, AccessHash: 3000, Megagroup: true, Forum: forum, Title: "secret title"}
}

func dlg(peer tg.PeerClass, top, readInbox, unread int) *tg.Dialog {
	return &tg.Dialog{Peer: peer, TopMessage: top, ReadInboxMaxID: readInbox, UnreadCount: unread}
}

// expectDialogs serves main and archive lists (each a single page) in order.
func (e syncEnv) expectDialogs(main, archive *tg.MessagesDialogs) {
	gomock.InOrder(
		e.api.EXPECT().MessagesGetDialogs(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, r *tg.MessagesGetDialogsRequest) (tg.MessagesDialogsClass, error) {
				if r.FolderID != 0 {
					panic("main list first")
				}
				return main, nil
			}),
		e.api.EXPECT().MessagesGetDialogs(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, r *tg.MessagesGetDialogsRequest) (tg.MessagesDialogsClass, error) {
				if r.FolderID != 1 {
					panic("archive second")
				}
				return archive, nil
			}),
	)
}

func empty() *tg.MessagesDialogs { return &tg.MessagesDialogs{} }

// Everything stored and read: the pass costs the two getDialogs pages only
// (gomock fails on any other RPC) and marks nothing.
func TestDialogSync_NothingToDo(t *testing.T) {
	e := newSync(t, true)
	e.repo.last = map[int64]int{userA: 50, -groupRaw: 7, chanMark: 900, 1: 3}
	self := &tg.User{ID: 1, Self: true, AccessHash: 1}
	main := &tg.MessagesDialogs{
		Dialogs: []tg.DialogClass{
			dlg(&tg.PeerUser{UserID: userA}, 50, 49, 0), // last message outgoing: read_inbox lags
			dlg(&tg.PeerChat{ChatID: groupRaw}, 7, 7, 0),
			dlg(&tg.PeerUser{UserID: 1}, 3, 3, 0), // Saved Messages
			&tg.DialogFolder{},
		},
		Users: []tg.UserClass{user(userA), self},
		Chats: []tg.ChatClass{&tg.Chat{ID: groupRaw, Title: "g"}},
	}
	archive := &tg.MessagesDialogs{
		Dialogs: []tg.DialogClass{dlg(&tg.PeerChannel{ChannelID: chanRaw}, 900, 900, 0)},
		Chats:   []tg.ChatClass{supergroup(true)},
	}
	e.expectDialogs(main, archive)
	require.NoError(t, e.s.SyncDialogs(t.Context()))
	assert.Empty(t, e.q.bf)
	assert.Empty(t, e.reader.marks)
	assert.Equal(t, []time.Duration{time.Second}, *e.sleeps, "pages are paced")
	assert.Contains(t, e.peers.saved, chanMark, "dialog entities are stored")
	out := e.buf.String()
	assert.Contains(t, out, `level=INFO msg="dialog sync done" component=telegram dialogs=4 skipped=0 chats_behind=0 `+
		`messages_behind=0 chats_marked_read=0 pages=2 duration_ms=`)
	assert.NotContains(t, out, "secret")
}

// A chat with stored history behind Telegram gets a backfill from the last
// stored id; its save_message jobs mark it read, so no direct mark.
func TestDialogSync_BehindEnqueuesBackfill(t *testing.T) {
	e := newSync(t, true)
	e.repo.last = map[int64]int{chanMark: 40, userA: 5}
	e.expectDialogs(&tg.MessagesDialogs{
		Dialogs: []tg.DialogClass{
			dlg(&tg.PeerChannel{ChannelID: chanRaw}, 50, 40, 10),
			dlg(&tg.PeerUser{UserID: userA}, 9, 9, 0), // behind, nothing unread
		},
		Users: []tg.UserClass{user(userA)},
		Chats: []tg.ChatClass{supergroup(false)},
	}, empty())
	require.NoError(t, e.s.SyncDialogs(t.Context()))
	assert.Equal(t, []jobs.BackfillHistory{{ChatID: chanMark, MinID: 40}, {ChatID: userA, MinID: 5}}, e.q.bf)
	assert.Equal(t, []string{"-1000000000030", "10"}, e.q.bfKeys)
	assert.Equal(t, jobs.BackfillMinIDField, e.q.bfField)
	assert.Empty(t, e.reader.marks)
	assert.Contains(t, e.buf.String(), "chats_behind=2 messages_behind=14 chats_marked_read=0")
}

// No stored history: only the unread messages (min_id = read_inbox_max_id,
// pages bounded by unread_count and the cap) are fetched, then marked read
// by their save_message jobs.
func TestDialogSync_NoHistoryFetchesOnlyUnread(t *testing.T) {
	e := newSync(t, true)
	e.expectDialogs(&tg.MessagesDialogs{
		Dialogs: []tg.DialogClass{
			dlg(&tg.PeerUser{UserID: userA}, 50, 45, 5),
			dlg(&tg.PeerChannel{ChannelID: chanRaw}, 90000, 100, 50000),
			dlg(&tg.PeerChat{ChatID: groupRaw}, 70, 70, 0), // no history, nothing unread: left alone
		},
		Users: []tg.UserClass{user(userA)},
		Chats: []tg.ChatClass{supergroup(false), &tg.Chat{ID: groupRaw}},
	}, empty())
	require.NoError(t, e.s.SyncDialogs(t.Context()))
	assert.Equal(t, []jobs.BackfillHistory{
		{ChatID: userA, MinID: 45, MaxPages: 1},
		{ChatID: chanMark, MinID: 100, MaxPages: 10},
	}, e.q.bf)
	assert.Empty(t, e.reader.marks)

	// The backfill then fetches only messages above read_inbox_max_id, one page.
	e.peers.hash = e.peers.saved
	e.api.EXPECT().MessagesGetHistory(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
		func(_ context.Context, r *tg.MessagesGetHistoryRequest) (tg.MessagesMessagesClass, error) {
			assert.Equal(t, &tg.InputPeerUser{UserID: userA, AccessHash: userA * 100}, r.Peer)
			assert.Equal(t, 45, r.MinID)
			res := &tg.MessagesMessages{Users: []tg.UserClass{user(userA)}}
			for id := 50; id > 45; id-- {
				res.Messages = append(res.Messages, &tg.Message{ID: id, PeerID: &tg.PeerUser{UserID: userA}, Date: 1})
			}
			return res, nil
		})
	require.NoError(t, e.l.Backfill(t.Context(), userA, 45, 1))
	assert.Equal(t, []int{46, 47, 48, 49, 50}, e.q.order)
	assert.Equal(t, jobs.ReadTarget{PeerKind: jobs.PeerUser, PeerID: userA, AccessHash: userA * 100}, e.q.pl[4].Read)
	assert.NotContains(t, e.buf.String(), "backfill truncated", "a capped fetch is expected to stop early")
}

// Everything stored but unread: marked read through the reader up to
// top_message; unread_mark, mentions and reactions are cleared with one RPC each.
func TestDialogSync_UpToDateMarksRead(t *testing.T) {
	e := newSync(t, true)
	e.repo.last = map[int64]int{userA: 50, 1: 8, -groupRaw: 7}
	marked := dlg(&tg.PeerChat{ChatID: groupRaw}, 7, 7, 0)
	marked.UnreadMark = true
	mentions := dlg(&tg.PeerUser{UserID: userA}, 50, 40, 3)
	mentions.UnreadMentionsCount, mentions.UnreadReactionsCount = 1, 2
	e.expectDialogs(&tg.MessagesDialogs{
		Dialogs: []tg.DialogClass{mentions, dlg(&tg.PeerUser{UserID: 1}, 8, 6, 2), marked},
		Users:   []tg.UserClass{user(userA), &tg.User{ID: 1, Self: true}},
		Chats:   []tg.ChatClass{&tg.Chat{ID: groupRaw}},
	}, empty())
	userPeer := &tg.InputPeerUser{UserID: userA, AccessHash: userA * 100}
	e.api.EXPECT().MessagesReadMentions(gomock.Any(), &tg.MessagesReadMentionsRequest{Peer: userPeer}).
		Return(&tg.MessagesAffectedHistory{}, nil)
	e.api.EXPECT().MessagesReadReactions(gomock.Any(), &tg.MessagesReadReactionsRequest{Peer: userPeer}).
		Return(&tg.MessagesAffectedHistory{}, nil)
	e.api.EXPECT().MessagesMarkDialogUnread(gomock.Any(), &tg.MessagesMarkDialogUnreadRequest{
		Peer: &tg.InputDialogPeer{Peer: &tg.InputPeerChat{ChatID: groupRaw}},
	}).Return(true, nil)
	require.NoError(t, e.s.SyncDialogs(t.Context()))
	assert.Empty(t, e.q.bf)
	assert.Equal(t, []mark{
		{chat: userA, kind: jobs.PeerUser, hash: userA * 100, max: 50},
		{chat: 1, kind: jobs.PeerSelf, max: 8},
	}, e.reader.marks)
	assert.Contains(t, e.buf.String(), "chats_marked_read=3")
}

// Main list and archive are both paged with offsets from the last dialog.
func TestDialogSync_ArchivePaged(t *testing.T) {
	e := newSync(t, true)
	full := &tg.MessagesDialogsSlice{Count: 101}
	for id := int64(100); id >= 1; id-- {
		uid := 1000 + id
		e.repo.last[uid] = int(id)
		full.Dialogs = append(full.Dialogs, dlg(&tg.PeerUser{UserID: uid}, int(id), int(id), 0))
		full.Users = append(full.Users, user(uid))
		full.Messages = append(full.Messages, &tg.Message{ID: int(id), PeerID: &tg.PeerUser{UserID: uid}, Date: int(5000 + id)})
	}
	e.repo.last[2000] = 1
	tail := &tg.MessagesDialogsSlice{
		Count: 101, Dialogs: []tg.DialogClass{dlg(&tg.PeerUser{UserID: 2000}, 1, 1, 0)},
		Users: []tg.UserClass{user(2000)},
	}
	var reqs []tg.MessagesGetDialogsRequest
	calls := 0
	e.api.EXPECT().MessagesGetDialogs(gomock.Any(), gomock.Any()).Times(3).DoAndReturn(
		func(_ context.Context, r *tg.MessagesGetDialogsRequest) (tg.MessagesDialogsClass, error) {
			reqs = append(reqs, *r)
			calls++
			switch calls {
			case 1:
				return empty(), nil
			case 2:
				return full, nil
			}
			return tail, nil
		})
	require.NoError(t, e.s.SyncDialogs(t.Context()))
	require.Len(t, reqs, 3)
	assert.Equal(t, 0, reqs[0].FolderID)
	assert.Equal(t, 1, reqs[1].FolderID)
	assert.Equal(t, &tg.InputPeerEmpty{}, reqs[1].OffsetPeer)
	assert.Equal(t, 1, reqs[2].FolderID)
	assert.Equal(t, 1, reqs[2].OffsetID)
	assert.Equal(t, 5001, reqs[2].OffsetDate)
	assert.Equal(t, &tg.InputPeerUser{UserID: 1001, AccessHash: 100100}, reqs[2].OffsetPeer)
	assert.Len(t, *e.sleeps, 2)
	assert.Contains(t, e.buf.String(), "dialogs=101 ")
}

// Chats the account left or was kicked from are skipped without RPCs.
func TestDialogSync_ForbiddenSkipped(t *testing.T) {
	e := newSync(t, true)
	e.expectDialogs(&tg.MessagesDialogs{
		Dialogs: []tg.DialogClass{
			dlg(&tg.PeerChannel{ChannelID: chanRaw}, 50, 1, 49),
			dlg(&tg.PeerChat{ChatID: groupRaw}, 50, 1, 49),
			dlg(&tg.PeerChat{ChatID: 21}, 50, 1, 49),
		},
		Chats: []tg.ChatClass{
			&tg.ChannelForbidden{ID: chanRaw, AccessHash: 1},
			&tg.ChatForbidden{ID: groupRaw},
			&tg.Chat{ID: 21, Deactivated: true},
		},
	}, empty())
	require.NoError(t, e.s.SyncDialogs(t.Context()))
	assert.Empty(t, e.q.bf)
	assert.Empty(t, e.reader.marks)
	out := e.buf.String()
	assert.Contains(t, out, "dialogs=3 skipped=3")
	assert.Contains(t, out, `msg="dialog skipped" component=telegram chat_id=-1000000000030 reason=forbidden`)
	assert.Contains(t, out, `chat_id=-21 reason=left`)
}

// MARK_READ=false: fetch only, never mark or clear anything.
func TestDialogSync_MarkReadDisabled(t *testing.T) {
	e := newSync(t, false)
	e.repo.last = map[int64]int{chanMark: 40, userA: 50}
	behind := dlg(&tg.PeerChannel{ChannelID: chanRaw}, 50, 40, 10)
	behind.UnreadMark, behind.UnreadMentionsCount = true, 1
	e.expectDialogs(&tg.MessagesDialogs{
		Dialogs: []tg.DialogClass{behind, dlg(&tg.PeerUser{UserID: userA}, 50, 40, 3)},
		Users:   []tg.UserClass{user(userA)},
		Chats:   []tg.ChatClass{supergroup(true)},
	}, empty())
	require.NoError(t, e.s.SyncDialogs(t.Context()))
	assert.Equal(t, []jobs.BackfillHistory{{ChatID: chanMark, MinID: 40}}, e.q.bf)
	assert.Empty(t, e.reader.marks)
}

// A min entity gets the stored full access hash; without one it is skipped.
func TestDialogSync_MinPeerUsesStoredHash(t *testing.T) {
	e := newSync(t, true)
	e.repo.last = map[int64]int{chanMark: 50, userA: 50}
	e.peers.hash = map[int64]telegram.PeerInfo{chanMark: {ChatID: chanMark, Kind: jobs.PeerChannel, ID: chanRaw, AccessHash: 3000}}
	minChan := supergroup(false)
	minChan.Min, minChan.AccessHash = true, 0
	e.expectDialogs(&tg.MessagesDialogs{
		Dialogs: []tg.DialogClass{
			dlg(&tg.PeerChannel{ChannelID: chanRaw}, 50, 40, 10),
			dlg(&tg.PeerUser{UserID: userA}, 50, 40, 10),
		},
		Users: []tg.UserClass{&tg.User{ID: userA, Min: true}},
		Chats: []tg.ChatClass{minChan},
	}, empty())
	require.NoError(t, e.s.SyncDialogs(t.Context()))
	assert.Equal(t, []mark{{chat: chanMark, kind: jobs.PeerChannel, hash: 3000, max: 50}}, e.reader.marks)
	assert.Contains(t, e.buf.String(), `chat_id=10 reason=no_access_hash`)
}

// FLOOD_WAIT fails the pass; the worker releases the job for X+1s.
func TestDialogSync_FloodWaitReturned(t *testing.T) {
	e := newSync(t, true)
	e.api.EXPECT().MessagesGetDialogs(gomock.Any(), gomock.Any()).Return(nil, tgerr.New(420, "FLOOD_WAIT_9"))
	_, ok := tgerr.AsFloodWait(e.s.SyncDialogs(t.Context()))
	require.True(t, ok)
}

// Forum with unread: getForumTopics; unread topics whose messages are all
// stored are marked read (topic id as thread, General with the chat), newer
// topics are left to the chat backfill's save_message jobs.
func TestDialogSync_ForumTopics(t *testing.T) {
	e := newSync(t, true)
	e.repo.last = map[int64]int{chanMark: 100}
	e.expectDialogs(&tg.MessagesDialogs{
		Dialogs: []tg.DialogClass{dlg(&tg.PeerChannel{ChannelID: chanRaw}, 120, 80, 9)},
		Chats:   []tg.ChatClass{supergroup(true)},
	}, empty())
	input := &tg.InputPeerChannel{ChannelID: chanRaw, AccessHash: 3000}
	e.api.EXPECT().MessagesGetForumTopics(gomock.Any(), &tg.MessagesGetForumTopicsRequest{Peer: input, Limit: 100}).
		Return(&tg.MessagesForumTopics{Topics: []tg.ForumTopicClass{
			&tg.ForumTopic{ID: 1, TopMessage: 90, ReadInboxMaxID: 85, UnreadCount: 1},
			&tg.ForumTopic{ID: 7, TopMessage: 95, ReadInboxMaxID: 93, UnreadCount: 2},
			&tg.ForumTopic{ID: 9, TopMessage: 120, ReadInboxMaxID: 99, UnreadCount: 6}, // not stored yet
			&tg.ForumTopic{ID: 11, TopMessage: 80, ReadInboxMaxID: 80},                 // read
			&tg.ForumTopicDeleted{ID: 12},
		}}, nil)
	require.NoError(t, e.s.SyncDialogs(t.Context()))
	assert.Equal(t, []jobs.BackfillHistory{{ChatID: chanMark, MinID: 100}}, e.q.bf)
	assert.Equal(t, []mark{
		{chat: chanMark, kind: jobs.PeerChannel, hash: 3000, max: 90},
		{chat: chanMark, kind: jobs.PeerChannel, hash: 3000, thread: 7, max: 95},
	}, e.reader.marks)
	assert.Equal(t, []time.Duration{time.Second, time.Second}, *e.sleeps, "topic pages are paced too")
}

// Topics are paged with offsets from the last topic; an up-to-date forum is
// read at chat level too.
func TestDialogSync_ForumTopicsPaged(t *testing.T) {
	e := newSync(t, true)
	e.repo.last = map[int64]int{chanMark: 500}
	e.expectDialogs(&tg.MessagesDialogs{
		Dialogs: []tg.DialogClass{dlg(&tg.PeerChannel{ChannelID: chanRaw}, 500, 400, 1)},
		Chats:   []tg.ChatClass{supergroup(true)},
	}, empty())
	first := &tg.MessagesForumTopics{}
	for id := 200; id > 100; id-- {
		first.Topics = append(first.Topics, &tg.ForumTopic{ID: id, TopMessage: id + 200, ReadInboxMaxID: id + 200})
		first.Messages = append(first.Messages, &tg.Message{ID: id + 200, PeerID: &tg.PeerChannel{ChannelID: chanRaw}, Date: id})
	}
	var reqs []tg.MessagesGetForumTopicsRequest
	gomock.InOrder(
		e.api.EXPECT().MessagesGetForumTopics(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, r *tg.MessagesGetForumTopicsRequest) (*tg.MessagesForumTopics, error) {
				reqs = append(reqs, *r)
				return first, nil
			}),
		e.api.EXPECT().MessagesGetForumTopics(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, r *tg.MessagesGetForumTopicsRequest) (*tg.MessagesForumTopics, error) {
				reqs = append(reqs, *r)
				return &tg.MessagesForumTopics{Topics: []tg.ForumTopicClass{
					&tg.ForumTopic{ID: 50, TopMessage: 500, ReadInboxMaxID: 499, UnreadCount: 1},
				}}, nil
			}),
	)
	require.NoError(t, e.s.SyncDialogs(t.Context()))
	require.Len(t, reqs, 2)
	assert.Equal(t, 101, reqs[1].OffsetTopic)
	assert.Equal(t, 301, reqs[1].OffsetID)
	assert.Equal(t, 101, reqs[1].OffsetDate)
	assert.Equal(t, []mark{
		{chat: chanMark, kind: jobs.PeerChannel, hash: 3000, max: 500},
		{chat: chanMark, kind: jobs.PeerChannel, hash: 3000, thread: 50, max: 500},
	}, e.reader.marks)
}

// Backfilled messages map exactly like live updates: forum topic and reply
// thread ids, and the ReadTarget.ThreadID the reader uses for readDiscussion.
func TestBackfill_ThreadIDsMatchLiveUpdates(t *testing.T) {
	forumTopic := func(id int) *tg.Message {
		rh := &tg.MessageReplyHeader{ForumTopic: true}
		rh.SetReplyToMsgID(7)
		return &tg.Message{ID: id, PeerID: &tg.PeerChannel{ChannelID: chanRaw}, Date: 1, ReplyTo: rh}
	}
	replyThread := func(id int) *tg.Message {
		rh := &tg.MessageReplyHeader{}
		rh.SetReplyToMsgID(id - 1)
		rh.SetReplyToTopID(40) // comment thread / reply thread of post 40
		return &tg.Message{ID: id, PeerID: &tg.PeerChannel{ChannelID: chanRaw}, Date: 1, ReplyTo: rh}
	}
	for name, tc := range map[string]struct {
		forum  bool
		msg    func(int) *tg.Message
		thread int
	}{"forum topic": {true, forumTopic, 7}, "reply thread": {false, replyThread, 40}} {
		t.Run(name, func(t *testing.T) {
			chats := []tg.ChatClass{supergroup(tc.forum)}
			live := setup(t, nil)
			require.NoError(t, live.d.Handle(t.Context(), &tg.Updates{
				Chats:   chats,
				Updates: []tg.UpdateClass{&tg.UpdateNewChannelMessage{Message: tc.msg(61)}},
			}))

			e := newSync(t, true)
			e.peers.hash = map[int64]telegram.PeerInfo{chanMark: {ChatID: chanMark, Kind: jobs.PeerChannel, ID: chanRaw, AccessHash: 3000}}
			gomock.InOrder(
				e.api.EXPECT().MessagesGetHistory(gomock.Any(), gomock.Any()).
					Return(&tg.MessagesChannelMessages{Chats: chats, Messages: []tg.MessageClass{tc.msg(61)}}, nil),
				e.api.EXPECT().MessagesGetHistory(gomock.Any(), gomock.Any()).Return(&tg.MessagesChannelMessages{}, nil),
			)
			require.NoError(t, e.l.Backfill(t.Context(), chanMark, 60, 0))

			require.Len(t, live.q.pl, 1)
			require.Len(t, e.q.pl, 1)
			got := e.q.pl[0]
			assert.Equal(t, tc.thread, got.Read.ThreadID)
			assert.Equal(t, live.q.pl[0].Read, got.Read)
			assert.Equal(t, live.q.pl[0].Message.TopicID, got.Message.TopicID)
			assert.Equal(t, live.q.pl[0].Message.ThreadTopID, got.Message.ThreadTopID)
		})
	}
}

// RPC cost: 150 dialogs with nothing missing cost 3 getDialogs pages
// (2 main + 1 archive) and nothing else; N chats behind add no RPC to the
// pass, only N backfill jobs (each then 2 getHistory for a small gap).
func TestDialogSync_RPCCost(t *testing.T) {
	for _, behind := range []int{0, 10} {
		e := newSync(t, true)
		var pages [][]tg.DialogClass
		var users []tg.UserClass
		var cur []tg.DialogClass
		for i := range 150 {
			uid := int64(5000 + i)
			users = append(users, user(uid))
			e.repo.last[uid] = 100
			top := 100
			if i < behind {
				top = 103
			}
			cur = append(cur, dlg(&tg.PeerUser{UserID: uid}, top, top, 0))
			if len(cur) == 100 {
				pages, cur = append(pages, cur), nil
			}
		}
		pages = append(pages, cur)
		getDialogs := 0
		e.api.EXPECT().MessagesGetDialogs(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
			func(_ context.Context, r *tg.MessagesGetDialogsRequest) (tg.MessagesDialogsClass, error) {
				getDialogs++
				if r.FolderID == 1 {
					return &tg.MessagesDialogs{}, nil
				}
				p := pages[0]
				if r.OffsetID != 0 {
					p = pages[1]
				}
				return &tg.MessagesDialogsSlice{Count: 150, Dialogs: p, Users: users}, nil
			})
		require.NoError(t, e.s.SyncDialogs(t.Context()))
		assert.Equal(t, 3, getDialogs)
		assert.Len(t, e.q.bf, behind)
		t.Logf("dialogs=150 behind=%d: pass RPCs=%d (getDialogs), backfill jobs=%d", behind, getDialogs, len(e.q.bf))
	}
}
