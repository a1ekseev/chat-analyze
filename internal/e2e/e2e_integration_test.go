//go:build integration

// Package e2e_test drives real gotd update dispatching, the listener, the
// queue, the worker, the message repository and the Telegram read/peer stores
// on a real PostgreSQL, with only the Telegram RPC API mocked.
package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/gotd/log"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/a1ekseev/chat-analyze/internal/jobs"
	"github.com/a1ekseev/chat-analyze/internal/logx"
	"github.com/a1ekseev/chat-analyze/internal/message"
	"github.com/a1ekseev/chat-analyze/internal/pgtest"
	"github.com/a1ekseev/chat-analyze/internal/queue"
	"github.com/a1ekseev/chat-analyze/internal/telegram"
	"github.com/a1ekseev/chat-analyze/internal/telegram/mocks"
	"github.com/a1ekseev/chat-analyze/internal/worker"
)

const (
	canary = "CANARY-7f3a"

	selfID      = int64(1000)
	peerUserID  = int64(2000)
	senderID    = int64(3000)
	channelRaw  = int64(5)
	channelMark = int64(-1000000000005) // marked id of channel 5
	chanHash    = int64(777)
	userHash    = int64(888)
)

// env is the fully wired pipeline (everything real except Telegram RPC).
type env struct {
	ctx  context.Context
	pool *pgxpool.Pool
	q    *queue.Queue
	api  *mocks.MockAPI
	d    tg.UpdateDispatcher
	w    *worker.Worker
	bw   *worker.LongWorker
	sw   *worker.LongWorker
	l    *telegram.Listener
	r    *telegram.Reader
	buf  *bytes.Buffer
}

// newEnv wires the pipeline. wrap, if not nil, decorates the message store.
func newEnv(t *testing.T, wrap func(worker.MessageStore, *pgxpool.Pool) worker.MessageStore) *env {
	t.Helper()
	ctx := context.Background()
	pool := pgtest.New(t)
	buf := &bytes.Buffer{}
	logger := logx.New(buf, slog.LevelDebug, "json")
	api := mocks.NewMockAPI(gomock.NewController(t))
	q := queue.New(pool, 5)
	repo := message.NewRepository(pool)

	peers := telegram.NewPeerCache(pool)
	peers.SetSelf(&tg.User{ID: selfID, Self: true, AccessHash: 1, FirstName: "Self"})
	listener := telegram.NewListener(q, repo, peers, api, logger)
	listener.Start(ctx)
	e := &env{ctx: ctx, pool: pool, q: q, api: api, d: tg.NewUpdateDispatcher(), buf: buf, l: listener}
	listener.Register(&e.d)

	var store worker.MessageStore = repo
	if wrap != nil {
		store = wrap(store, pool)
	}
	reader := telegram.NewReader(api, telegram.NewReadMarks(pool), logger)
	wcfg := worker.Config{MarkRead: true, Lease: time.Minute, PollInterval: 10 * time.Millisecond}
	e.r = reader
	e.w = worker.New(q, store, reader, wcfg, logger)
	e.bw = worker.NewBackfill(q, listener, wcfg, logger)
	e.sw = worker.NewSync(q, telegram.NewDialogSync(listener, reader, true), wcfg, logger)
	return e
}

func (e *env) deliver(t *testing.T, u tg.UpdateClass, users []tg.UserClass, chats []tg.ChatClass) {
	t.Helper()
	require.NoError(t, e.d.Handle(e.ctx, &tg.Updates{
		Updates: []tg.UpdateClass{u}, Users: users, Chats: chats, Date: int(time.Now().Unix()),
	}))
}

// drain runs the worker until no due job is left, then flushes the read
// marks once (a failed flush is logged by the reader and checked by tests).
func (e *env) drain(t *testing.T) {
	t.Helper()
	for range 20 {
		did, err := e.w.RunOnce(e.ctx)
		require.NoError(t, err)
		if !did {
			_ = e.r.Flush(e.ctx)
			return
		}
	}
	t.Fatal("worker did not drain")
}

type jobRow struct {
	Status  string
	Version int
	RunAt   time.Time
}

func (e *env) jobFor(t *testing.T, chatID int64, msgID int) jobRow {
	t.Helper()
	var r jobRow
	require.NoError(t, e.pool.QueryRow(e.ctx,
		`SELECT status, version, run_at FROM jobs WHERE kind = $1 AND dedup_key = $2`,
		jobs.KindSaveMessage, jobs.DedupKey(chatID, msgID)).Scan(&r.Status, &r.Version, &r.RunAt))
	return r
}

func (e *env) count(t *testing.T, sql string, args ...any) int {
	t.Helper()
	var n int
	require.NoError(t, e.pool.QueryRow(e.ctx, sql, args...).Scan(&n))
	return n
}

func forumChannel(title string) *tg.Channel {
	return &tg.Channel{ID: channelRaw, AccessHash: chanHash, Megagroup: true, Forum: true, Title: title}
}

func topicMsg(id int, text string) *tg.Message {
	rh := &tg.MessageReplyHeader{ForumTopic: true}
	rh.SetReplyToMsgID(200) // sets the flag the Get accessor checks
	return &tg.Message{
		ID: id, Date: int(time.Now().Unix()), Message: text,
		PeerID:  &tg.PeerChannel{ChannelID: channelRaw},
		FromID:  &tg.PeerUser{UserID: senderID},
		ReplyTo: rh,
	}
}

func newChannelMsg(m *tg.Message) *tg.UpdateNewChannelMessage {
	return &tg.UpdateNewChannelMessage{Message: m, Pts: 1, PtsCount: 1}
}

func senderUser(name string) []tg.UserClass {
	return []tg.UserClass{&tg.User{ID: senderID, AccessHash: 5, FirstName: name, Username: "u_" + name}}
}

func TestE2E_ForumTopicMessage(t *testing.T) {
	e := newEnv(t, nil)
	e.api.EXPECT().ChannelsReadHistory(gomock.Any(), &tg.ChannelsReadHistoryRequest{
		Channel: &tg.InputChannel{ChannelID: channelRaw, AccessHash: chanHash}, MaxID: 10,
	}).Return(true, nil).Times(1)
	e.api.EXPECT().MessagesReadDiscussion(gomock.Any(), &tg.MessagesReadDiscussionRequest{
		Peer:  &tg.InputPeerChannel{ChannelID: channelRaw, AccessHash: chanHash},
		MsgID: 200, ReadMaxID: 10,
	}).Return(true, nil).Times(1)

	e.deliver(t, newChannelMsg(topicMsg(10, "hello")), senderUser("Ann"), []tg.ChatClass{forumChannel("Forum")})
	e.drain(t)

	var topic *int
	var text string
	require.NoError(t, e.pool.QueryRow(e.ctx, `SELECT topic_id, message FROM messages WHERE chat_id=$1 AND message_id=10`,
		channelMark).Scan(&topic, &text))
	require.NotNil(t, topic)
	require.Equal(t, 200, *topic)
	require.Equal(t, "hello", text)
	require.Equal(t, "done", e.jobFor(t, channelMark, 10).Status)
}

func TestE2E_PrivateOutgoing(t *testing.T) {
	e := newEnv(t, nil)
	e.api.EXPECT().MessagesReadHistory(gomock.Any(), &tg.MessagesReadHistoryRequest{
		Peer:  &tg.InputPeerUser{UserID: peerUserID, AccessHash: userHash},
		MaxID: 7,
	}).Return(&tg.MessagesAffectedMessages{}, nil).Times(1)

	u := &tg.UpdateNewMessage{Message: &tg.Message{
		ID: 7, Out: true, Date: int(time.Now().Unix()), Message: "sent",
		PeerID: &tg.PeerUser{UserID: peerUserID},
	}, Pts: 1, PtsCount: 1}
	e.deliver(t, u, []tg.UserClass{&tg.User{ID: peerUserID, AccessHash: userHash, FirstName: "Bob"}}, nil)
	e.drain(t)

	var out bool
	var chatType string
	var sender int64
	require.NoError(t, e.pool.QueryRow(e.ctx,
		`SELECT out, chat_type, sender_id FROM messages WHERE chat_id=$1 AND message_id=7`, peerUserID).
		Scan(&out, &chatType, &sender))
	require.True(t, out)
	require.Equal(t, message.ChatTypePrivate, chatType)
	require.Equal(t, selfID, sender)
	require.Equal(t, "done", e.jobFor(t, peerUserID, 7).Status)
}

func TestE2E_DuplicateDelivery(t *testing.T) {
	e := newEnv(t, nil)
	e.api.EXPECT().ChannelsReadHistory(gomock.Any(), gomock.Any()).Return(true, nil).Times(1)
	e.api.EXPECT().MessagesReadDiscussion(gomock.Any(), gomock.Any()).Return(true, nil).Times(1)

	upd := func() tg.UpdateClass { return newChannelMsg(topicMsg(10, "dup")) }
	chats := []tg.ChatClass{forumChannel("Forum")}
	e.deliver(t, upd(), senderUser("Ann"), chats)
	e.deliver(t, upd(), senderUser("Ann"), chats)

	require.Equal(t, 1, e.count(t, `SELECT count(*) FROM jobs`))
	require.Equal(t, 2, e.jobFor(t, channelMark, 10).Version)
	e.drain(t)
	require.Equal(t, 1, e.count(t, `SELECT count(*) FROM messages`))
	require.Equal(t, "done", e.jobFor(t, channelMark, 10).Status)

	// Redelivery after processing: re-saved, but already marked read.
	e.deliver(t, upd(), senderUser("Ann"), chats)
	e.drain(t)
	require.Equal(t, 1, e.count(t, `SELECT count(*) FROM messages`))
	require.Equal(t, "done", e.jobFor(t, channelMark, 10).Status)
	require.Equal(t, 2, e.count(t, `SELECT count(*) FROM tg_read_marks`)) // chat + thread
}

func TestE2E_FloodWait(t *testing.T) {
	e := newEnv(t, nil)
	gomock.InOrder(
		e.api.EXPECT().ChannelsReadHistory(gomock.Any(), gomock.Any()).
			Return(false, tgerr.New(420, "FLOOD_WAIT_5")).Times(1),
		e.api.EXPECT().ChannelsReadHistory(gomock.Any(), gomock.Any()).Return(true, nil).Times(1),
	)
	e.api.EXPECT().MessagesReadDiscussion(gomock.Any(), gomock.Any()).Return(true, nil).Times(1)

	e.deliver(t, newChannelMsg(topicMsg(10, "flood")), senderUser("Ann"), []tg.ChatClass{forumChannel("Forum")})
	e.drain(t)

	// The job never waits for Telegram: the mark stays pending in the reader.
	require.Equal(t, 1, e.count(t, `SELECT count(*) FROM messages WHERE message_id=10`))
	require.Equal(t, "done", e.jobFor(t, channelMark, 10).Status)
	require.Equal(t, 0, e.count(t, `SELECT count(*) FROM tg_read_marks`))
	require.Contains(t, e.buf.String(), `"msg":"flood wait"`)

	require.NoError(t, e.r.Flush(e.ctx))
	require.Equal(t, 2, e.count(t, `SELECT count(*) FROM tg_read_marks`)) // chat + thread
}

func TestE2E_EditDuringProcessing(t *testing.T) {
	e := newEnv(t, nil)
	e.api.EXPECT().ChannelsReadHistory(gomock.Any(), gomock.Any()).Return(true, nil).Times(1)
	e.api.EXPECT().MessagesReadDiscussion(gomock.Any(), gomock.Any()).Return(true, nil).Times(1)
	chats := []tg.ChatClass{forumChannel("Forum")}

	e.deliver(t, newChannelMsg(topicMsg(10, "original")), senderUser("Ann"), chats)
	claimed, ok, err := e.q.Claim(e.ctx, jobs.KindSaveMessage, time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1, claimed.Version)

	edited := topicMsg(10, "edited")
	edited.EditDate = int(time.Now().Unix())
	e.deliver(t, &tg.UpdateEditChannelMessage{Message: edited, Pts: 2, PtsCount: 1}, senderUser("Ann"), chats)

	done, err := e.q.Complete(e.ctx, claimed.ID, claimed.Version)
	require.NoError(t, err)
	require.False(t, done)
	require.Equal(t, "pending", e.jobFor(t, channelMark, 10).Status)

	e.drain(t)
	var text string
	require.NoError(t, e.pool.QueryRow(e.ctx, `SELECT message FROM messages WHERE message_id=10`).Scan(&text))
	require.Equal(t, "edited", text)
	j := e.jobFor(t, channelMark, 10)
	require.Equal(t, "done", j.Status)
	require.Equal(t, 2, j.Version)
}

// brokenStore fails one message with a real PgError whose Message carries
// the canary (`invalid input syntax for type integer: "CANARY-7f3a"`), so
// PgError.Error() itself contains it and only logx.SanitizeError keeps it
// out of logs and last_error. (A CHECK violation, used before, puts the
// value only into Detail, which Error() never prints: the test was vacuous.)
type brokenStore struct {
	worker.MessageStore
	pool  *pgxpool.Pool
	badID int
}

func (s brokenStore) Upsert(ctx context.Context, m message.Message) error {
	if m.MessageID == s.badID {
		var n int
		if err := s.pool.QueryRow(ctx, `SELECT $1::text::int`, canary).Scan(&n); err != nil {
			return fmt.Errorf("upsert message chat_id=%d message_id=%d: %w", m.ChatID, m.MessageID, err)
		}
	}
	return s.MessageStore.Upsert(ctx, m)
}

func TestE2E_NoMessageDataInLogs(t *testing.T) {
	const okID, badID, floodID = 10, 11, 12
	e := newEnv(t, func(s worker.MessageStore, pool *pgxpool.Pool) worker.MessageStore {
		return brokenStore{MessageStore: s, pool: pool, badID: badID}
	})

	floodSeen := false
	e.api.EXPECT().ChannelsReadHistory(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *tg.ChannelsReadHistoryRequest) (bool, error) {
			if req.MaxID >= floodID && !floodSeen {
				floodSeen = true
				return false, tgerr.New(420, "FLOOD_WAIT_5")
			}
			return true, nil
		}).AnyTimes()
	e.api.EXPECT().MessagesReadDiscussion(gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()

	chats := []tg.ChatClass{forumChannel("Forum " + canary)}
	users := senderUser(canary)
	mk := func(id int) *tg.Message {
		m := topicMsg(id, "text "+canary)
		m.Entities = []tg.MessageEntityClass{&tg.MessageEntityTextURL{Offset: 0, Length: 4, URL: "https://" + canary + ".example"}}
		return m
	}
	for _, id := range []int{okID, badID, floodID} {
		e.deliver(t, newChannelMsg(mk(id)), users, chats)
	}
	// Topic creation service message carrying a canary title.
	e.deliver(t, &tg.UpdateNewChannelMessage{Message: &tg.MessageService{
		ID: 201, Date: int(time.Now().Unix()), PeerID: &tg.PeerChannel{ChannelID: channelRaw},
		FromID: &tg.PeerUser{UserID: senderID},
		Action: &tg.MessageActionTopicCreate{Title: "topic " + canary},
	}, Pts: 2, PtsCount: 1}, users, chats)
	e.drain(t)

	// Sanity: all three failure/success paths really happened.
	require.Equal(t, "done", e.jobFor(t, channelMark, okID).Status)
	require.Equal(t, "pending", e.jobFor(t, channelMark, badID).Status)
	require.Equal(t, "done", e.jobFor(t, channelMark, floodID).Status) // flood hits the reader, not the job
	require.Equal(t, 1, e.count(t, `SELECT count(*) FROM messages WHERE message_id = $1`, floodID))
	require.Equal(t, 0, e.count(t, `SELECT count(*) FROM messages WHERE message_id = $1`, badID))
	var badErr string
	require.NoError(t, e.pool.QueryRow(e.ctx, `SELECT last_error FROM jobs WHERE dedup_key = $1`,
		jobs.DedupKey(channelMark, badID)).Scan(&badErr))
	require.NotEmpty(t, badErr)
	require.Contains(t, badErr, "22P02", "the failure is the canary-bearing PgError")

	// gotd bridge: canary attrs (string "message", any "entities") must be dropped.
	gl := logx.NewGotdLogger(logx.New(e.buf, slog.LevelDebug, "json"), slog.LevelDebug)
	gl.Log(e.ctx, log.LevelWarn, "gotd event",
		log.String("message", canary),
		log.Any("entities", []string{canary}),
		log.Int64("channel_id", channelRaw))

	logs := e.buf.String()
	require.NotEmpty(t, logs)
	require.Contains(t, logs, "job failed, will retry")
	require.Contains(t, logs, "flood wait")
	require.NotContains(t, logs, canary)

	rows, err := e.pool.Query(e.ctx, `SELECT coalesce(last_error, '') FROM jobs`)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var le string
		require.NoError(t, rows.Scan(&le))
		require.NotContains(t, le, canary)
	}
	require.NoError(t, rows.Err())
}

// backfillJob reads the backfill_history job of channel 5.
func (e *env) backfillJob(t *testing.T) (status string, payload jobs.BackfillHistory, runAt time.Time) {
	t.Helper()
	var raw []byte
	require.NoError(t, e.pool.QueryRow(e.ctx,
		`SELECT status, payload, run_at FROM jobs WHERE kind = $1 AND dedup_key = $2`,
		jobs.KindBackfillHistory, jobs.BackfillDedupKey(channelMark)).Scan(&status, &raw, &runAt))
	require.NoError(t, json.Unmarshal(raw, &payload))
	return status, payload, runAt
}

func channelHistory(ids ...int) *tg.MessagesChannelMessages {
	r := &tg.MessagesChannelMessages{Chats: []tg.ChatClass{forumChannel("Forum")}, Users: senderUser("Ann")}
	for _, id := range ids {
		r.Messages = append(r.Messages, topicMsg(id, "backfilled"))
	}
	return r
}

// ChannelTooLong → durable job → FLOOD_WAIT longer than the in-job cap (30m)
// → queue retry with the same min_id even though live messages moved the last
// stored id past the gap meanwhile.
func TestE2E_BackfillFloodWaitThenSuccess(t *testing.T) {
	e := newEnv(t, nil)
	e.api.EXPECT().ChannelsReadHistory(gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()
	e.api.EXPECT().MessagesReadDiscussion(gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()
	chats := []tg.ChatClass{forumChannel("Forum")}

	e.deliver(t, newChannelMsg(topicMsg(10, "stored")), senderUser("Ann"), chats)
	e.drain(t)
	e.l.OnChannelTooLong(channelRaw)
	status, p, _ := e.backfillJob(t)
	require.Equal(t, "pending", status)
	require.Equal(t, jobs.BackfillHistory{ChatID: channelMark, MinID: 10}, p)

	var minIDs []int
	gomock.InOrder(
		e.api.EXPECT().MessagesGetHistory(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, r *tg.MessagesGetHistoryRequest) (tg.MessagesMessagesClass, error) {
				minIDs = append(minIDs, r.MinID)
				return nil, tgerr.New(420, "FLOOD_WAIT_3600")
			}),
		e.api.EXPECT().MessagesGetHistory(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, r *tg.MessagesGetHistoryRequest) (tg.MessagesMessagesClass, error) {
				minIDs = append(minIDs, r.MinID)
				return channelHistory(30, 12, 11), nil
			}),
		e.api.EXPECT().MessagesGetHistory(gomock.Any(), gomock.Any()).Return(channelHistory(), nil),
	)
	before := time.Now()
	did, err := e.bw.RunOnce(e.ctx)
	require.NoError(t, err)
	require.True(t, did)
	status, _, runAt := e.backfillJob(t)
	require.Equal(t, "pending", status)
	require.WithinRange(t, runAt, before.Add(time.Hour), time.Now().Add(time.Hour+2*time.Second))
	require.Equal(t, 1, e.count(t, `SELECT count(*) FROM jobs WHERE kind = 'backfill_history' AND attempts = 0`),
		"a FLOOD_WAIT does not count the attempt")

	// A live message arrives and another TooLong fires while the job waits.
	e.deliver(t, newChannelMsg(topicMsg(30, "live")), senderUser("Ann"), chats)
	e.drain(t)
	e.l.OnChannelTooLong(channelRaw)
	_, p, runAt2 := e.backfillJob(t)
	require.Equal(t, 10, p.MinID, "min_id must not move past the gap")
	require.True(t, runAt2.Equal(runAt), "re-enqueue keeps the flood wait")

	_, err = e.pool.Exec(e.ctx, `UPDATE jobs SET run_at = now() WHERE kind = $1`, jobs.KindBackfillHistory)
	require.NoError(t, err)
	did, err = e.bw.RunOnce(e.ctx)
	require.NoError(t, err)
	require.True(t, did)
	status, _, _ = e.backfillJob(t)
	require.Equal(t, "done", status)
	require.Equal(t, []int{10, 10}, minIDs)

	e.drain(t)
	require.Equal(t, 4, e.count(t, `SELECT count(*) FROM messages WHERE message_id IN (10, 11, 12, 30)`))
	require.Contains(t, e.buf.String(), `"msg":"backfill done"`)
}

// A backfill interrupted by a crash (lease never released) is reaped and
// processed later with the original min_id.
func TestE2E_BackfillSurvivesRestart(t *testing.T) {
	e := newEnv(t, nil)
	e.api.EXPECT().ChannelsReadHistory(gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()
	e.api.EXPECT().MessagesReadDiscussion(gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()
	e.deliver(t, newChannelMsg(topicMsg(10, "stored")), senderUser("Ann"), []tg.ChatClass{forumChannel("Forum")})
	e.drain(t)
	e.l.OnChannelTooLong(channelRaw)

	// The process claims the job and dies.
	_, ok, err := e.q.Claim(e.ctx, jobs.KindBackfillHistory, time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	status, _, _ := e.backfillJob(t)
	require.Equal(t, "running", status)

	// After the restart the lease expires and maintenance reaps it.
	_, err = e.pool.Exec(e.ctx, `UPDATE jobs SET locked_until = now() - interval '1 second' WHERE kind = $1`,
		jobs.KindBackfillHistory)
	require.NoError(t, err)
	n, err := e.q.Reap(e.ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, n)

	e.api.EXPECT().MessagesGetHistory(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, r *tg.MessagesGetHistoryRequest) (tg.MessagesMessagesClass, error) {
			require.Equal(t, 10, r.MinID)
			if r.OffsetID == 0 {
				return channelHistory(11), nil
			}
			return channelHistory(), nil
		}).Times(2)
	did, err := e.bw.RunOnce(e.ctx)
	require.NoError(t, err)
	require.True(t, did)
	status, _, _ = e.backfillJob(t)
	require.Equal(t, "done", status)
	e.drain(t)
	require.Equal(t, 1, e.count(t, `SELECT count(*) FROM messages WHERE message_id = 11`))
}

// Shutdown in the middle of a backfill releases the job: pending again, due
// now, and the attempt is not counted.
func TestE2E_BackfillShutdownReleasesJob(t *testing.T) {
	e := newEnv(t, nil)
	e.api.EXPECT().ChannelsReadHistory(gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()
	e.api.EXPECT().MessagesReadDiscussion(gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()
	e.deliver(t, newChannelMsg(topicMsg(10, "stored")), senderUser("Ann"), []tg.ChatClass{forumChannel("Forum")})
	e.drain(t)
	e.l.OnChannelTooLong(channelRaw)

	ctx, cancel := context.WithCancel(e.ctx)
	e.api.EXPECT().MessagesGetHistory(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *tg.MessagesGetHistoryRequest) (tg.MessagesMessagesClass, error) {
			cancel()
			return channelHistory(12, 11), nil
		})
	did, err := e.bw.RunOnce(ctx)
	require.NoError(t, err)
	require.True(t, did)
	status, p, _ := e.backfillJob(t)
	require.Equal(t, "pending", status)
	require.Equal(t, 10, p.MinID)
	require.Equal(t, 1, e.count(t, `SELECT count(*) FROM jobs WHERE kind = 'backfill_history'
		AND attempts = 0 AND locked_until IS NULL AND run_at <= now()`))
	require.Contains(t, e.buf.String(), `"msg":"job released"`)
	require.Equal(t, 0, e.count(t, `SELECT count(*) FROM jobs WHERE kind = 'save_message' AND status = 'pending'`))
}

func dialogsOf(ds ...*tg.Dialog) *tg.MessagesDialogs {
	r := &tg.MessagesDialogs{
		Chats: []tg.ChatClass{forumChannel("Forum")},
		Users: []tg.UserClass{&tg.User{ID: peerUserID, AccessHash: userHash, FirstName: "Bob"}},
	}
	for _, d := range ds {
		r.Dialogs = append(r.Dialogs, d)
	}
	return r
}

func (e *env) expectDialogs(main *tg.MessagesDialogs) {
	e.api.EXPECT().MessagesGetDialogs(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, r *tg.MessagesGetDialogsRequest) (tg.MessagesDialogsClass, error) {
			if r.FolderID == 1 {
				return &tg.MessagesDialogs{}, nil
			}
			return main, nil
		}).Times(2)
}

// The dialog sync job goes through the PG queue: a forum behind the stored
// history and a private chat with no history but unread messages get
// backfill_history jobs; their messages are saved and marked read (chat and
// topic). A second pass over the synced state enqueues and marks nothing.
func TestE2E_DialogSync(t *testing.T) {
	e := newEnv(t, nil)
	chats := []tg.ChatClass{forumChannel("Forum")}
	e.api.EXPECT().ChannelsReadHistory(gomock.Any(), gomock.Any()).Return(true, nil).Times(1) // live message 10
	e.api.EXPECT().MessagesReadDiscussion(gomock.Any(), gomock.Any()).Return(true, nil).Times(1)
	e.deliver(t, newChannelMsg(topicMsg(10, "stored")), senderUser("Ann"), chats)
	e.drain(t)

	// Downtime: the forum got 11 and 12 in topic 200, Bob wrote 7 (6 was read).
	e.expectDialogs(dialogsOf(
		&tg.Dialog{Peer: &tg.PeerChannel{ChannelID: channelRaw}, TopMessage: 12, ReadInboxMaxID: 10, UnreadCount: 2},
		&tg.Dialog{Peer: &tg.PeerUser{UserID: peerUserID}, TopMessage: 7, ReadInboxMaxID: 6, UnreadCount: 1},
	))
	e.api.EXPECT().MessagesGetForumTopics(gomock.Any(), gomock.Any()).Return(&tg.MessagesForumTopics{
		Topics: []tg.ForumTopicClass{&tg.ForumTopic{ID: 200, TopMessage: 12, ReadInboxMaxID: 10, UnreadCount: 2}},
	}, nil)
	_, err := e.q.EnqueueIdle(e.ctx, jobs.KindSyncDialogs, jobs.SyncDialogsDedupKey, struct{}{})
	require.NoError(t, err)
	did, err := e.sw.RunOnce(e.ctx)
	require.NoError(t, err)
	require.True(t, did)
	require.Equal(t, 1, e.count(t, `SELECT count(*) FROM jobs WHERE kind = 'sync_dialogs' AND status = 'done'`))
	_, p, _ := e.backfillJob(t)
	require.Equal(t, jobs.BackfillHistory{ChatID: channelMark, MinID: 10}, p)
	require.Equal(t, 1, e.count(t, `SELECT count(*) FROM jobs WHERE kind = 'backfill_history' AND dedup_key = $1
		AND payload = '{"chat_id": 2000, "min_id": 6, "max_pages": 1}'::jsonb`, jobs.BackfillDedupKey(peerUserID)))

	e.api.EXPECT().MessagesGetHistory(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, r *tg.MessagesGetHistoryRequest) (tg.MessagesMessagesClass, error) {
			if _, ok := r.Peer.(*tg.InputPeerUser); ok {
				require.Equal(t, &tg.InputPeerUser{UserID: peerUserID, AccessHash: userHash}, r.Peer)
				require.Equal(t, 6, r.MinID)
				return &tg.MessagesMessages{Messages: []tg.MessageClass{&tg.Message{
					ID: 7, Date: int(time.Now().Unix()), Message: "hi", PeerID: &tg.PeerUser{UserID: peerUserID},
				}}}, nil
			}
			require.Equal(t, 10, r.MinID)
			if r.OffsetID == 0 {
				return channelHistory(12, 11), nil
			}
			return channelHistory(), nil
		}).Times(3)
	for range 2 {
		did, err := e.bw.RunOnce(e.ctx)
		require.NoError(t, err)
		require.True(t, did)
	}
	e.api.EXPECT().ChannelsReadHistory(gomock.Any(), &tg.ChannelsReadHistoryRequest{
		Channel: &tg.InputChannel{ChannelID: channelRaw, AccessHash: chanHash}, MaxID: 12,
	}).Return(true, nil).Times(1)
	e.api.EXPECT().MessagesReadDiscussion(gomock.Any(), &tg.MessagesReadDiscussionRequest{
		Peer: &tg.InputPeerChannel{ChannelID: channelRaw, AccessHash: chanHash}, MsgID: 200, ReadMaxID: 12,
	}).Return(true, nil).Times(1)
	e.api.EXPECT().MessagesReadHistory(gomock.Any(), &tg.MessagesReadHistoryRequest{
		Peer: &tg.InputPeerUser{UserID: peerUserID, AccessHash: userHash}, MaxID: 7,
	}).Return(&tg.MessagesAffectedMessages{}, nil).Times(1)
	e.drain(t)
	require.Equal(t, 3, e.count(t, `SELECT count(*) FROM messages WHERE (chat_id = $1 AND message_id IN (10, 11, 12) AND topic_id = 200)`, channelMark))
	require.Equal(t, 1, e.count(t, `SELECT count(*) FROM messages WHERE chat_id = $1 AND message_id = 7`, peerUserID))

	// Synced: the next pass costs the getDialogs pages only.
	e.expectDialogs(dialogsOf(
		&tg.Dialog{Peer: &tg.PeerChannel{ChannelID: channelRaw}, TopMessage: 12, ReadInboxMaxID: 12},
		&tg.Dialog{Peer: &tg.PeerUser{UserID: peerUserID}, TopMessage: 7, ReadInboxMaxID: 7},
	))
	_, err = e.q.EnqueueIdle(e.ctx, jobs.KindSyncDialogs, jobs.SyncDialogsDedupKey, struct{}{})
	require.NoError(t, err)
	did, err = e.sw.RunOnce(e.ctx)
	require.NoError(t, err)
	require.True(t, did)
	require.Equal(t, 0, e.count(t, `SELECT count(*) FROM jobs WHERE status = 'pending'`))
	require.Contains(t, e.buf.String(), `"chats_behind":0`)
}
