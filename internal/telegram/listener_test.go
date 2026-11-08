package telegram_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/a1ekseev/chat-analyze/internal/jobs"
	"github.com/a1ekseev/chat-analyze/internal/queue"
	"github.com/a1ekseev/chat-analyze/internal/telegram"
	"github.com/a1ekseev/chat-analyze/internal/telegram/mocks"
)

type enq struct {
	mu      sync.Mutex
	keys    []string
	pl      []jobs.SaveMessage
	err     error
	panicOn bool
	order   []int

	bfKeys  []string
	bf      []jobs.BackfillChannel
	bfField string
}

func (e *enq) EnqueueKeepMin(ctx context.Context, kind, key string, payload any, field string) (queue.EnqueueResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return queue.EnqueueResult{}, err
	}
	if e.panicOn {
		panic("sensitive-panic-value")
	}
	if kind != jobs.KindBackfillChannel {
		return queue.EnqueueResult{}, errors.New("bad kind")
	}
	e.bfKeys = append(e.bfKeys, key)
	e.bf = append(e.bf, payload.(jobs.BackfillChannel))
	e.bfField = field
	return queue.EnqueueResult{ID: 7, Version: 1, Inserted: true}, e.err
}

func (e *enq) Enqueue(_ context.Context, kind, key string, payload any) (queue.EnqueueResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.panicOn {
		panic("sensitive-panic-value")
	}
	if kind != jobs.KindSaveMessage {
		return queue.EnqueueResult{}, errors.New("bad kind")
	}
	e.keys = append(e.keys, key)
	sm := payload.(jobs.SaveMessage)
	e.pl = append(e.pl, sm)
	e.order = append(e.order, sm.Message.MessageID)
	return queue.EnqueueResult{Inserted: true}, e.err
}

type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}
func (l *lockedBuf) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }
func (l *lockedBuf) Reset()         { l.mu.Lock(); defer l.mu.Unlock(); l.b.Reset() }

type viewCall struct {
	chat      int64
	msg, view int
}

type repo struct {
	views   []viewCall
	last    int
	ok      bool
	lastErr error
}

func (r *repo) UpdateViews(_ context.Context, chatID int64, id, views int) error {
	r.views = append(r.views, viewCall{chatID, id, views})
	return nil
}

func (r *repo) LastMessageID(context.Context, int64) (int, bool, error) {
	return r.last, r.ok, r.lastErr
}

type fakePeers struct {
	saved     map[int64]telegram.PeerInfo
	hash      map[int64]telegram.PeerInfo
	panicSelf bool
}

func (p *fakePeers) Save(_ context.Context, _ int64, ps map[int64]telegram.PeerInfo) error {
	if p.saved == nil {
		p.saved = map[int64]telegram.PeerInfo{}
	}
	maps.Copy(p.saved, ps)
	return nil
}

func (p *fakePeers) Get(_ context.Context, _, chatID int64) (telegram.PeerInfo, bool, error) {
	v, ok := p.hash[chatID]
	return v, ok, nil
}

func (p *fakePeers) Self() telegram.PeerInfo {
	if p.panicSelf {
		panic("sensitive-panic-value")
	}
	return p.self()
}

func (p *fakePeers) self() telegram.PeerInfo { return telegram.PeerInfo{ID: 1, Kind: jobs.PeerUser} }

type env struct {
	l   *telegram.Listener
	d   tg.UpdateDispatcher
	q   *enq
	r   *repo
	p   *fakePeers
	buf *lockedBuf
}

func setup(t *testing.T, api telegram.API) env {
	t.Helper()
	q, r, p := &enq{}, &repo{}, &fakePeers{}
	buf := &lockedBuf{}
	l := telegram.NewListener(q, r, p, api, slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	telegram.SetBackfillPagePause(l, 0) // TestListener_BackfillPagePause covers it
	d := tg.NewUpdateDispatcher()
	l.Register(&d)
	return env{l, d, q, r, p, buf}
}

func channelMsg(id int) *tg.Message {
	return &tg.Message{ID: id, PeerID: &tg.PeerChannel{ChannelID: 5}, Date: 100, Message: "secret text"}
}

var channel = &tg.Channel{ID: 5, AccessHash: 77, Title: "T", Broadcast: true}

func TestListener_NewChannelMessageEnqueued(t *testing.T) {
	e := setup(t, nil)
	err := e.d.Handle(t.Context(), &tg.Updates{
		Updates: []tg.UpdateClass{&tg.UpdateNewChannelMessage{Message: channelMsg(9)}},
		Chats:   []tg.ChatClass{channel},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"-1000000000005:9"}, e.q.keys)
	assert.Contains(t, e.p.saved, int64(-1000000000005))
}

func TestListener_EditEnqueuedWithSameKey(t *testing.T) {
	e := setup(t, nil)
	upd := &tg.Updates{Chats: []tg.ChatClass{channel}, Updates: []tg.UpdateClass{
		&tg.UpdateNewChannelMessage{Message: channelMsg(9)},
		&tg.UpdateEditChannelMessage{Message: channelMsg(9)},
	}}
	require.NoError(t, e.d.Handle(t.Context(), upd))
	assert.Equal(t, []string{"-1000000000005:9", "-1000000000005:9"}, e.q.keys)
}

func TestListener_SkipsEmptyAndNoPeer(t *testing.T) {
	e := setup(t, nil)
	upd := &tg.Updates{Updates: []tg.UpdateClass{
		&tg.UpdateNewMessage{Message: &tg.MessageEmpty{ID: 1}},
		&tg.UpdateNewMessage{Message: &tg.Message{ID: 2}},
	}}
	require.NoError(t, e.d.Handle(t.Context(), upd))
	assert.Empty(t, e.q.keys)
	assert.Contains(t, e.buf.String(), "update skipped")
}

func TestListener_ViewsUpdated(t *testing.T) {
	e := setup(t, nil)
	upd := &tg.Updates{Updates: []tg.UpdateClass{&tg.UpdateChannelMessageViews{ChannelID: 5, ID: 9, Views: 42}}}
	require.NoError(t, e.d.Handle(t.Context(), upd))
	assert.Equal(t, []viewCall{{-1000000000005, 9, 42}}, e.r.views)
}

func TestListener_EnqueueErrorDoesNotFail(t *testing.T) {
	e := setup(t, nil)
	e.q.err = errors.New("boom")
	upd := &tg.Updates{Chats: []tg.ChatClass{channel}, Updates: []tg.UpdateClass{&tg.UpdateNewChannelMessage{Message: channelMsg(9)}}}
	require.NoError(t, e.d.Handle(t.Context(), upd))
	assert.Contains(t, e.buf.String(), "enqueue failed")
	assert.NotContains(t, e.buf.String(), "secret text")
}

func TestListener_FallbackLogs(t *testing.T) {
	e := setup(t, nil)
	require.NoError(t, e.d.Handle(t.Context(), &tg.Updates{Updates: []tg.UpdateClass{&tg.UpdateUserStatus{}}}))
	assert.Contains(t, e.buf.String(), "unsupported")
}

func TestListener_PanicRecovered(t *testing.T) {
	e := setup(t, nil)
	e.q.panicOn = true
	upd := &tg.Updates{Chats: []tg.ChatClass{channel}, Updates: []tg.UpdateClass{&tg.UpdateNewChannelMessage{Message: channelMsg(9)}}}
	require.NoError(t, e.d.Handle(t.Context(), upd))
	assert.Contains(t, e.buf.String(), `msg="panic recovered" component=telegram panic=string`)
	assert.NotContains(t, e.buf.String(), "sensitive-panic-value")
}

func TestListener_LogCatalogue(t *testing.T) {
	e := setup(t, nil)
	upd := &tg.Updates{Chats: []tg.ChatClass{channel}, Updates: []tg.UpdateClass{
		&tg.UpdateNewChannelMessage{Message: channelMsg(9)},
		&tg.UpdateEditChannelMessage{Message: &tg.MessageEmpty{ID: 3}},
	}}
	require.NoError(t, e.d.Handle(t.Context(), upd))
	out := e.buf.String()
	assert.Contains(t, out, `msg="update received" component=telegram update_type=updateNewChannelMessage `+
		`chat_id=-1000000000005 message_id=9 chat_type=channel is_edit=false is_service=false`)
	assert.Contains(t, out, `msg="job enqueued" component=queue job_id=0 job_kind=save_message version=0 inserted=true`)
	assert.Contains(t, out, `msg="update skipped" component=telegram update_type=updateEditChannelMessage reason=empty is_edit=true`)
	assert.NotContains(t, out, "telegram_listener")
	assert.NotContains(t, out, " edit=")
}

func withHash(e env) {
	e.r.last, e.r.ok = 10, true
	e.p.hash = map[int64]telegram.PeerInfo{-1000000000005: {ChatID: -1000000000005, Kind: jobs.PeerChannel, ID: 5, AccessHash: 77}}
}

func page(ids ...int) *tg.MessagesChannelMessages {
	r := &tg.MessagesChannelMessages{Chats: []tg.ChatClass{channel}}
	for _, id := range ids {
		r.Messages = append(r.Messages, channelMsg(id))
	}
	return r
}

func seq(from, to int) []int { // descending
	var out []int
	for i := to; i >= from; i-- {
		out = append(out, i)
	}
	return out
}

func TestListener_BackfillPagesAscending(t *testing.T) {
	api := mocks.NewMockAPI(gomock.NewController(t))
	e := setup(t, api)
	withHash(e)
	var offsets []int
	calls := 0
	api.EXPECT().MessagesGetHistory(gomock.Any(), gomock.Any()).Times(3).DoAndReturn(
		func(_ context.Context, req *tg.MessagesGetHistoryRequest) (tg.MessagesMessagesClass, error) {
			offsets = append(offsets, req.OffsetID)
			assert.Equal(t, 10, req.MinID)
			assert.Equal(t, 100, req.Limit)
			assert.Empty(t, e.q.keys, "nothing may be enqueued while fetching")
			calls++
			switch calls {
			case 1:
				return page(seq(21, 120)...), nil // 100 messages, newest first
			case 2:
				return page(seq(11, 20)...), nil
			}
			return page(), nil
		})
	require.NoError(t, e.l.Backfill(t.Context(), 5, 10))
	assert.Equal(t, []int{0, 21, 11}, offsets)
	require.Len(t, e.q.order, 110)
	assert.True(t, slices.IsSorted(e.q.order))
	assert.Equal(t, 11, e.q.order[0])
	out := e.buf.String()
	assert.Contains(t, out, `msg="backfill done"`)
	assert.Contains(t, out, "component=telegram chat_id=-1000000000005 from_message_id=10 fetched=110 duration_ms=")
}

func TestListener_BackfillFetchErrorEnqueuesNothing(t *testing.T) {
	api := mocks.NewMockAPI(gomock.NewController(t))
	e := setup(t, api)
	withHash(e)
	gomock.InOrder(
		api.EXPECT().MessagesGetHistory(gomock.Any(), gomock.Any()).Return(page(seq(21, 120)...), nil),
		api.EXPECT().MessagesGetHistory(gomock.Any(), gomock.Any()).Return(nil, errors.New("rpc down")),
	)
	require.Error(t, e.l.Backfill(t.Context(), 5, 10))
	assert.Empty(t, e.q.keys)
}

func TestListener_BackfillTruncated(t *testing.T) {
	api := mocks.NewMockAPI(gomock.NewController(t))
	e := setup(t, api)
	withHash(e)
	next := 1_000_000
	api.EXPECT().MessagesGetHistory(gomock.Any(), gomock.Any()).Times(1000).DoAndReturn(
		func(context.Context, *tg.MessagesGetHistoryRequest) (tg.MessagesMessagesClass, error) {
			next--
			return page(next), nil
		})
	require.NoError(t, e.l.Backfill(t.Context(), 5, 10))
	assert.Len(t, e.q.keys, 1000)
	assert.Contains(t, e.buf.String(), "backfill truncated")
	assert.NotContains(t, e.buf.String(), "backfill done")
}

func TestListener_BackfillContextCancelled(t *testing.T) {
	api := mocks.NewMockAPI(gomock.NewController(t))
	e := setup(t, api)
	withHash(e)
	ctx, cancel := context.WithCancel(t.Context())
	api.EXPECT().MessagesGetHistory(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
		func(context.Context, *tg.MessagesGetHistoryRequest) (tg.MessagesMessagesClass, error) {
			cancel()
			return page(seq(21, 120)...), nil
		})
	require.ErrorIs(t, e.l.Backfill(ctx, 5, 10), context.Canceled)
	assert.Empty(t, e.q.keys)
}

func TestListener_BackfillNoHash(t *testing.T) {
	e := setup(t, mocks.NewMockAPI(gomock.NewController(t)))
	e.r.last, e.r.ok = 10, true
	require.NoError(t, e.l.Backfill(t.Context(), 5, 10))
	assert.Empty(t, e.q.keys)
	assert.Contains(t, e.buf.String(), "level=WARN")
}

func TestListener_BackfillNoHistoryFetchesOnePage(t *testing.T) {
	api := mocks.NewMockAPI(gomock.NewController(t))
	e := setup(t, api)
	withHash(e)
	api.EXPECT().MessagesGetHistory(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
		func(_ context.Context, req *tg.MessagesGetHistoryRequest) (tg.MessagesMessagesClass, error) {
			assert.Equal(t, 0, req.MinID)
			assert.Equal(t, 0, req.OffsetID)
			return page(seq(1001, 1100)...), nil // a full page: more history exists
		})
	require.NoError(t, e.l.Backfill(t.Context(), 5, 0))
	require.Len(t, e.q.order, 100)
	assert.True(t, slices.IsSorted(e.q.order))
	out := e.buf.String()
	assert.Contains(t, out, `level=WARN msg="backfill limited" component=telegram reason=no_history chat_id=-1000000000005`)
	assert.Contains(t, out, "from_message_id=0 fetched=100")
	assert.NotContains(t, out, "backfill truncated")
}

func TestListener_BackfillEnqueueErrorFails(t *testing.T) {
	api := mocks.NewMockAPI(gomock.NewController(t))
	e := setup(t, api)
	withHash(e)
	e.q.err = errors.New("db down")
	api.EXPECT().MessagesGetHistory(gomock.Any(), gomock.Any()).Return(page(12, 11), nil)
	api.EXPECT().MessagesGetHistory(gomock.Any(), gomock.Any()).Return(page(), nil)
	require.Error(t, e.l.Backfill(t.Context(), 5, 10))
	assert.Len(t, e.q.keys, 1, "stops at the first failed enqueue")
	assert.NotContains(t, e.buf.String(), "backfill done")
}

// A FLOOD_WAIT longer than the in-job cap goes back to the worker, which
// schedules the retry through the queue.
func TestListener_BackfillFloodWaitIsReturned(t *testing.T) {
	api := mocks.NewMockAPI(gomock.NewController(t))
	e := setup(t, api)
	withHash(e)
	sleeps := fakeSleep(e.l)
	api.EXPECT().MessagesGetHistory(gomock.Any(), gomock.Any()).Return(nil, tgerr.New(420, "FLOOD_WAIT_3600"))
	err := e.l.Backfill(t.Context(), 5, 10)
	d, ok := tgerr.AsFloodWait(err)
	require.True(t, ok, "the worker needs the flood wait to schedule the retry")
	assert.Equal(t, time.Hour, d)
	assert.Empty(t, *sleeps)
	assert.Empty(t, e.q.keys)
}

func fakeSleep(l *telegram.Listener) *[]time.Duration {
	var got []time.Duration
	telegram.SetBackfillSleep(l, func(ctx context.Context, d time.Duration) error {
		got = append(got, d)
		return ctx.Err()
	})
	return &got
}

// Pages are spaced by the pause; the first page is fetched at once.
func TestListener_BackfillPagePause(t *testing.T) {
	api := mocks.NewMockAPI(gomock.NewController(t))
	e := setup(t, api)
	withHash(e)
	telegram.SetBackfillPagePause(e.l, time.Second)
	sleeps := fakeSleep(e.l)
	gomock.InOrder(
		api.EXPECT().MessagesGetHistory(gomock.Any(), gomock.Any()).Return(page(seq(21, 120)...), nil),
		api.EXPECT().MessagesGetHistory(gomock.Any(), gomock.Any()).Return(page(seq(11, 20)...), nil),
		api.EXPECT().MessagesGetHistory(gomock.Any(), gomock.Any()).Return(page(), nil),
	)
	require.NoError(t, e.l.Backfill(t.Context(), 5, 10))
	assert.Equal(t, []time.Duration{time.Second, time.Second}, *sleeps)
	require.Len(t, e.q.order, 110)
}

// FLOOD_WAIT on page 2: wait in the job and retry that page only.
func TestListener_BackfillFloodWaitRetriesSamePage(t *testing.T) {
	api := mocks.NewMockAPI(gomock.NewController(t))
	e := setup(t, api)
	withHash(e)
	sleeps := fakeSleep(e.l)
	var offsets []int
	calls := 0
	api.EXPECT().MessagesGetHistory(gomock.Any(), gomock.Any()).Times(4).DoAndReturn(
		func(_ context.Context, req *tg.MessagesGetHistoryRequest) (tg.MessagesMessagesClass, error) {
			offsets = append(offsets, req.OffsetID)
			calls++
			switch calls {
			case 1:
				return page(seq(21, 120)...), nil
			case 2:
				return nil, tgerr.New(420, "FLOOD_WAIT_3")
			case 3:
				return page(seq(11, 20)...), nil
			}
			return page(), nil
		})
	require.NoError(t, e.l.Backfill(t.Context(), 5, 10))
	assert.Equal(t, []int{0, 21, 21, 11}, offsets)
	assert.Equal(t, []time.Duration{4 * time.Second}, *sleeps)
	require.Len(t, e.q.order, 110)
	assert.True(t, slices.IsSorted(e.q.order))
	out := e.buf.String()
	assert.Contains(t, out, `level=WARN msg="flood wait" component=telegram chat_id=-1000000000005 wait_s=3`)
	assert.Contains(t, out, `msg="backfill done"`)
}

// In-job waiting is capped at 30 minutes in total; then the error goes back
// to the queue.
func TestListener_BackfillFloodWaitCumulativeCap(t *testing.T) {
	api := mocks.NewMockAPI(gomock.NewController(t))
	e := setup(t, api)
	withHash(e)
	sleeps := fakeSleep(e.l)
	api.EXPECT().MessagesGetHistory(gomock.Any(), gomock.Any()).Times(2).Return(nil, tgerr.New(420, "FLOOD_WAIT_1000"))
	err := e.l.Backfill(t.Context(), 5, 10)
	d, ok := tgerr.AsFloodWait(err)
	require.True(t, ok)
	assert.Equal(t, 1000*time.Second, d)
	assert.Equal(t, []time.Duration{1001 * time.Second}, *sleeps, "a second wait would pass the 30m cap")
	assert.Empty(t, e.q.keys)
}

// A wait that would outlive the job's lease (ctx deadline) goes back to the
// queue instead.
func TestListener_BackfillFloodWaitRespectsDeadline(t *testing.T) {
	api := mocks.NewMockAPI(gomock.NewController(t))
	e := setup(t, api)
	withHash(e)
	sleeps := fakeSleep(e.l)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	api.EXPECT().MessagesGetHistory(gomock.Any(), gomock.Any()).Times(1).Return(nil, tgerr.New(420, "FLOOD_WAIT_120"))
	_, ok := tgerr.AsFloodWait(e.l.Backfill(ctx, 5, 10))
	require.True(t, ok)
	assert.Empty(t, *sleeps)
}

// Shutdown during a flood wait stops the backfill at once.
func TestListener_BackfillFloodWaitInterruptedByShutdown(t *testing.T) {
	api := mocks.NewMockAPI(gomock.NewController(t))
	e := setup(t, api)
	withHash(e)
	ctx, cancel := context.WithCancel(t.Context())
	api.EXPECT().MessagesGetHistory(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
		func(context.Context, *tg.MessagesGetHistoryRequest) (tg.MessagesMessagesClass, error) {
			cancel()
			return nil, tgerr.New(420, "FLOOD_WAIT_600")
		})
	start := time.Now()
	require.ErrorIs(t, e.l.Backfill(ctx, 5, 10), context.Canceled)
	assert.Less(t, time.Since(start), 5*time.Second)
	assert.Empty(t, e.q.keys)
}

func TestListener_OnChannelTooLongEnqueuesBackfillJob(t *testing.T) {
	// No API expectations: nothing is fetched in the gotd callback.
	e := setup(t, mocks.NewMockAPI(gomock.NewController(t)))
	withHash(e)
	e.l.Start(t.Context())
	e.l.OnChannelTooLong(5)
	assert.Equal(t, []string{"5"}, e.q.bfKeys)
	assert.Equal(t, []jobs.BackfillChannel{{ChannelID: 5, MinID: 10}}, e.q.bf)
	assert.Equal(t, jobs.BackfillMinIDField, e.q.bfField)
	assert.Empty(t, e.q.keys)
	out := e.buf.String()
	assert.Contains(t, out, `level=WARN msg="channel too long" component=telegram chat_id=-1000000000005`)
	assert.Contains(t, out, `msg="job enqueued" component=queue job_id=7 job_kind=backfill_channel version=1 inserted=true`)
}

func TestListener_OnChannelTooLongDuringShutdown(t *testing.T) {
	e := setup(t, nil)
	withHash(e)
	ctx, cancel := context.WithCancel(t.Context())
	e.l.Start(ctx)
	cancel() // workers are stopping, Telegram is still running
	e.l.OnChannelTooLong(5)
	assert.Equal(t, []jobs.BackfillChannel{{ChannelID: 5, MinID: 10}}, e.q.bf)
}

func TestListener_OnChannelTooLongNoHistory(t *testing.T) {
	e := setup(t, nil)
	e.l.Start(t.Context())
	e.l.OnChannelTooLong(5)
	assert.Equal(t, []jobs.BackfillChannel{{ChannelID: 5, MinID: 0}}, e.q.bf)
}

func TestListener_OnChannelTooLongErrors(t *testing.T) {
	e := setup(t, nil)
	e.r.lastErr = errors.New("db down")
	e.l.OnChannelTooLong(5) // before Start: falls back to a background ctx
	assert.Empty(t, e.q.bf)
	assert.Contains(t, e.buf.String(), `msg="backfill enqueue failed"`)

	e = setup(t, nil)
	e.q.err = errors.New("db down")
	e.l.OnChannelTooLong(5)
	assert.Contains(t, e.buf.String(), `msg="backfill enqueue failed"`)
}

func TestListener_OnChannelTooLongPanicRecovered(t *testing.T) {
	e := setup(t, nil)
	e.q.panicOn = true
	e.l.Start(t.Context())
	e.l.OnChannelTooLong(5)
	assert.Contains(t, e.buf.String(), "panic recovered")
	assert.NotContains(t, e.buf.String(), "sensitive-panic-value")
}

func TestListener_MinChannelUsesStoredAccessHash(t *testing.T) {
	e := setup(t, nil)
	e.p.hash = map[int64]telegram.PeerInfo{-1000000000005: {Kind: jobs.PeerChannel, AccessHash: 88}}
	minCh := &tg.Channel{ID: 5, AccessHash: 66, Title: "T", Min: true, Megagroup: true}
	upd := &tg.Updates{Chats: []tg.ChatClass{minCh}, Updates: []tg.UpdateClass{&tg.UpdateNewChannelMessage{Message: channelMsg(9)}}}
	require.NoError(t, e.d.Handle(t.Context(), upd))
	require.Len(t, e.q.pl, 1)
	rd := e.q.pl[0].Read
	assert.Equal(t, jobs.ReadTarget{PeerKind: jobs.PeerChannel, PeerID: 5, AccessHash: 88}, rd)

	r, api, _ := newReader(t)
	api.EXPECT().ChannelsReadHistory(gomock.Any(), &tg.ChannelsReadHistoryRequest{
		Channel: &tg.InputChannel{ChannelID: 5, AccessHash: 88}, MaxID: 9,
	}).Return(true, nil)
	require.NoError(t, markNow(t.Context(), r, -1000000000005, rd, 9))
}

func TestListener_MinChannelWithoutStoredHashWarnsOnce(t *testing.T) {
	e := setup(t, nil)
	minCh := &tg.Channel{ID: 5, AccessHash: 66, Title: "T", Min: true}
	upd := &tg.Updates{Chats: []tg.ChatClass{minCh}, Updates: []tg.UpdateClass{
		&tg.UpdateNewChannelMessage{Message: channelMsg(9)},
		&tg.UpdateNewChannelMessage{Message: channelMsg(10)},
	}}
	require.NoError(t, e.d.Handle(t.Context(), upd))
	require.Len(t, e.q.pl, 2)
	assert.True(t, e.q.pl[0].Read.IsMin)
	assert.Equal(t, 1, strings.Count(e.buf.String(),
		`level=WARN msg="mark read skipped" component=telegram reason=no_full_access_hash chat_id=-1000000000005`))

	r, _, marks := newReader(t) // no API call expected
	require.NoError(t, markNow(t.Context(), r, -1000000000005, e.q.pl[0].Read, 9))
	assert.Empty(t, marks.m)
}
