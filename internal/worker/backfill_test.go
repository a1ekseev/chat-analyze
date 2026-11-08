package worker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/gotd/td/tgerr"
	"github.com/stretchr/testify/require"

	"github.com/a1ekseev/chat-analyze/internal/jobs"
	"github.com/a1ekseev/chat-analyze/internal/queue"
	"github.com/a1ekseev/chat-analyze/internal/worker"
)

type fakeBackfiller struct {
	calls  []jobs.BackfillHistory
	err    error
	pan    bool
	ctxErr error
	hook   func(ctx context.Context)
}

func (f *fakeBackfiller) Backfill(ctx context.Context, chatID int64, minID, maxPages int) error {
	f.calls = append(f.calls, jobs.BackfillHistory{ChatID: chatID, MinID: minID, MaxPages: maxPages})
	if f.hook != nil {
		f.hook(ctx)
	}
	f.ctxErr = ctx.Err()
	if f.pan {
		panic("CANARY panic value")
	}
	if f.err != nil {
		return f.err
	}
	return ctx.Err()
}

func bfPayload(t *testing.T) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(jobs.BackfillHistory{ChatID: -1000000000005, MinID: 10, MaxPages: 3})
	require.NoError(t, err)
	return raw
}

func newBackfillWorker(q worker.Queue, b worker.Backfiller, buf *bytes.Buffer) *worker.LongWorker {
	l := slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return worker.NewBackfill(q, b, worker.Config{Lease: time.Minute, PollInterval: 10 * time.Millisecond}, l)
}

func TestBackfillWorker_Success(t *testing.T) {
	q := &fakeQueue{has: true, job: queue.Job{ID: 1, Kind: jobs.KindBackfillHistory, Attempts: 1, Payload: bfPayload(t)}}
	b := &fakeBackfiller{}
	did, err := newBackfillWorker(q, b, &bytes.Buffer{}).RunOnce(t.Context())
	require.NoError(t, err)
	require.True(t, did)
	require.Equal(t, jobs.KindBackfillHistory, q.kind)
	require.Equal(t, worker.BackfillLease, q.lease, "a long history fetch must not outlive its lease")
	require.Equal(t, []jobs.BackfillHistory{{ChatID: -1000000000005, MinID: 10, MaxPages: 3}}, b.calls)
	require.True(t, q.completed)
	require.False(t, q.failed)
}

func TestBackfillWorker_FloodWaitRetry(t *testing.T) {
	q := &fakeQueue{has: true, job: queue.Job{ID: 1, Attempts: 1, MaxAttempts: 10, Payload: bfPayload(t)}}
	var buf bytes.Buffer
	b := &fakeBackfiller{err: tgerr.New(420, "FLOOD_WAIT_3")}
	did, err := newBackfillWorker(q, b, &buf).RunOnce(t.Context())
	require.NoError(t, err)
	require.True(t, did)
	require.False(t, q.failed, "a FLOOD_WAIT must not count the attempt")
	require.True(t, q.released)
	require.Equal(t, 4*time.Second, q.retryIn)
	require.Contains(t, buf.String(), `"msg":"flood wait","component":"worker","job_id":1,"chat_id":-1000000000005,"wait_s":3`)
	require.Contains(t, buf.String(), `"msg":"job released"`)
}

func TestBackfillWorker_ErrorBackoff(t *testing.T) {
	q := &fakeQueue{has: true, job: queue.Job{ID: 1, Attempts: 3, MaxAttempts: 10, Payload: bfPayload(t)}}
	_, err := newBackfillWorker(q, &fakeBackfiller{err: errors.New("rpc down")}, &bytes.Buffer{}).RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, queue.Backoff(3), q.retryIn)
}

func TestBackfillWorker_BadPayloadPermanent(t *testing.T) {
	q := &fakeQueue{has: true, job: queue.Job{ID: 1, Attempts: 1, Payload: json.RawMessage(`{"chat_id":"CANARY"}`)}}
	b := &fakeBackfiller{}
	_, err := newBackfillWorker(q, b, &bytes.Buffer{}).RunOnce(t.Context())
	require.NoError(t, err)
	require.ErrorIs(t, q.cause, queue.ErrPermanent)
	require.NotContains(t, q.cause.Error(), "CANARY")
	require.Empty(t, b.calls)
}

func TestBackfillWorker_PanicRecovered(t *testing.T) {
	q := &fakeQueue{has: true, job: queue.Job{ID: 1, Attempts: 1, MaxAttempts: 10, Payload: bfPayload(t)}}
	var buf bytes.Buffer
	_, err := newBackfillWorker(q, &fakeBackfiller{pan: true}, &buf).RunOnce(t.Context())
	require.NoError(t, err)
	require.True(t, q.failed)
	require.Contains(t, buf.String(), `"msg":"panic recovered"`)
	require.NotContains(t, buf.String(), "CANARY")
}

// Shutdown interrupts a long history fetch; the job is released (attempt not
// burned) with a live context and retried after the restart.
func TestBackfillWorker_ShutdownReleasesJob(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	q := newHookQueue(queue.Job{ID: 1, Attempts: 1, MaxAttempts: 10, Payload: bfPayload(t)})
	b := &fakeBackfiller{hook: func(context.Context) { cancel() }}
	var buf bytes.Buffer
	w := worker.NewBackfill(q, b, worker.Config{Lease: time.Minute, PollInterval: time.Hour},
		slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	did, err := w.RunOnce(ctx)
	require.NoError(t, err)
	require.True(t, did)
	require.ErrorIs(t, b.ctxErr, context.Canceled, "the fetch sees the shutdown")
	require.NoError(t, recvErr(t, q.releases), "Release must get a live context")
	noCall(t, q.fails, "Fail")
	require.Contains(t, buf.String(), `"msg":"job released"`)
}

// A handler error without shutdown (here a deadline from below) is failed.
func TestBackfillWorker_ErrorWithoutShutdownFails(t *testing.T) {
	q := newHookQueue(queue.Job{ID: 1, Attempts: 1, MaxAttempts: 10, Payload: bfPayload(t)})
	b := &fakeBackfiller{err: context.DeadlineExceeded}
	w := worker.NewBackfill(q, b, worker.Config{Lease: time.Minute, PollInterval: time.Hour},
		slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)))
	did, err := w.RunOnce(t.Context())
	require.NoError(t, err)
	require.True(t, did)
	require.NoError(t, recvErr(t, q.fails))
	noCall(t, q.releases, "Release")
}

func TestBackfillWorker_RunStopsOnCancel(t *testing.T) {
	q := newHookQueue()
	w := worker.NewBackfill(q, &fakeBackfiller{}, worker.Config{Lease: time.Minute, PollInterval: 5 * time.Millisecond},
		slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	time.Sleep(30 * time.Millisecond)
	q.push(queue.Job{ID: 2, Attempts: 1, Payload: bfPayload(t)})
	require.NoError(t, recvErr(t, q.completes))
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Run did not stop")
	}
}

type fakeSyncer struct {
	calls int
	err   error
}

func (f *fakeSyncer) SyncDialogs(context.Context) error { f.calls++; return f.err }

func TestSyncWorker_RunsPass(t *testing.T) {
	q := &fakeQueue{has: true, job: queue.Job{ID: 1, Attempts: 1, Payload: json.RawMessage(`{}`)}}
	s := &fakeSyncer{}
	w := worker.NewSync(q, s, worker.Config{Lease: time.Minute, PollInterval: time.Hour}, slog.New(slog.DiscardHandler))
	did, err := w.RunOnce(t.Context())
	require.NoError(t, err)
	require.True(t, did)
	require.Equal(t, jobs.KindSyncDialogs, q.kind)
	require.Equal(t, worker.BackfillLease, q.lease)
	require.Equal(t, 1, s.calls)
	require.True(t, q.completed)
}

func TestSyncWorker_FloodWaitReleases(t *testing.T) {
	q := &fakeQueue{has: true, job: queue.Job{ID: 1, Attempts: 1, MaxAttempts: 10, Payload: json.RawMessage(`{}`)}}
	w := worker.NewSync(q, &fakeSyncer{err: tgerr.New(420, "FLOOD_WAIT_7")},
		worker.Config{Lease: time.Minute, PollInterval: time.Hour}, slog.New(slog.DiscardHandler))
	_, err := w.RunOnce(t.Context())
	require.NoError(t, err)
	require.True(t, q.released)
	require.False(t, q.failed)
	require.Equal(t, 8*time.Second, q.retryIn)
}
