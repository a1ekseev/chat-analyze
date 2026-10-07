package worker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/tgerr"
	"github.com/stretchr/testify/require"

	"github.com/a1ekseev/chat-analyze/internal/jobs"
	"github.com/a1ekseev/chat-analyze/internal/message"
	"github.com/a1ekseev/chat-analyze/internal/queue"
	"github.com/a1ekseev/chat-analyze/internal/worker"
)

type calls struct {
	mu   sync.Mutex
	list []string
}

func (c *calls) add(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.list = append(c.list, s)
}

type fakeStore struct {
	c    *calls
	pan  bool
	hook func(ctx context.Context) error
}

func (f *fakeStore) Upsert(ctx context.Context, _ message.Message) error {
	if f.hook != nil {
		if err := f.hook(ctx); err != nil {
			return err
		}
	}
	if f.pan {
		panic("boom CANARY")
	}
	f.c.add("upsert")
	return nil
}

func (f *fakeStore) UpsertTopic(context.Context, int64, int, string) error {
	f.c.add("topic")
	return nil
}

type fakeReader struct {
	c   *calls
	err error
}

func (f *fakeReader) MarkRead(context.Context, int64, jobs.ReadTarget, int) error {
	f.c.add("markread")
	return f.err
}

type fakeQueue struct {
	job        queue.Job
	has        bool
	retryIn    time.Duration
	failed     bool
	completed  bool
	dead       bool
	superseded bool
	claimErr   error
	kind       string
	lease      time.Duration
	cause      error
	released   bool
}

func (q *fakeQueue) Claim(ctx context.Context, kind string, lease time.Duration) (queue.Job, bool, error) {
	q.kind, q.lease = kind, lease
	if q.claimErr != nil {
		return queue.Job{}, false, q.claimErr
	}
	if q.has {
		q.has = false
		return q.job, true, nil
	}
	return queue.Job{}, false, ctx.Err()
}

func (q *fakeQueue) Complete(context.Context, int64, int) (bool, error) {
	q.completed = true
	return !q.superseded, nil
}

func (q *fakeQueue) Fail(_ context.Context, _ queue.Job, retryIn time.Duration, cause error) (bool, error) {
	q.failed, q.retryIn, q.cause = true, retryIn, cause
	return q.dead, nil
}

func (q *fakeQueue) Release(context.Context, int64, int) (bool, error) {
	q.released = true
	return true, nil
}

func payload(t *testing.T, topic bool) json.RawMessage {
	t.Helper()
	m := message.Message{
		ChatID: 5, MessageID: 9, ChatType: message.ChatTypeGroup,
		Text: new("héllo"), Entities: json.RawMessage(`[{"type":"bold"}]`),
	}
	if topic {
		m.TopicID, m.TopicTitle = new(3), new("Topic")
	}
	raw, err := json.Marshal(jobs.SaveMessage{Message: m})
	require.NoError(t, err)
	return raw
}

func newWorker(q worker.Queue, s *fakeStore, r *fakeReader, markRead bool, buf *bytes.Buffer) *worker.Worker {
	l := slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return worker.New(q, s, r, worker.Config{MarkRead: markRead, Lease: time.Minute, PollInterval: 10 * time.Millisecond}, l)
}

func TestHandle_UpsertThenMarkRead(t *testing.T) {
	c := &calls{}
	w := newWorker(&fakeQueue{}, &fakeStore{c: c}, &fakeReader{c: c}, true, &bytes.Buffer{})
	require.NoError(t, w.Handle(t.Context(), queue.Job{Payload: payload(t, false)}))
	require.Equal(t, []string{"upsert", "markread"}, c.list)
}

func TestHandle_MarkReadDisabled(t *testing.T) {
	c := &calls{}
	w := newWorker(&fakeQueue{}, &fakeStore{c: c}, &fakeReader{c: c}, false, &bytes.Buffer{})
	require.NoError(t, w.Handle(t.Context(), queue.Job{Payload: payload(t, false)}))
	require.Equal(t, []string{"upsert"}, c.list)
}

func TestHandle_TopicTitleUpserted(t *testing.T) {
	c := &calls{}
	w := newWorker(&fakeQueue{}, &fakeStore{c: c}, &fakeReader{c: c}, true, &bytes.Buffer{})
	require.NoError(t, w.Handle(t.Context(), queue.Job{Payload: payload(t, true)}))
	require.Equal(t, []string{"upsert", "topic", "markread"}, c.list)
}

func TestHandle_BadPayloadPermanent(t *testing.T) {
	c := &calls{}
	w := newWorker(&fakeQueue{}, &fakeStore{c: c}, &fakeReader{c: c}, true, &bytes.Buffer{})
	err := w.Handle(t.Context(), queue.Job{Payload: json.RawMessage(`{not json`)})
	require.ErrorIs(t, err, queue.ErrPermanent)
	require.Empty(t, c.list)
}

func TestHandle_PanicRecovered(t *testing.T) {
	var buf bytes.Buffer
	w := newWorker(&fakeQueue{}, &fakeStore{c: &calls{}, pan: true}, &fakeReader{}, true, &buf)
	err := w.Handle(t.Context(), queue.Job{ID: 4, Payload: payload(t, false)})
	require.Error(t, err)
	require.NotContains(t, err.Error(), "CANARY")
	require.Contains(t, buf.String(), `"msg":"panic recovered"`)
	require.NotContains(t, buf.String(), `"CANARY"`)
}

func TestRunOnce_FloodWaitRetryIn(t *testing.T) {
	c := &calls{}
	q := &fakeQueue{has: true, job: queue.Job{ID: 1, Attempts: 1, MaxAttempts: 5, Payload: payload(t, false)}}
	var buf bytes.Buffer
	w := newWorker(q, &fakeStore{c: c}, &fakeReader{c: c, err: tgerr.New(420, "FLOOD_WAIT_5")}, true, &buf)
	did, err := w.RunOnce(t.Context())
	require.NoError(t, err)
	require.True(t, did)
	require.True(t, q.failed)
	require.Equal(t, 6*time.Second, q.retryIn)
	require.Contains(t, buf.String(), `"msg":"flood wait"`)
	require.Contains(t, buf.String(), `"msg":"job failed, will retry"`)
}

func TestRunOnce_BackoffOnError(t *testing.T) {
	c := &calls{}
	q := &fakeQueue{has: true, job: queue.Job{ID: 1, Attempts: 3, MaxAttempts: 5, Payload: payload(t, false)}}
	w := newWorker(q, &fakeStore{c: c}, &fakeReader{c: c, err: errors.New("net")}, true, &bytes.Buffer{})
	_, err := w.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 4*time.Second, q.retryIn)
}

func TestRunOnce_DeadAndSupersededAndStored(t *testing.T) {
	c := &calls{}
	var buf bytes.Buffer
	q := &fakeQueue{has: true, dead: true, job: queue.Job{ID: 1, Attempts: 5, MaxAttempts: 5, Payload: json.RawMessage(`x`)}}
	w := newWorker(q, &fakeStore{c: c}, &fakeReader{c: c}, true, &buf)
	_, err := w.RunOnce(t.Context())
	require.NoError(t, err)
	require.Contains(t, buf.String(), `"msg":"job dead"`)

	buf.Reset()
	q = &fakeQueue{has: true, superseded: true, job: queue.Job{ID: 2, Version: 1, Payload: payload(t, false)}}
	w = newWorker(q, &fakeStore{c: c}, &fakeReader{c: c}, false, &buf)
	_, err = w.RunOnce(t.Context())
	require.NoError(t, err)
	require.Contains(t, buf.String(), `"msg":"job superseded"`)

	buf.Reset()
	q = &fakeQueue{has: true, job: queue.Job{ID: 3, Attempts: 1, Payload: payload(t, false)}}
	w = newWorker(q, &fakeStore{c: c}, &fakeReader{c: c}, false, &buf)
	_, err = w.RunOnce(t.Context())
	require.NoError(t, err)
	require.Contains(t, buf.String(), `"msg":"message stored"`)
	require.Contains(t, buf.String(), `"text_len":5`)
	require.Contains(t, buf.String(), `"entities_count":1`)
	require.NotContains(t, buf.String(), "héllo")
}

func TestWorker_StopsOnContextCancel(t *testing.T) {
	q := &fakeQueue{claimErr: errors.New("db down")}
	w := newWorker(q, &fakeStore{c: &calls{}}, &fakeReader{}, true, &bytes.Buffer{})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx, nil) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Run did not stop")
	}
}

// hookQueue is a concurrency-safe fake queue for Run/RunOnce context tests.
type hookQueue struct {
	mu        sync.Mutex
	jobs      []queue.Job
	claims    int
	completes chan error // ctx.Err() seen by Complete
	fails     chan error // ctx.Err() seen by Fail
	failCause chan error
	releases  chan error // ctx.Err() seen by Release
}

func newHookQueue(initial ...queue.Job) *hookQueue {
	return &hookQueue{
		jobs: initial, completes: make(chan error, 8), fails: make(chan error, 8), failCause: make(chan error, 8),
		releases: make(chan error, 8),
	}
}

func (q *hookQueue) push(j queue.Job) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.jobs = append(q.jobs, j)
}

func (q *hookQueue) Claim(context.Context, string, time.Duration) (queue.Job, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.claims++
	if len(q.jobs) == 0 {
		return queue.Job{}, false, nil
	}
	j := q.jobs[0]
	q.jobs = q.jobs[1:]
	return j, true, nil
}

func (q *hookQueue) Complete(ctx context.Context, _ int64, _ int) (bool, error) {
	q.completes <- ctx.Err()
	return true, nil
}

func (q *hookQueue) Fail(ctx context.Context, _ queue.Job, _ time.Duration, cause error) (bool, error) {
	q.fails <- ctx.Err()
	q.failCause <- cause
	return false, nil
}

func (q *hookQueue) Release(ctx context.Context, _ int64, _ int) (bool, error) {
	q.releases <- ctx.Err()
	return true, nil
}

func noCall(t *testing.T, ch <-chan error, what string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("%s must not be called", what)
	default:
	}
}

func newWorkerCfg(q worker.Queue, s *fakeStore, cfg worker.Config, buf *bytes.Buffer) *worker.Worker {
	l := slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return worker.New(q, s, &fakeReader{c: &calls{}}, cfg, l)
}

func recvErr(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for queue call")
		return nil
	}
}

func TestRunOnce_FailUsesFreshContextAfterLeaseTimeout(t *testing.T) {
	q := newHookQueue(queue.Job{ID: 1, Attempts: 1, MaxAttempts: 5, Payload: payload(t, false)})
	s := &fakeStore{c: &calls{}, hook: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	w := newWorkerCfg(q, s, worker.Config{Lease: 20 * time.Millisecond, PollInterval: time.Hour}, &bytes.Buffer{})
	did, err := w.RunOnce(t.Context())
	require.NoError(t, err)
	require.True(t, did)
	require.NoError(t, recvErr(t, q.fails), "Fail must get a live context")
	require.ErrorIs(t, recvErr(t, q.failCause), context.DeadlineExceeded)
}

// A save_message that fails while the service shuts down is released, not
// failed: shutdown must not burn an attempt.
func TestRunOnce_ShutdownReleasesJob(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	q := newHookQueue(queue.Job{ID: 1, Attempts: 1, MaxAttempts: 5, Payload: payload(t, false)})
	s := &fakeStore{c: &calls{}, hook: func(context.Context) error { cancel(); return errors.New("pool closed") }}
	var buf bytes.Buffer
	w := newWorkerCfg(q, s, worker.Config{Lease: time.Minute, PollInterval: time.Hour}, &buf)
	did, err := w.RunOnce(ctx)
	require.NoError(t, err)
	require.True(t, did)
	require.NoError(t, recvErr(t, q.releases), "Release must get a live context")
	noCall(t, q.fails, "Fail")
	require.Contains(t, buf.String(), `"msg":"job released","component":"worker","job_id":1`)
}

// A lease timeout is a real failure even if shutdown started meanwhile.
func TestRunOnce_LeaseTimeoutFailsEvenDuringShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	q := newHookQueue(queue.Job{ID: 1, Attempts: 1, MaxAttempts: 5, Payload: payload(t, false)})
	s := &fakeStore{c: &calls{}, hook: func(hctx context.Context) error { <-hctx.Done(); cancel(); return hctx.Err() }}
	w := newWorkerCfg(q, s, worker.Config{Lease: 20 * time.Millisecond, PollInterval: time.Hour}, &bytes.Buffer{})
	did, err := w.RunOnce(ctx)
	require.NoError(t, err)
	require.True(t, did)
	require.NoError(t, recvErr(t, q.fails))
	require.ErrorIs(t, recvErr(t, q.failCause), context.DeadlineExceeded)
	noCall(t, q.releases, "Release")
}

func TestRunOnce_InFlightJobCompletesAfterCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	q := newHookQueue(queue.Job{ID: 1, Attempts: 1, Payload: payload(t, false)})
	s := &fakeStore{c: &calls{}, hook: func(context.Context) error { cancel(); return nil }}
	w := newWorkerCfg(q, s, worker.Config{Lease: time.Minute, PollInterval: time.Hour}, &bytes.Buffer{})
	did, err := w.RunOnce(ctx)
	require.NoError(t, err)
	require.True(t, did)
	require.NoError(t, recvErr(t, q.completes), "Complete must get a live context")
}

func TestRun_WakesOnChannelWithoutWaitingForPoll(t *testing.T) {
	q := newHookQueue()
	w := newWorkerCfg(q, &fakeStore{c: &calls{}}, worker.Config{Lease: time.Minute, PollInterval: time.Hour}, &bytes.Buffer{})
	ctx, cancel := context.WithCancel(t.Context())
	wake := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx, wake) }()
	time.Sleep(50 * time.Millisecond) // let Run go idle
	q.push(queue.Job{ID: 1, Attempts: 1, Payload: payload(t, false)})
	wake <- struct{}{}
	require.NoError(t, recvErr(t, q.completes))
	cancel()
	require.NoError(t, <-done)
}

func TestRun_IdleCancelReturnsPromptly(t *testing.T) {
	q := newHookQueue()
	w := newWorkerCfg(q, &fakeStore{c: &calls{}}, worker.Config{Lease: time.Minute, PollInterval: time.Hour}, &bytes.Buffer{})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx, make(chan struct{})) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("idle Run did not stop")
	}
}

func TestRun_ClosedWakeChannelDoesNotSpin(t *testing.T) {
	q := newHookQueue()
	w := newWorkerCfg(q, &fakeStore{c: &calls{}}, worker.Config{Lease: time.Minute, PollInterval: time.Hour}, &bytes.Buffer{})
	ctx, cancel := context.WithCancel(t.Context())
	wake := make(chan struct{})
	close(wake)
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx, wake) }()
	time.Sleep(100 * time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	q.mu.Lock()
	defer q.mu.Unlock()
	require.LessOrEqual(t, q.claims, 3)
}

func TestRunOnce_DecodeErrorLeaksNothing(t *testing.T) {
	var buf bytes.Buffer
	q := newHookQueue(queue.Job{ID: 1, Attempts: 1, MaxAttempts: 5, Payload: json.RawMessage(`{"message":"CANARY"}`)})
	w := newWorkerCfg(q, &fakeStore{c: &calls{}}, worker.Config{Lease: time.Minute, PollInterval: time.Hour}, &buf)
	_, err := w.RunOnce(t.Context())
	require.NoError(t, err)
	recvErr(t, q.fails)
	cause := recvErr(t, q.failCause)
	require.ErrorIs(t, cause, queue.ErrPermanent)
	require.NotContains(t, cause.Error(), "CANARY")
	require.NotContains(t, buf.String(), "CANARY")
}
