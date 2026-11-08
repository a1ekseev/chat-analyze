package app

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/a1ekseev/chat-analyze/internal/logx"
)

type events struct {
	mu  sync.Mutex
	log []string
}

func (e *events) add(s string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.log = append(e.log, s)
}

func (e *events) list() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.log)
}

// fakeHTTP blocks Serve until Stop is called.
type fakeHTTP struct {
	ev      *events
	stopped chan struct{}
	once    sync.Once
}

func newFakeHTTP(ev *events) *fakeHTTP { return &fakeHTTP{ev: ev, stopped: make(chan struct{})} }

func (f *fakeHTTP) serve() error { <-f.stopped; return nil }

func (f *fakeHTTP) stop(context.Context) error {
	f.ev.add("http stopped")
	f.once.Do(func() { close(f.stopped) })
	return nil
}

func TestRunStages_ShutdownOrder(t *testing.T) {
	ev := &events{}
	h := newFakeHTTP(ev)
	tgAlive := make(chan struct{})
	var workerSawLiveTelegram, flushSawLiveTelegram bool
	startCtx := make(chan context.Context, 1)
	s := stages{
		Start: func(ctx context.Context) { startCtx <- ctx },
		Background: []func(context.Context) error{
			func(ctx context.Context) error {
				<-ctx.Done()
				// In-flight job still needs Telegram (MarkRead).
				time.Sleep(50 * time.Millisecond)
				select {
				case <-tgAlive:
				default:
					workerSawLiveTelegram = true
				}
				ev.add("worker done")
				return nil
			},
		},
		Flush: func(ctx context.Context) {
			_, hasDeadline := ctx.Deadline()
			select {
			case <-tgAlive:
			default:
				flushSawLiveTelegram = hasDeadline && ctx.Err() == nil
			}
			ev.add("flush")
		},
		Telegram: func(ctx context.Context) error {
			<-ctx.Done()
			close(tgAlive)
			ev.add("telegram done")
			return nil
		},
		Serve: h.serve,
		Stop:  h.stop,
	}
	var logs bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runStages(ctx, s, 5*time.Second, logx.New(&logs, slog.LevelInfo, "json")) }()
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("runStages did not return")
	}
	require.Error(t, (<-startCtx).Err(), "Start gets the Background ctx, cancelled by now")
	assert.True(t, workerSawLiveTelegram, "telegram must outlive the workers")
	assert.True(t, flushSawLiveTelegram, "read marks are flushed, bounded, before telegram stops")
	assert.Equal(t, []string{"worker done", "flush", "telegram done", "http stopped"}, ev.list())
	assert.Contains(t, logs.String(), `"msg":"shutdown started"`)
	assert.Contains(t, logs.String(), `"msg":"shutdown complete"`)
	assert.Contains(t, logs.String(), `"reason":"signal"`)
}

func TestRunStages_TelegramFailureStopsEverything(t *testing.T) {
	ev := &events{}
	h := newFakeHTTP(ev)
	boom := errors.New("telegram boom")
	s := stages{
		Background: []func(context.Context) error{
			func(ctx context.Context) error { <-ctx.Done(); ev.add("worker done"); return nil },
		},
		Telegram: func(context.Context) error { return boom },
		Serve:    h.serve,
		Stop:     h.stop,
	}
	var logs bytes.Buffer
	err := runStages(context.Background(), s, 5*time.Second, logx.New(&logs, slog.LevelInfo, "json"))
	require.ErrorIs(t, err, boom)
	assert.Equal(t, []string{"worker done", "http stopped"}, ev.list())
	assert.Contains(t, logs.String(), `"reason":"error"`)
}

func TestRunStages_WorkerFailureStopsTelegram(t *testing.T) {
	ev := &events{}
	h := newFakeHTTP(ev)
	boom := errors.New("worker boom")
	s := stages{
		Background: []func(context.Context) error{
			func(context.Context) error { return boom },
		},
		Telegram: func(ctx context.Context) error { <-ctx.Done(); ev.add("telegram done"); return nil },
		Serve:    h.serve,
		Stop:     h.stop,
	}
	err := runStages(context.Background(), s, 5*time.Second, logx.New(&bytes.Buffer{}, slog.LevelInfo, "json"))
	require.ErrorIs(t, err, boom)
	assert.Equal(t, []string{"telegram done", "http stopped"}, ev.list())
}

func TestRunStages_HTTPFailureStopsEverything(t *testing.T) {
	ev := &events{}
	boom := errors.New("listen boom")
	s := stages{
		Background: []func(context.Context) error{
			func(ctx context.Context) error { <-ctx.Done(); ev.add("worker done"); return nil },
		},
		Telegram: func(ctx context.Context) error { <-ctx.Done(); ev.add("telegram done"); return nil },
		Serve:    func() error { return boom },
		Stop:     func(context.Context) error { ev.add("http stopped"); return nil },
	}
	err := runStages(context.Background(), s, 5*time.Second, logx.New(&bytes.Buffer{}, slog.LevelInfo, "json"))
	require.ErrorIs(t, err, boom)
	assert.Equal(t, []string{"worker done", "telegram done", "http stopped"}, ev.list())
}

func TestRunStages_Timeout(t *testing.T) {
	stuck := make(chan struct{})
	defer close(stuck)
	h := newFakeHTTP(&events{})
	s := stages{
		Background: []func(context.Context) error{
			func(context.Context) error { <-stuck; return nil }, // ignores cancellation
		},
		Telegram: func(ctx context.Context) error { <-ctx.Done(); return nil },
		Serve:    h.serve,
		Stop:     h.stop,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var logs bytes.Buffer
	err := runStages(ctx, s, 100*time.Millisecond, logx.New(&logs, slog.LevelInfo, "json"))
	require.ErrorIs(t, err, ErrShutdownTimeout)
	assert.Contains(t, logs.String(), `"msg":"shutdown timed out"`)
}
