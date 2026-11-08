package telegram_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	gotd "github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/stretchr/testify/require"

	"github.com/a1ekseev/chat-analyze/internal/telegram"
)

// fakeClock is a manual clock; sleep advances it.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func (c *fakeClock) sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.t = c.t.Add(d)
	return nil
}

// invoker records the start time of each call and returns errs in order.
type invoker struct {
	clock *fakeClock
	at    []time.Duration
	errs  []error
}

func (i *invoker) Invoke(context.Context, bin.Encoder, bin.Decoder) error {
	i.at = append(i.at, i.clock.t.Sub(time.Unix(0, 0)))
	if len(i.errs) > 0 {
		err := i.errs[0]
		i.errs = i.errs[1:]
		return err
	}
	return nil
}

func newLimited(rps float64) (*fakeClock, *invoker, func(context.Context) error) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	inv := &invoker{clock: clock}
	l := telegram.NewRateLimiter(rps, slog.New(slog.DiscardHandler))
	telegram.SetRateClock(l, clock.now, clock.sleep)
	call := l.Handle(inv)
	return clock, inv, func(ctx context.Context) error {
		return call(ctx, &tg.MessagesReadHistoryRequest{}, nil)
	}
}

func TestRateLimiter_BurstThenSteady(t *testing.T) {
	_, inv, call := newLimited(10)
	for range 6 {
		require.NoError(t, call(context.Background()))
	}
	ms := time.Millisecond
	require.Equal(t, []time.Duration{0, 0, 0, 100 * ms, 200 * ms, 300 * ms}, inv.at)
}

func TestRateLimiter_FloodWaitPausesEveryCall(t *testing.T) {
	_, inv, call := newLimited(10)
	inv.errs = []error{tgerr.New(420, "FLOOD_WAIT_5")}
	err := call(context.Background())
	d, ok := tgerr.AsFloodWait(err)
	require.True(t, ok, "the FLOOD_WAIT is returned, not retried")
	require.Equal(t, 5*time.Second, d)

	require.NoError(t, call(context.Background()))
	require.Equal(t, []time.Duration{0, 6 * time.Second}, inv.at, "next call waits X+1s")
}

func TestRateLimiter_PauseBeyondDeadlineFailsFast(t *testing.T) {
	clock, inv, call := newLimited(10)
	inv.errs = []error{tgerr.New(420, "FLOOD_WAIT_60")}
	_ = call(context.Background())

	ctx, cancel := context.WithDeadline(context.Background(), clock.t.Add(10*time.Second))
	defer cancel()
	err := call(ctx)
	d, ok := tgerr.AsFloodWait(err)
	require.True(t, ok)
	require.Equal(t, 61*time.Second, d)
	require.Len(t, inv.at, 1, "no RPC during the pause")
}

func TestRateLimiter_ContextCancelled(t *testing.T) {
	// Real clock: the 4th call waits ~1s for its slot.
	l := telegram.NewRateLimiter(1, slog.New(slog.DiscardHandler))
	call := l.Handle(gotd.InvokeFunc(func(context.Context, bin.Encoder, bin.Decoder) error { return nil }))
	for range 3 {
		require.NoError(t, call(context.Background(), &tg.MessagesReadHistoryRequest{}, nil))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := call(ctx, &tg.MessagesReadHistoryRequest{}, nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
