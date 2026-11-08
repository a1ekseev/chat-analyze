package telegram

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/a1ekseev/chat-analyze/internal/logx"
)

// rateBurst is how many RPCs may go back to back after an idle period.
const rateBurst = 3

// RateLimiter is an account-wide RPC limiter (a gotd middleware): at most
// rps calls per second with a small burst, and after any FLOOD_WAIT_X every
// call waits until X+1s have passed. The FLOOD_WAIT error itself is returned
// to the caller (no retry here), so callers that wait it out themselves
// (Backfill) and the queue keep their own handling; their wait ends at the
// same deadline. A call whose ctx deadline comes before the pause ends fails
// at once with a FLOOD_WAIT error for the remaining time.
type RateLimiter struct {
	interval time.Duration
	log      *slog.Logger
	now      func() time.Time
	sleep    func(context.Context, time.Duration) error

	mu     sync.Mutex
	next   time.Time // GCRA theoretical arrival time
	paused time.Time // no call starts before this (FLOOD_WAIT)
}

// NewRateLimiter returns a limiter of rps calls per second.
func NewRateLimiter(rps float64, logger *slog.Logger) *RateLimiter {
	return &RateLimiter{
		interval: time.Duration(float64(time.Second) / rps),
		log:      logger.With(logx.Component("telegram")),
		now:      time.Now,
		sleep:    sleepCtx,
	}
}

var _ telegram.Middleware = (*RateLimiter)(nil)

// Handle implements telegram.Middleware.
func (l *RateLimiter) Handle(next tg.Invoker) telegram.InvokeFunc {
	return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		if err := l.wait(ctx); err != nil {
			return err
		}
		err := next.Invoke(ctx, input, output)
		if d, ok := tgerr.AsFloodWait(err); ok {
			l.pause(ctx, d+time.Second, input)
		}
		return err //nolint:wrapcheck // transparent middleware
	}
}

// reserve takes the next slot and returns when it starts.
func (l *RateLimiter) reserve() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	tolerance := time.Duration(rateBurst-1) * l.interval
	at := latest(l.next.Add(-tolerance), now, l.paused)
	l.next = latest(l.next, at).Add(l.interval)
	return at
}

func latest(ts ...time.Time) time.Time {
	out := ts[0]
	for _, t := range ts[1:] {
		if t.After(out) {
			out = t
		}
	}
	return out
}

func (l *RateLimiter) pausedUntil() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.paused
}

func (l *RateLimiter) pause(ctx context.Context, d time.Duration, input bin.Encoder) {
	l.mu.Lock()
	until := l.now().Add(d)
	if until.After(l.paused) {
		l.paused = until
	}
	l.mu.Unlock()
	method := "unknown"
	if t, ok := input.(interface{ TypeName() string }); ok {
		method = t.TypeName()
	}
	l.log.WarnContext(ctx, "flood wait", slog.String("method", method), slog.Int64("wait_s", int64(d/time.Second)))
}

// wait blocks until the call may start. A pause set while sleeping is
// honoured too.
func (l *RateLimiter) wait(ctx context.Context) error {
	for {
		at := l.reserve()
		if p := l.pausedUntil(); p.After(l.now()) {
			if dl, ok := ctx.Deadline(); ok && !dl.After(p) {
				// The caller can not wait that long: let it reschedule.
				return tgerr.New(420, fmt.Sprintf("FLOOD_WAIT_%d", int(math.Ceil(p.Sub(l.now()).Seconds()))))
			}
		}
		if d := at.Sub(l.now()); d > 0 {
			if err := l.sleep(ctx, d); err != nil {
				return err
			}
		}
		if !l.pausedUntil().After(l.now()) {
			return nil
		}
	}
}
