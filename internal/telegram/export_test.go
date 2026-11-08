package telegram

import (
	"context"
	"time"
)

// SetBackfillSleep replaces the FLOOD_WAIT sleep of Backfill.
func SetBackfillSleep(l *Listener, f func(context.Context, time.Duration) error) { l.sleep = f }

// SetBackfillPagePause replaces the pause between Backfill pages.
func SetBackfillPagePause(l *Listener, d time.Duration) { l.pause = d }

// SetRateClock replaces the clock and sleep of a RateLimiter.
func SetRateClock(l *RateLimiter, now func() time.Time, sleep func(context.Context, time.Duration) error) {
	l.now, l.sleep = now, sleep
}
