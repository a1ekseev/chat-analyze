package telegram

import (
	"context"
	"time"
)

// SetBackfillSleep replaces the FLOOD_WAIT sleep of Backfill.
func SetBackfillSleep(l *Listener, f func(context.Context, time.Duration) error) { l.sleep = f }
