package app

import (
	"context"
	"log/slog"
	"time"

	"github.com/a1ekseev/chat-analyze/internal/jobs"
	"github.com/a1ekseev/chat-analyze/internal/logx"
	"github.com/a1ekseev/chat-analyze/internal/queue"
)

// readyPoll is how often the scheduler checks that Telegram is ready.
const readyPoll = time.Second

// syncEnqueuer is the part of queue.Queue the dialog sync scheduler needs.
type syncEnqueuer interface {
	EnqueueIdle(ctx context.Context, kind, dedupKey string, payload any) (queue.EnqueueResult, error)
}

// RunDialogSync enqueues the sync_dialogs job as soon as Telegram is
// connected and authorized (startup catch-up), then every interval, until
// ctx is cancelled. A job still pending or running is left alone, so at
// most one pass is queued. Enqueue errors are logged; the next tick retries.
func RunDialogSync(ctx context.Context, q syncEnqueuer, interval time.Duration, ready func() bool, logger *slog.Logger) error {
	log := logger.With(logx.Component("sync"))
	enqueue := func() {
		res, err := q.EnqueueIdle(ctx, jobs.KindSyncDialogs, jobs.SyncDialogsDedupKey, struct{}{})
		if err != nil {
			if ctx.Err() == nil {
				log.ErrorContext(ctx, "dialog sync enqueue failed", logx.Err(err))
			}
			return
		}
		log.DebugContext(ctx, "job enqueued", logx.JobID(res.ID), slog.String("job_kind", jobs.KindSyncDialogs),
			slog.Int("version", res.Version), slog.Bool("inserted", res.Inserted))
	}
	poll := time.NewTicker(readyPoll)
	defer poll.Stop()
	for !ready() {
		select {
		case <-ctx.Done():
			return nil
		case <-poll.C:
		}
	}
	enqueue()
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			enqueue()
		}
	}
}
