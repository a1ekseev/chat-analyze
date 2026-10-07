package app

import (
	"context"
	"log/slog"
	"time"

	"github.com/a1ekseev/chat-analyze/internal/logx"
	"github.com/a1ekseev/chat-analyze/internal/queue"
)

const (
	maintenanceInterval = time.Minute
	statsInterval       = 5 * time.Minute
)

// RunMaintenance reaps expired leases and deletes old done and dead jobs
// every minute and logs queue stats every five minutes, until ctx is
// cancelled.
func RunMaintenance(ctx context.Context, q *queue.Queue, retention queue.Retention, logger *slog.Logger) error {
	tick := time.NewTicker(maintenanceInterval)
	defer tick.Stop()
	stats := time.NewTicker(statsInterval)
	defer stats.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			maintenanceTick(ctx, q, retention, logger)
		case <-stats.C:
			statsTick(ctx, q, logger)
		}
	}
}

func maintenanceTick(ctx context.Context, q *queue.Queue, retention queue.Retention, logger *slog.Logger) {
	log := logger.With(logx.Component("maintenance"))
	if n, err := q.Reap(ctx); err != nil {
		log.ErrorContext(ctx, "jobs reap failed", logx.Err(err))
	} else if n > 0 {
		log.WarnContext(ctx, "jobs reaped", slog.Int64("count", n))
	}
	if n, err := q.Cleanup(ctx, retention); err != nil {
		log.ErrorContext(ctx, "jobs cleanup failed", logx.Err(err))
	} else {
		log.DebugContext(ctx, "jobs cleaned up", slog.Int64("count", n))
	}
}

func statsTick(ctx context.Context, q *queue.Queue, logger *slog.Logger) {
	log := logger.With(logx.Component("maintenance"))
	s, err := q.Stats(ctx)
	if err != nil {
		log.ErrorContext(ctx, "queue stats failed", logx.Err(err))
		return
	}
	log.InfoContext(ctx, "queue stats",
		slog.Int64("pending", s.Pending),
		slog.Int64("running", s.Running),
		slog.Int64("dead", s.Dead),
		slog.Int64("done_24h", s.Done24h),
		slog.Int64("oldest_pending_age_s", int64(s.OldestPendingAge.Seconds())),
	)
}
