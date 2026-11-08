package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/a1ekseev/chat-analyze/internal/jobs"
	"github.com/a1ekseev/chat-analyze/internal/logx"
	"github.com/a1ekseev/chat-analyze/internal/queue"
)

// BackfillLease is the minimum lease of backfill_history and sync_dialogs
// jobs: up to 1000 history pages 1s apart (~17m plus RTT, ~25m at 0.5s RTT),
// plus up to 30 minutes of in-job FLOOD_WAIT waiting
// (telegram.Listener.Backfill), takes far longer than one save_message job.
const BackfillLease = 60 * time.Minute

// Backfiller fetches a chat gap and enqueues its messages
// (telegram.Listener.Backfill).
type Backfiller interface {
	Backfill(ctx context.Context, chatID int64, minID, maxPages int) error
}

// DialogSyncer runs one dialog sync pass (telegram.DialogSync.SyncDialogs).
type DialogSyncer interface {
	SyncDialogs(ctx context.Context) error
}

// LongWorker processes long jobs of one kind (backfill_history or
// sync_dialogs), one at a time.
type LongWorker struct {
	q     Queue
	kind  string
	run   func(ctx context.Context, payload json.RawMessage) (chatID *int64, err error)
	lease time.Duration
	poll  time.Duration
	log   *slog.Logger
}

func newLong(q Queue, kind string, cfg Config, logger *slog.Logger,
	run func(ctx context.Context, payload json.RawMessage) (*int64, error),
) *LongWorker {
	return &LongWorker{
		q: q, kind: kind, run: run, lease: max(cfg.Lease, BackfillLease), poll: cfg.PollInterval,
		log: logger.With(logx.Component("worker")),
	}
}

// NewBackfill creates the backfill_history worker. Its lease is
// max(cfg.Lease, BackfillLease); MarkRead is not used.
func NewBackfill(q Queue, b Backfiller, cfg Config, logger *slog.Logger) *LongWorker {
	return newLong(q, jobs.KindBackfillHistory, cfg, logger, func(ctx context.Context, raw json.RawMessage) (*int64, error) {
		var p jobs.BackfillHistory
		// The decode error is dropped on purpose: json errors can quote payload bytes.
		if err := json.Unmarshal(raw, &p); err != nil || p.ChatID == 0 {
			return nil, fmt.Errorf("decode payload: %w", queue.ErrPermanent)
		}
		if err := b.Backfill(ctx, p.ChatID, p.MinID, p.MaxPages); err != nil {
			return &p.ChatID, fmt.Errorf("backfill chat %d: %w", p.ChatID, err)
		}
		return &p.ChatID, nil
	})
}

// NewSync creates the sync_dialogs worker (same lease as NewBackfill).
func NewSync(q Queue, s DialogSyncer, cfg Config, logger *slog.Logger) *LongWorker {
	return newLong(q, jobs.KindSyncDialogs, cfg, logger, func(ctx context.Context, _ json.RawMessage) (*int64, error) {
		if err := s.SyncDialogs(ctx); err != nil {
			return nil, fmt.Errorf("sync dialogs: %w", err)
		}
		return nil, nil
	})
}

// handle runs one job; panics are recovered into errors.
func (w *LongWorker) handle(ctx context.Context, job queue.Job) (chatID *int64, err error) {
	defer func() {
		if r := recover(); r != nil {
			w.log.ErrorContext(ctx, "panic recovered", logx.JobID(job.ID),
				slog.String("panic", fmt.Sprintf("%T", r)))
			err = fmt.Errorf("job %d: panic: %T", job.ID, r)
		}
	}()
	return w.run(ctx, job.Payload)
}

// RunOnce claims one job and processes it; it reports whether a job existed.
// Unlike save_message the work is interrupted by shutdown (ctx): the job is
// then released (queue.Release, the attempt is not counted) with a live
// context and runs again after the restart with the same payload, so nothing
// is lost. Errors, lease timeouts and FLOOD_WAITs longer than the in-job cap
// follow the save_message retry rules (a FLOOD_WAIT does not count the
// attempt).
func (w *LongWorker) RunOnce(ctx context.Context) (bool, error) {
	job, ok, err := w.q.Claim(ctx, w.kind, w.lease)
	if err != nil {
		return false, fmt.Errorf("claim: %w", err)
	}
	if !ok {
		return false, nil
	}
	hctx, hcancel := context.WithTimeout(ctx, w.lease)
	chatID, herr := w.handle(hctx, job)
	shutdown := herr != nil && interruptedByShutdown(ctx, hctx)
	hcancel()

	// Bookkeeping gets its own live context: shutdown may have cancelled ctx.
	jctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finishTimeout)
	defer cancel()
	if shutdown {
		return true, releaseJob(jctx, w.q, w.log, job, 0)
	}
	if herr != nil {
		return true, failJob(jctx, w.q, w.log, job, herr, chatID)
	}
	done, err := w.q.Complete(jctx, job.ID, job.Version)
	if err != nil {
		w.log.ErrorContext(jctx, "job complete failed", logx.JobID(job.ID), logx.Err(err))
	} else if !done {
		// Re-enqueued while running (a new gap): it runs again.
		w.log.DebugContext(jctx, "job superseded", logx.JobID(job.ID), slog.Int("version", job.Version))
	}
	return true, nil
}

// Run processes jobs until ctx is cancelled. It polls every PollInterval and
// does not consume the shared wake channel, which belongs to the
// save_message workers.
func (w *LongWorker) Run(ctx context.Context) error {
	return pollLoop(ctx, nil, w.poll, w.RunOnce, w.log)
}
