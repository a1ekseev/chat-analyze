package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/gotd/td/constant"

	"github.com/a1ekseev/chat-analyze/internal/jobs"
	"github.com/a1ekseev/chat-analyze/internal/logx"
	"github.com/a1ekseev/chat-analyze/internal/queue"
)

// BackfillLease is the minimum lease of a backfill_channel job: up to 1000
// history pages 1s apart (~17m plus RTT, ~25m at 0.5s RTT), plus up to 30
// minutes of in-job FLOOD_WAIT waiting (telegram.Listener.Backfill), takes
// far longer than one save_message job.
const BackfillLease = 60 * time.Minute

// Backfiller fetches a channel gap and enqueues its messages
// (telegram.Listener.Backfill).
type Backfiller interface {
	Backfill(ctx context.Context, channelID int64, minID int) error
}

// BackfillWorker processes backfill_channel jobs, one at a time.
type BackfillWorker struct {
	q     Queue
	b     Backfiller
	lease time.Duration
	poll  time.Duration
	log   *slog.Logger
}

// NewBackfill creates a BackfillWorker. Its lease is max(cfg.Lease,
// BackfillLease); MarkRead is not used.
func NewBackfill(q Queue, b Backfiller, cfg Config, logger *slog.Logger) *BackfillWorker {
	return &BackfillWorker{
		q: q, b: b, lease: max(cfg.Lease, BackfillLease), poll: cfg.PollInterval,
		log: logger.With(logx.Component("worker")),
	}
}

// handle runs one backfill job; panics are recovered into errors.
func (w *BackfillWorker) handle(ctx context.Context, job queue.Job, p jobs.BackfillChannel) (err error) {
	defer func() {
		if r := recover(); r != nil {
			w.log.ErrorContext(ctx, "panic recovered", logx.JobID(job.ID),
				slog.String("panic", fmt.Sprintf("%T", r)))
			err = fmt.Errorf("job %d: panic: %T", job.ID, r)
		}
	}()
	if err := w.b.Backfill(ctx, p.ChannelID, p.MinID); err != nil {
		return fmt.Errorf("backfill channel %d: %w", p.ChannelID, err)
	}
	return nil
}

// RunOnce claims one backfill_channel job and processes it; it reports
// whether a job existed. Unlike save_message the fetch is interrupted by
// shutdown (ctx): the job is then released (queue.Release, the attempt is
// not counted) with a live context and runs again after the restart with the
// same min_id, so nothing is lost. Errors, lease timeouts and FLOOD_WAITs
// longer than the in-job cap follow the save_message retry rules (a
// FLOOD_WAIT does not count the attempt).
func (w *BackfillWorker) RunOnce(ctx context.Context) (bool, error) {
	job, ok, err := w.q.Claim(ctx, jobs.KindBackfillChannel, w.lease)
	if err != nil {
		return false, fmt.Errorf("claim: %w", err)
	}
	if !ok {
		return false, nil
	}
	var (
		p        jobs.BackfillChannel
		herr     error
		chatID   *int64
		shutdown bool
	)
	// The decode error is dropped on purpose: json errors can quote payload bytes.
	if uerr := json.Unmarshal(job.Payload, &p); uerr != nil || p.ChannelID <= 0 {
		herr = fmt.Errorf("decode payload: %w", queue.ErrPermanent)
	} else {
		var marked constant.TDLibPeerID
		marked.Channel(p.ChannelID)
		chatID = new(int64(marked))
		hctx, hcancel := context.WithTimeout(ctx, w.lease)
		herr = w.handle(hctx, job, p)
		shutdown = herr != nil && interruptedByShutdown(ctx, hctx)
		hcancel()
	}

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
		// Re-enqueued by a new ChannelTooLong while running: it runs again.
		w.log.DebugContext(jctx, "job superseded", logx.JobID(job.ID), slog.Int("version", job.Version))
	}
	return true, nil
}

// Run processes backfill jobs until ctx is cancelled. It polls every
// PollInterval and does not consume the shared wake channel, which belongs
// to the save_message workers.
func (w *BackfillWorker) Run(ctx context.Context) error {
	return pollLoop(ctx, nil, w.poll, w.RunOnce, w.log)
}
