// Package worker processes queue jobs: save_message (store the message,
// upsert forum topic titles, optionally mark it read in Telegram) and
// backfill_channel (fetch a channel gap and enqueue its messages).
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/gotd/td/tgerr"

	"github.com/a1ekseev/chat-analyze/internal/jobs"
	"github.com/a1ekseev/chat-analyze/internal/logx"
	"github.com/a1ekseev/chat-analyze/internal/message"
	"github.com/a1ekseev/chat-analyze/internal/queue"
)

// MessageStore persists messages and forum topics.
type MessageStore interface {
	Upsert(ctx context.Context, m message.Message) error
	UpsertTopic(ctx context.Context, chatID int64, topicID int, title string) error
}

// ReadMarker records messages to be marked read in Telegram.
type ReadMarker interface {
	MarkRead(ctx context.Context, chatID int64, t jobs.ReadTarget, maxID int) error
}

// Queue is the subset of queue.Queue the worker needs.
type Queue interface {
	Claim(ctx context.Context, kind string, lease time.Duration) (queue.Job, bool, error)
	Complete(ctx context.Context, id int64, version int) (bool, error)
	Fail(ctx context.Context, job queue.Job, retryIn time.Duration, cause error) (bool, error)
	Release(ctx context.Context, id int64, version int, delay time.Duration) (bool, error)
}

// finishTimeout bounds Complete/Fail after Handle returned.
const finishTimeout = 10 * time.Second

// Config tunes the worker.
type Config struct {
	MarkRead     bool
	Lease        time.Duration
	PollInterval time.Duration
}

// Worker handles save_message jobs.
type Worker struct {
	q      Queue
	store  MessageStore
	reader ReadMarker
	cfg    Config
	log    *slog.Logger
}

// New creates a Worker.
func New(q Queue, store MessageStore, reader ReadMarker, cfg Config, logger *slog.Logger) *Worker {
	return &Worker{
		q: q, store: store, reader: reader, cfg: cfg,
		log: logger.With(logx.Component("worker")),
	}
}

// Handle processes one save_message job. Undecodable payloads yield an error
// wrapping queue.ErrPermanent; panics are recovered into errors.
func (w *Worker) Handle(ctx context.Context, job queue.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			w.log.ErrorContext(ctx, "panic recovered",
				logx.JobID(job.ID),
				slog.String("panic", fmt.Sprintf("%T", r)),
				slog.String("stack", string(debug.Stack())))
			err = fmt.Errorf("job %d: panic: %T", job.ID, r)
		}
	}()

	var p jobs.SaveMessage
	// The decode error is dropped on purpose: json errors can quote payload bytes.
	if uerr := json.Unmarshal(job.Payload, &p); uerr != nil {
		return fmt.Errorf("decode payload: %w", queue.ErrPermanent)
	}
	m := p.Message
	if err := w.store.Upsert(ctx, m); err != nil {
		return fmt.Errorf("upsert message %d:%d: %w", m.ChatID, m.MessageID, err)
	}
	if m.TopicTitle != nil && m.TopicID != nil {
		if err := w.store.UpsertTopic(ctx, m.ChatID, *m.TopicID, *m.TopicTitle); err != nil {
			return fmt.Errorf("upsert topic %d:%d: %w", m.ChatID, *m.TopicID, err)
		}
	}
	if w.cfg.MarkRead {
		if err := w.reader.MarkRead(ctx, m.ChatID, p.Read, m.MessageID); err != nil {
			return fmt.Errorf("mark read %d:%d: %w", m.ChatID, m.MessageID, err)
		}
	}
	return nil
}

// RunOnce claims one job and processes it. It reports whether a job existed.
// Once a job is claimed it is finished with a context that is not cancelled
// (bounded by Lease) so shutdown does not abandon it midway. If it still
// fails after shutdown began (for example the Telegram client or the pool
// is closing) it is released, not failed, so shutdown never burns an attempt;
// a lease timeout is always a failure.
func (w *Worker) RunOnce(ctx context.Context) (bool, error) {
	job, ok, err := w.q.Claim(ctx, jobs.KindSaveMessage, w.cfg.Lease)
	if err != nil {
		return false, fmt.Errorf("claim: %w", err)
	}
	if !ok {
		return false, nil
	}
	base := context.WithoutCancel(ctx)
	hctx, hcancel := context.WithTimeout(base, w.cfg.Lease)
	start := time.Now()
	herr := w.Handle(hctx, job)
	shutdown := interruptedByShutdown(ctx, hctx)
	hcancel()

	// Bookkeeping gets its own context: Handle may have used up the lease.
	jctx, cancel := context.WithTimeout(base, finishTimeout)
	defer cancel()
	if herr == nil {
		w.complete(jctx, job, time.Since(start))
		return true, nil
	}

	if shutdown {
		return true, releaseJob(jctx, w.q, w.log, job, 0)
	}
	var chatID *int64
	var p jobs.SaveMessage
	if json.Unmarshal(job.Payload, &p) == nil {
		chatID = &p.Message.ChatID
	}
	return true, failJob(jctx, w.q, w.log, job, herr, chatID)
}

// interruptedByShutdown reports whether a failed handler should be released
// instead of failed: the run context (parent) was cancelled, and the handler
// context did not run out of its lease. Check it before the handler context
// is cancelled.
func interruptedByShutdown(parent, hctx context.Context) bool {
	return parent.Err() != nil && !errors.Is(hctx.Err(), context.DeadlineExceeded)
}

// releaseJob returns a job interrupted by shutdown (delay 0) or by a
// FLOOD_WAIT to the queue without burning the attempt.
func releaseJob(ctx context.Context, q Queue, log *slog.Logger, job queue.Job, delay time.Duration) error {
	ok, err := q.Release(ctx, job.ID, job.Version, delay)
	if err != nil {
		return fmt.Errorf("release job %d: %w", job.ID, err)
	}
	if !ok {
		log.DebugContext(ctx, "job superseded", logx.JobID(job.ID), slog.Int("version", job.Version))
		return nil
	}
	log.DebugContext(ctx, "job released", logx.JobID(job.ID), slog.String("job_kind", job.Kind),
		logx.Attempt(job.Attempts), slog.Int64("retry_in_ms", delay.Milliseconds()))
	return nil
}

// failJob records a failed attempt retried after queue.Backoff. A
// FLOOD_WAIT_X is not the job's fault: the job is released and runs again
// after X+1s without counting the attempt. chatID (optional) is logged with
// flood waits.
func failJob(ctx context.Context, q Queue, log *slog.Logger, job queue.Job, herr error, chatID *int64) error {
	retryIn := queue.Backoff(job.Attempts)
	if d, isFlood := tgerr.AsFloodWait(herr); isFlood {
		attrs := []slog.Attr{logx.JobID(job.ID)}
		if chatID != nil {
			attrs = append(attrs, logx.ChatID(*chatID))
		}
		attrs = append(attrs, slog.Int64("wait_s", int64(d/time.Second)))
		log.LogAttrs(ctx, slog.LevelWarn, "flood wait", attrs...)
		return releaseJob(ctx, q, log, job, d+time.Second)
	}
	dead, ferr := q.Fail(ctx, job, retryIn, herr)
	if ferr != nil {
		return fmt.Errorf("fail job %d: %w", job.ID, ferr)
	}
	if dead {
		log.ErrorContext(ctx, "job dead", logx.JobID(job.ID),
			slog.String("job_kind", job.Kind), slog.Int("attempts", job.Attempts), logx.Err(herr))
	} else {
		log.WarnContext(ctx, "job failed, will retry", logx.JobID(job.ID),
			logx.Attempt(job.Attempts), slog.Int("max_attempts", job.MaxAttempts),
			slog.Int64("retry_in_ms", retryIn.Milliseconds()), logx.Err(herr))
	}
	return nil
}

func (w *Worker) complete(ctx context.Context, job queue.Job, took time.Duration) {
	done, err := w.q.Complete(ctx, job.ID, job.Version)
	if err != nil {
		// The job stays running and is re-claimed after its lease expires.
		w.log.ErrorContext(ctx, "job complete failed", logx.JobID(job.ID), logx.Err(err))
		return
	}
	if !done {
		w.log.DebugContext(ctx, "job superseded", logx.JobID(job.ID), slog.Int("version", job.Version))
		return
	}
	attrs := []slog.Attr{logx.JobID(job.ID)}
	var p jobs.SaveMessage
	if json.Unmarshal(job.Payload, &p) == nil {
		attrs = append(attrs, p.Message.LogValue().Group()...)
	}
	attrs = append(attrs, logx.Attempt(job.Attempts), logx.Duration(took))
	w.log.LogAttrs(ctx, slog.LevelInfo, "message stored", attrs...)
}

// Run processes jobs until ctx is cancelled, then returns nil after the
// in-flight job (if any) has finished. wake may be nil.
func (w *Worker) Run(ctx context.Context, wake <-chan struct{}) error {
	return pollLoop(ctx, wake, w.cfg.PollInterval, w.RunOnce, w.log)
}

// pollLoop calls runOnce until ctx is cancelled; when no job was due it waits
// for wake (nil: never), the poll ticker or ctx.
func pollLoop(ctx context.Context, wake <-chan struct{}, poll time.Duration,
	runOnce func(context.Context) (bool, error), log *slog.Logger,
) error {
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for ctx.Err() == nil {
		did, err := runOnce(ctx)
		if err != nil {
			if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
				return nil
			}
			log.ErrorContext(ctx, "worker iteration failed", logx.Err(err))
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(poll):
			}
			continue
		}
		if did {
			continue
		}
		select {
		case _, ok := <-wake:
			if !ok {
				wake = nil // closed: rely on the ticker, do not spin
			}
		case <-ticker.C:
		case <-ctx.Done():
		}
	}
	return nil
}
