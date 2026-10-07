// Package queue is a PostgreSQL-backed job queue (SKIP LOCKED claims,
// version-guarded completion, LISTEN/NOTIFY wakeups).
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/a1ekseev/chat-analyze/internal/logx"
)

// ErrPermanent marks a failure that must not be retried.
var ErrPermanent = errors.New("permanent job failure")

// Job is a claimed unit of work.
type Job struct {
	ID          int64
	Kind        string
	Version     int
	Attempts    int
	MaxAttempts int
	Payload     json.RawMessage
}

// EnqueueResult describes the outcome of Enqueue.
type EnqueueResult struct {
	ID       int64
	Version  int
	Inserted bool
}

// Stats is a snapshot of queue state.
type Stats struct {
	Pending, Running, Dead, Done24h int64
	OldestPendingAge                time.Duration
}

// Queue is the job queue. Interval parameters are passed as time.Duration,
// which pgx encodes to the interval type.
type Queue struct {
	pool        *pgxpool.Pool
	maxAttempts int
}

// New creates a Queue; maxAttempts is stored on each enqueued job.
func New(pool *pgxpool.Pool, maxAttempts int) *Queue {
	return &Queue{pool: pool, maxAttempts: maxAttempts}
}

const enqueueSQL = `
INSERT INTO jobs (kind, dedup_key, payload, max_attempts) VALUES ($1, $2, $3, $4)
ON CONFLICT (kind, dedup_key) DO UPDATE SET
  payload = EXCLUDED.payload, status = 'pending', attempts = 0, run_at = now(),
  last_error = NULL, version = jobs.version + 1, updated_at = now()
RETURNING id, version, (xmax = 0) AS inserted`

// Enqueue inserts or re-arms the job (kind, dedupKey) and notifies listeners,
// in one transaction.
func (q *Queue) Enqueue(ctx context.Context, kind, dedupKey string, payload any) (EnqueueResult, error) {
	return q.enqueue(ctx, enqueueSQL, kind, dedupKey, payload)
}

const enqueueKeepMinSQL = `
INSERT INTO jobs (kind, dedup_key, payload, max_attempts) VALUES ($1, $2, $3, $4)
ON CONFLICT (kind, dedup_key) DO UPDATE SET
  payload = CASE
    WHEN jobs.status = 'done' THEN EXCLUDED.payload
    ELSE jsonb_set(EXCLUDED.payload, ARRAY[$5::text], to_jsonb(CASE
      WHEN (jobs.payload ->> $5::text)::bigint = 0 THEN (EXCLUDED.payload ->> $5::text)::bigint
      WHEN (EXCLUDED.payload ->> $5::text)::bigint = 0 THEN (jobs.payload ->> $5::text)::bigint
      ELSE LEAST((jobs.payload ->> $5::text)::bigint, (EXCLUDED.payload ->> $5::text)::bigint) END))
    END,
  status = 'pending', attempts = 0,
  run_at = CASE WHEN jobs.status = 'pending' THEN jobs.run_at ELSE now() END,
  last_error = NULL, version = jobs.version + 1, updated_at = now()
RETURNING id, version, (xmax = 0) AS inserted`

// EnqueueKeepMin is Enqueue for jobs whose payload carries a numeric lower
// bound in field (for example a backfill's min_id). When the existing job is
// not done yet, the smaller of the old and the new value is kept, so
// re-enqueueing never narrows the pending work. 0 means "no lower bound
// known" (fetch one page) and never wins over a real value: 0 merged with
// 500 is 500 either way. A pending job also keeps its
// run_at so a retry delay (FLOOD_WAIT) is not bypassed. A done job is re-armed
// with the new payload as is.
func (q *Queue) EnqueueKeepMin(ctx context.Context, kind, dedupKey string, payload any, field string) (EnqueueResult, error) {
	return q.enqueue(ctx, enqueueKeepMinSQL, kind, dedupKey, payload, field)
}

func (q *Queue) enqueue(ctx context.Context, sql, kind, dedupKey string, payload any, extra ...any) (EnqueueResult, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return EnqueueResult{}, fmt.Errorf("enqueue marshal payload: %w", err)
	}
	args := append([]any{kind, dedupKey, raw, q.maxAttempts}, extra...)
	var res EnqueueResult
	err = pgx.BeginFunc(ctx, q.pool, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, sql, args...).Scan(&res.ID, &res.Version, &res.Inserted); err != nil {
			return fmt.Errorf("insert: %w", err)
		}
		if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, NotifyChannel, kind); err != nil {
			return fmt.Errorf("notify: %w", err)
		}
		return nil
	})
	if err != nil {
		return EnqueueResult{}, fmt.Errorf("enqueue: %w", err)
	}
	return res, nil
}

const claimSQL = `
UPDATE jobs SET status = 'running', attempts = attempts + 1,
       locked_until = now() + $2::interval, updated_at = now()
WHERE id = (SELECT id FROM jobs WHERE kind = $1 AND status = 'pending' AND run_at <= now()
            ORDER BY run_at, id FOR UPDATE SKIP LOCKED LIMIT 1)
RETURNING id, version, attempts, max_attempts, payload`

// Claim leases the next due job of kind; ok is false when none is available.
func (q *Queue) Claim(ctx context.Context, kind string, lease time.Duration) (Job, bool, error) {
	j := Job{Kind: kind}
	var payload []byte
	err := q.pool.QueryRow(ctx, claimSQL, kind, lease).
		Scan(&j.ID, &j.Version, &j.Attempts, &j.MaxAttempts, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, false, nil
	}
	if err != nil {
		return Job{}, false, fmt.Errorf("claim: %w", err)
	}
	j.Payload = json.RawMessage(payload)
	return j, true, nil
}

const completeSQL = `
UPDATE jobs SET status = 'done', locked_until = NULL, updated_at = now()
WHERE id = $1 AND version = $2 AND status = 'running'`

// Complete marks the job done. false means it was superseded by a newer
// Enqueue (or is no longer running) and nothing was changed.
func (q *Queue) Complete(ctx context.Context, id int64, version int) (bool, error) {
	tag, err := q.pool.Exec(ctx, completeSQL, id, version)
	if err != nil {
		return false, fmt.Errorf("complete job %d: %w", id, err)
	}
	return tag.RowsAffected() == 1, nil
}

const failSQL = `
UPDATE jobs SET status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'pending' END,
       run_at = now() + $3::interval, last_error = $4, locked_until = NULL, updated_at = now()
WHERE id = $1 AND version = $2 AND status = 'running'
RETURNING status = 'dead'`

const failPermanentSQL = `
UPDATE jobs SET status = 'dead', last_error = $3, locked_until = NULL, updated_at = now()
WHERE id = $1 AND version = $2 AND status = 'running'
RETURNING status = 'dead'`

// Fail records a failure. ErrPermanent in cause kills the job immediately;
// otherwise it is retried after retryIn until attempts reach max_attempts.
// dead is false when the job was superseded (version guard matched nothing).
func (q *Queue) Fail(ctx context.Context, job Job, retryIn time.Duration, cause error) (dead bool, err error) {
	lastErr := logx.SanitizeError(cause)
	if errors.Is(cause, ErrPermanent) {
		err = q.pool.QueryRow(ctx, failPermanentSQL, job.ID, job.Version, lastErr).Scan(&dead)
	} else {
		err = q.pool.QueryRow(ctx, failSQL, job.ID, job.Version, retryIn, lastErr).Scan(&dead)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("fail job %d: %w", job.ID, err)
	}
	return dead, nil
}

const releaseSQL = `
UPDATE jobs SET status = 'pending', attempts = GREATEST(attempts - 1, 0), locked_until = NULL,
       run_at = now(), updated_at = now()
WHERE id = $1 AND version = $2 AND status = 'running'`

// Release returns a running job to pending without counting the attempt; it
// is used when the handler was interrupted by shutdown. false means the job
// was superseded or is no longer running and nothing was changed.
func (q *Queue) Release(ctx context.Context, id int64, version int) (bool, error) {
	tag, err := q.pool.Exec(ctx, releaseSQL, id, version)
	if err != nil {
		return false, fmt.Errorf("release job %d: %w", id, err)
	}
	return tag.RowsAffected() == 1, nil
}

const reapSQL = `
UPDATE jobs SET status = 'pending', locked_until = NULL, updated_at = now()
WHERE status = 'running' AND locked_until < now()`

// Reap returns running jobs whose lease expired to pending.
func (q *Queue) Reap(ctx context.Context) (int64, error) {
	tag, err := q.pool.Exec(ctx, reapSQL)
	if err != nil {
		return 0, fmt.Errorf("reap: %w", err)
	}
	return tag.RowsAffected(), nil
}

const cleanupSQL = `
DELETE FROM jobs
WHERE (status = 'done' AND updated_at < now() - $1::interval)
   OR (status = 'dead' AND updated_at < now() - $2::interval)`

// Retention says how long finished jobs are kept.
type Retention struct {
	Done time.Duration // JOBS_RETENTION
	Dead time.Duration // JOBS_DEAD_RETENTION: dead payloads hold message text
}

// Cleanup deletes done and dead jobs older than their retention.
func (q *Queue) Cleanup(ctx context.Context, r Retention) (int64, error) {
	tag, err := q.pool.Exec(ctx, cleanupSQL, r.Done, r.Dead)
	if err != nil {
		return 0, fmt.Errorf("cleanup: %w", err)
	}
	return tag.RowsAffected(), nil
}

const statsSQL = `
SELECT count(*) FILTER (WHERE status = 'pending'),
       count(*) FILTER (WHERE status = 'running'),
       count(*) FILTER (WHERE status = 'dead'),
       count(*) FILTER (WHERE status = 'done' AND updated_at > now() - interval '24 hours'),
       COALESCE(now() - min(created_at) FILTER (WHERE status = 'pending'), interval '0')
FROM jobs`

// Stats returns queue counters.
func (q *Queue) Stats(ctx context.Context) (Stats, error) {
	var s Stats
	if err := q.pool.QueryRow(ctx, statsSQL).
		Scan(&s.Pending, &s.Running, &s.Dead, &s.Done24h, &s.OldestPendingAge); err != nil {
		return Stats{}, fmt.Errorf("stats: %w", err)
	}
	return s, nil
}
