//go:build integration

package queue_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/a1ekseev/chat-analyze/internal/pgtest"
	"github.com/a1ekseev/chat-analyze/internal/queue"
)

type row struct {
	Status    string
	Attempts  int
	Version   int
	LastError *string
	Payload   string
}

func getRow(t *testing.T, pool *pgxpool.Pool, id int64) row {
	t.Helper()
	var r row
	err := pool.QueryRow(context.Background(),
		`SELECT status, attempts, version, last_error, payload::text FROM jobs WHERE id = $1`, id).
		Scan(&r.Status, &r.Attempts, &r.Version, &r.LastError, &r.Payload)
	require.NoError(t, err)
	return r
}

func TestEnqueue_InsertThenUpdate(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	q := queue.New(pool, 5)

	r1, err := q.Enqueue(ctx, "analyze", "k1", map[string]int{"a": 1})
	require.NoError(t, err)
	require.True(t, r1.Inserted)
	require.Equal(t, 1, r1.Version)

	job, ok, err := q.Claim(ctx, "analyze", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1, job.Attempts)

	r2, err := q.Enqueue(ctx, "analyze", "k1", map[string]int{"a": 2})
	require.NoError(t, err)
	require.False(t, r2.Inserted)
	require.Equal(t, 2, r2.Version)
	require.Equal(t, r1.ID, r2.ID)

	got := getRow(t, pool, r1.ID)
	require.Equal(t, "pending", got.Status)
	require.Equal(t, 0, got.Attempts)
	require.JSONEq(t, `{"a":2}`, got.Payload)
}

func TestClaim_SkipLockedNoDoubleClaim(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	q := queue.New(pool, 5)
	for i := range 20 {
		_, err := q.Enqueue(ctx, "k", fmt.Sprintf("d%d", i), i)
		require.NoError(t, err)
	}
	var mu sync.Mutex
	seen := map[int64]int{}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for {
				job, ok, err := q.Claim(ctx, "k", time.Minute)
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				if !ok {
					return
				}
				mu.Lock()
				seen[job.ID]++
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	require.Len(t, seen, 20)
	for id, n := range seen {
		require.Equal(t, 1, n, "job %d claimed %d times", id, n)
	}
}

func TestClaim_RespectsRunAt(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	q := queue.New(pool, 5)
	_, err := q.Enqueue(ctx, "k", "d", 1)
	require.NoError(t, err)
	job, ok, err := q.Claim(ctx, "k", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	dead, err := q.Fail(ctx, job, time.Hour, errors.New("boom"))
	require.NoError(t, err)
	require.False(t, dead)
	_, ok, err = q.Claim(ctx, "k", time.Minute)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestComplete_StaleVersion(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	q := queue.New(pool, 5)
	r, err := q.Enqueue(ctx, "k", "d", 1)
	require.NoError(t, err)
	job, ok, err := q.Claim(ctx, "k", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1, job.Version)

	_, err = q.Enqueue(ctx, "k", "d", 2)
	require.NoError(t, err)

	done, err := q.Complete(ctx, job.ID, job.Version)
	require.NoError(t, err)
	require.False(t, done)
	require.Equal(t, "pending", getRow(t, pool, r.ID).Status)

	job2, ok, err := q.Claim(ctx, "k", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 2, job2.Version)
	done, err = q.Complete(ctx, job2.ID, job2.Version)
	require.NoError(t, err)
	require.True(t, done)
	require.Equal(t, "done", getRow(t, pool, r.ID).Status)
}

func TestFail_RetryThenDead(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	q := queue.New(pool, 2)
	r, err := q.Enqueue(ctx, "k", "d", 1)
	require.NoError(t, err)

	job, ok, err := q.Claim(ctx, "k", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	dead, err := q.Fail(ctx, job, 0, errors.New("first"))
	require.NoError(t, err)
	require.False(t, dead)
	require.Equal(t, "pending", getRow(t, pool, r.ID).Status)

	job, ok, err = q.Claim(ctx, "k", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	dead, err = q.Fail(ctx, job, 0, errors.New("second"))
	require.NoError(t, err)
	require.True(t, dead)
	got := getRow(t, pool, r.ID)
	require.Equal(t, "dead", got.Status)
	require.NotNil(t, got.LastError)
	require.NotEmpty(t, *got.LastError)
}

func TestFail_PermanentIsDeadImmediately(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	q := queue.New(pool, 5)
	r, err := q.Enqueue(ctx, "k", "d", 1)
	require.NoError(t, err)
	job, ok, err := q.Claim(ctx, "k", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	dead, err := q.Fail(ctx, job, time.Second, fmt.Errorf("bad payload: %w", queue.ErrPermanent))
	require.NoError(t, err)
	require.True(t, dead)
	require.Equal(t, "dead", getRow(t, pool, r.ID).Status)
}

func TestFail_StaleVersionNotDead(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	q := queue.New(pool, 5)
	r, err := q.Enqueue(ctx, "k", "d", 1)
	require.NoError(t, err)
	job, ok, err := q.Claim(ctx, "k", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	_, err = q.Enqueue(ctx, "k", "d", 2)
	require.NoError(t, err)
	dead, err := q.Fail(ctx, job, 0, queue.ErrPermanent)
	require.NoError(t, err)
	require.False(t, dead)
	require.Equal(t, "pending", getRow(t, pool, r.ID).Status)
}

func TestFail_LastErrorSanitized(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	q := queue.New(pool, 5)
	r, err := q.Enqueue(ctx, "k", "d", 1)
	require.NoError(t, err)
	job, ok, err := q.Claim(ctx, "k", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	cause := fmt.Errorf("insert: %w", &pgconn.PgError{Code: "23505", Message: "CANARY", Detail: "CANARY"})
	_, err = q.Fail(ctx, job, 0, cause)
	require.NoError(t, err)
	got := getRow(t, pool, r.ID)
	require.NotNil(t, got.LastError)
	require.NotContains(t, *got.LastError, "CANARY")
}

// Release returns a job interrupted by shutdown to pending without burning
// the attempt.
func TestRelease_RestoresAttempt(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	q := queue.New(pool, 5)
	r, err := q.Enqueue(ctx, "analyze", "k1", map[string]int{"a": 1})
	require.NoError(t, err)
	job, ok, err := q.Claim(ctx, "analyze", time.Hour)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1, job.Attempts)

	// Version guard: a stale version changes nothing.
	ok, err = q.Release(ctx, job.ID, job.Version+1, 0)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, "running", getRow(t, pool, r.ID).Status)

	ok, err = q.Release(ctx, job.ID, job.Version, 0)
	require.NoError(t, err)
	require.True(t, ok)
	got := getRow(t, pool, r.ID)
	require.Equal(t, "pending", got.Status)
	require.Equal(t, 0, got.Attempts)
	require.Equal(t, job.Version, got.Version)
	var unlocked, due bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT locked_until IS NULL, run_at <= now() FROM jobs WHERE id = $1`, r.ID).Scan(&unlocked, &due))
	require.True(t, unlocked)
	require.True(t, due)

	// Not running any more: nothing to release.
	ok, err = q.Release(ctx, job.ID, job.Version, 0)
	require.NoError(t, err)
	require.False(t, ok)

	// Immediately claimable again, with the same attempt number.
	job2, ok, err := q.Claim(ctx, "analyze", time.Hour)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1, job2.Attempts)
}

// Release with a delay (FLOOD_WAIT) keeps the attempt and defers the job,
// even at the last attempt.
func TestRelease_DelayKeepsAttempt(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	q := queue.New(pool, 1)
	r, err := q.Enqueue(ctx, "analyze", "k1", 1)
	require.NoError(t, err)
	job, ok, err := q.Claim(ctx, "analyze", time.Hour)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, job.MaxAttempts, job.Attempts)

	ok, err = q.Release(ctx, job.ID, job.Version, time.Hour)
	require.NoError(t, err)
	require.True(t, ok)
	got := getRow(t, pool, r.ID)
	require.Equal(t, "pending", got.Status)
	require.Equal(t, 0, got.Attempts)
	var wait time.Duration
	require.NoError(t, pool.QueryRow(ctx, `SELECT run_at - now() FROM jobs WHERE id = $1`, r.ID).Scan(&wait))
	require.InDelta(t, time.Hour.Seconds(), wait.Seconds(), 5)
	_, ok, err = q.Claim(ctx, "analyze", time.Hour)
	require.NoError(t, err)
	require.False(t, ok, "not due before the delay")
}

func TestReap_ReturnsExpiredRunning(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	q := queue.New(pool, 5)
	_, err := q.Enqueue(ctx, "k", "d", 1)
	require.NoError(t, err)
	_, ok, err := q.Claim(ctx, "k", time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)
	time.Sleep(20 * time.Millisecond)
	n, err := q.Reap(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
	_, ok, err = q.Claim(ctx, "k", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
}

// expireLease makes a running job's lease look expired without sleeping.
func expireLease(t *testing.T, pool *pgxpool.Pool, id int64) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`UPDATE jobs SET locked_until = now() - interval '1 second' WHERE id = $1`, id)
	require.NoError(t, err)
}

func TestReap_BelowMaxAttemptsPendingAndVersionBumped(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	q := queue.New(pool, 2)
	r, err := q.Enqueue(ctx, "k", "d", 1)
	require.NoError(t, err)
	job, ok, err := q.Claim(ctx, "k", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1, job.Attempts)
	expireLease(t, pool, job.ID)

	n, err := q.Reap(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
	got := getRow(t, pool, r.ID)
	require.Equal(t, "pending", got.Status)
	require.Equal(t, 1, got.Attempts)
	require.Equal(t, job.Version+1, got.Version)
	require.Nil(t, got.LastError)
}

// A job that kills the process on every attempt must end up dead, not loop.
func TestReap_MaxAttemptsGoesDead(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	q := queue.New(pool, 2)
	r, err := q.Enqueue(ctx, "k", "d", 1)
	require.NoError(t, err)
	for attempt := 1; attempt <= 2; attempt++ {
		job, ok, err := q.Claim(ctx, "k", time.Minute)
		require.NoError(t, err)
		require.True(t, ok, "attempt %d", attempt)
		require.Equal(t, attempt, job.Attempts)
		expireLease(t, pool, job.ID)
		n, err := q.Reap(ctx)
		require.NoError(t, err)
		require.EqualValues(t, 1, n)
	}
	got := getRow(t, pool, r.ID)
	require.Equal(t, "dead", got.Status)
	require.Equal(t, 2, got.Attempts)
	require.NotNil(t, got.LastError)
	require.Equal(t, "lease expired", *got.LastError)
	_, ok, err := q.Claim(ctx, "k", time.Minute)
	require.NoError(t, err)
	require.False(t, ok)
}

// After a reap and a re-claim by another worker, the worker whose lease
// expired must not be able to complete, fail or release the job.
func TestReap_StaleWorkerCannotFinish(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	q := queue.New(pool, 5)
	r, err := q.Enqueue(ctx, "k", "d", 1)
	require.NoError(t, err)
	stale, ok, err := q.Claim(ctx, "k", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	expireLease(t, pool, stale.ID)
	_, err = q.Reap(ctx)
	require.NoError(t, err)
	fresh, ok, err := q.Claim(ctx, "k", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	before := getRow(t, pool, r.ID)

	done, err := q.Complete(ctx, stale.ID, stale.Version)
	require.NoError(t, err)
	require.False(t, done)
	dead, err := q.Fail(ctx, stale, 0, queue.ErrPermanent)
	require.NoError(t, err)
	require.False(t, dead)
	ok, err = q.Release(ctx, stale.ID, stale.Version, 0)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, before, getRow(t, pool, r.ID))

	done, err = q.Complete(ctx, fresh.ID, fresh.Version)
	require.NoError(t, err)
	require.True(t, done)
}

func TestCleanup_DeletesOldDone(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	q := queue.New(pool, 5)
	oldR, err := q.Enqueue(ctx, "k", "old", 1)
	require.NoError(t, err)
	newR, err := q.Enqueue(ctx, "k", "new", 1)
	require.NoError(t, err)
	for range 2 {
		job, ok, err := q.Claim(ctx, "k", time.Minute)
		require.NoError(t, err)
		require.True(t, ok)
		done, err := q.Complete(ctx, job.ID, job.Version)
		require.NoError(t, err)
		require.True(t, done)
	}
	_, err = pool.Exec(ctx, `UPDATE jobs SET updated_at = now() - interval '48 hours' WHERE id = $1`, oldR.ID)
	require.NoError(t, err)

	n, err := q.Cleanup(ctx, queue.Retention{Done: 24 * time.Hour, Dead: 1000 * time.Hour})
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
	var cnt int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE id = $1`, newR.ID).Scan(&cnt))
	require.Equal(t, 1, cnt)
}

func TestStats_Counts(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	q := queue.New(pool, 1)

	s, err := q.Stats(ctx)
	require.NoError(t, err)
	require.Equal(t, queue.Stats{}, s)

	for _, k := range []string{"a", "b", "c", "d"} {
		_, err := q.Enqueue(ctx, "k", k, 1)
		require.NoError(t, err)
	}
	// a -> done, b -> dead, c -> running, d -> pending
	for i, want := range []string{"done", "dead", "running"} {
		job, ok, err := q.Claim(ctx, "k", time.Minute)
		require.NoError(t, err)
		require.True(t, ok)
		switch want {
		case "done":
			_, err = q.Complete(ctx, job.ID, job.Version)
		case "dead":
			_, err = q.Fail(ctx, job, 0, errors.New("x"))
		}
		require.NoError(t, err, "step %d", i)
	}
	time.Sleep(20 * time.Millisecond)
	s, err = q.Stats(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, s.Pending)
	require.EqualValues(t, 1, s.Running)
	require.EqualValues(t, 1, s.Dead)
	require.EqualValues(t, 1, s.Done24h)
	require.Greater(t, s.OldestPendingAge, time.Duration(0))
}

// TestEnqueue_WhileRunningAllowsSecondClaim documents that re-enqueueing a
// running job makes it claimable again at once, so with WORKERS>1 two
// versions of one message may be processed concurrently. This is safe: only
// the newest version's Complete succeeds, and messages.Upsert orders
// versions by updated_at (an older version never overwrites a newer one,
// see message.TestUpsert_OriginalAfterEditDoesNotOverwrite).
func TestEnqueue_WhileRunningAllowsSecondClaim(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	q := queue.New(pool, 5)

	_, err := q.Enqueue(ctx, "k", "msg", map[string]string{"v": "1"})
	require.NoError(t, err)
	first, ok, err := q.Claim(ctx, "k", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)

	_, err = q.Enqueue(ctx, "k", "msg", map[string]string{"v": "2"})
	require.NoError(t, err)
	second, ok, err := q.Claim(ctx, "k", time.Minute)
	require.NoError(t, err)
	require.True(t, ok, "re-enqueued running job is claimable again")
	require.Equal(t, first.ID, second.ID)
	require.Equal(t, first.Version+1, second.Version)
	require.JSONEq(t, `{"v":"2"}`, string(second.Payload))

	done, err := q.Complete(ctx, first.ID, first.Version)
	require.NoError(t, err)
	require.False(t, done, "the stale version cannot complete")
	done, err = q.Complete(ctx, second.ID, second.Version)
	require.NoError(t, err)
	require.True(t, done)
	require.Equal(t, "done", getRow(t, pool, first.ID).Status)
}

type minPayload struct {
	ChannelID int64 `json:"channel_id"`
	MinID     int   `json:"min_id"`
}

func TestEnqueueKeepMin(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	q := queue.New(pool, 5)
	enq := func(min int) queue.EnqueueResult {
		t.Helper()
		r, err := q.EnqueueKeepMin(ctx, "bf", "5", minPayload{ChannelID: 5, MinID: min}, "min_id")
		require.NoError(t, err)
		return r
	}

	r1 := enq(10)
	require.True(t, r1.Inserted)
	require.JSONEq(t, `{"channel_id":5,"min_id":10}`, getRow(t, pool, r1.ID).Payload)

	// Pending: a later TooLong never moves min_id forward, an earlier one does.
	r2 := enq(50)
	require.False(t, r2.Inserted)
	require.Equal(t, 2, r2.Version)
	require.JSONEq(t, `{"channel_id":5,"min_id":10}`, getRow(t, pool, r1.ID).Payload)
	enq(7)
	require.JSONEq(t, `{"channel_id":5,"min_id":7}`, getRow(t, pool, r1.ID).Payload)

	// Running: re-armed to pending, min_id kept.
	job, ok, err := q.Claim(ctx, "bf", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	enq(80)
	got := getRow(t, pool, job.ID)
	require.Equal(t, "pending", got.Status)
	require.JSONEq(t, `{"channel_id":5,"min_id":7}`, got.Payload)

	// Done: the gap was fetched, a new TooLong starts from the new min_id.
	job, ok, err = q.Claim(ctx, "bf", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	done, err := q.Complete(ctx, job.ID, job.Version)
	require.NoError(t, err)
	require.True(t, done)
	enq(90)
	got = getRow(t, pool, job.ID)
	require.Equal(t, "pending", got.Status)
	require.Equal(t, 0, got.Attempts)
	require.JSONEq(t, `{"channel_id":5,"min_id":90}`, got.Payload)
}

// min_id 0 means "no history": it must not win over a real lower bound.
func TestEnqueueKeepMin_ZeroIsNoBound(t *testing.T) {
	for _, tc := range []struct {
		name        string
		first, then int
		want        int
	}{
		{"zero then real", 0, 500, 500},
		{"real then zero", 500, 0, 500},
		{"real then smaller", 500, 200, 200},
		{"zero then zero", 0, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			pool := pgtest.New(t)
			q := queue.New(pool, 5)
			r, err := q.EnqueueKeepMin(ctx, "bf", "5", minPayload{ChannelID: 5, MinID: tc.first}, "min_id")
			require.NoError(t, err)
			_, err = q.EnqueueKeepMin(ctx, "bf", "5", minPayload{ChannelID: 5, MinID: tc.then}, "min_id")
			require.NoError(t, err)
			require.JSONEq(t, fmt.Sprintf(`{"channel_id":5,"min_id":%d}`, tc.want), getRow(t, pool, r.ID).Payload)
		})
	}
}

func TestEnqueueKeepMin_PendingKeepsBackoff(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	q := queue.New(pool, 5)
	r, err := q.EnqueueKeepMin(ctx, "bf", "5", minPayload{ChannelID: 5, MinID: 10}, "min_id")
	require.NoError(t, err)
	job, ok, err := q.Claim(ctx, "bf", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	_, err = q.Fail(ctx, job, time.Hour, errors.New("FLOOD_WAIT"))
	require.NoError(t, err)

	_, err = q.EnqueueKeepMin(ctx, "bf", "5", minPayload{ChannelID: 5, MinID: 20}, "min_id")
	require.NoError(t, err)
	var future bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT run_at > now() + interval '50 minutes' FROM jobs WHERE id = $1`, r.ID).Scan(&future))
	require.True(t, future, "re-enqueue must not bypass a pending retry delay")
	_, ok, err = q.Claim(ctx, "bf", time.Minute)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestCleanup_DeadJobsAfterDeadRetention(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	q := queue.New(pool, 1)
	ids := map[string]int64{}
	for _, k := range []string{"old-dead", "fresh-dead", "mid-done", "old-pending"} {
		r, err := q.Enqueue(ctx, "k", k, 1)
		require.NoError(t, err)
		ids[k] = r.ID
	}
	set := func(id int64, status, age string) {
		_, err := pool.Exec(ctx, `UPDATE jobs SET status = $2, updated_at = now() - $3::interval WHERE id = $1`, id, status, age)
		require.NoError(t, err)
	}
	set(ids["old-dead"], "dead", "31 days")
	set(ids["fresh-dead"], "dead", "29 days")
	set(ids["mid-done"], "done", "8 days")
	set(ids["old-pending"], "pending", "400 days")

	n, err := q.Cleanup(ctx, queue.Retention{Done: 7 * 24 * time.Hour, Dead: 30 * 24 * time.Hour})
	require.NoError(t, err)
	require.EqualValues(t, 2, n)
	var left []string
	rows, err := pool.Query(ctx, `SELECT dedup_key FROM jobs ORDER BY dedup_key`)
	require.NoError(t, err)
	for rows.Next() {
		var k string
		require.NoError(t, rows.Scan(&k))
		left = append(left, k)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{"fresh-dead", "old-pending"}, left)
}
