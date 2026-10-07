package queue

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/a1ekseev/chat-analyze/internal/logx"
)

// NotifyChannel is the Postgres channel used for job wakeups.
const NotifyChannel = "jobs"

const reconnectPause = time.Second

// Notifier turns Postgres NOTIFY on NotifyChannel into wake signals.
type Notifier struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
	c      chan struct{}
}

// NewNotifier creates a Notifier.
func NewNotifier(pool *pgxpool.Pool, logger *slog.Logger) *Notifier {
	return &Notifier{pool: pool, logger: logger, c: make(chan struct{}, 1)}
}

// C delivers coalesced wake signals (buffer 1, non-blocking send).
func (n *Notifier) C() <-chan struct{} { return n.c }

// Run listens until ctx is cancelled (returns nil), reconnecting with a
// pause after connection errors.
func (n *Notifier) Run(ctx context.Context) error {
	for {
		err := n.listen(ctx)
		if ctx.Err() != nil {
			return nil //nolint:nilerr // cancellation is the normal way to stop
		}
		n.logger.WarnContext(ctx, "queue notifier connection lost, reconnecting",
			logx.Component("queue"), logx.Err(err))
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(reconnectPause):
		}
	}
}

func (n *Notifier) listen(ctx context.Context) error {
	conn, err := n.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire: %w", err)
	}
	defer releaseClean(conn) //nolint:contextcheck // cleanup must outlive the cancelled ctx
	if _, err := conn.Exec(ctx, "LISTEN "+NotifyChannel); err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	// A notification may have been missed while disconnected.
	n.wake()
	for {
		if _, err := conn.Conn().WaitForNotification(ctx); err != nil {
			return fmt.Errorf("wait: %w", err)
		}
		n.wake()
	}
}

// releaseClean never returns a connection with an active LISTEN to the pool:
// it UNLISTENs, or destroys the connection if that fails.
func releaseClean(conn *pgxpool.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, "UNLISTEN *"); err != nil {
		_ = conn.Hijack().Close(ctx)
		return
	}
	conn.Release()
}

func (n *Notifier) wake() {
	select {
	case n.c <- struct{}{}:
	default:
	}
}
