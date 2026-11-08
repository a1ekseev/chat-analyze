package telegram

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/a1ekseev/chat-analyze/internal/jobs"
	"github.com/a1ekseev/chat-analyze/internal/logx"
)

// MarkStore remembers the highest max_id already marked read per (chat, thread).
type MarkStore interface {
	Get(ctx context.Context, chatID int64, threadID int) (int, bool, error)
	Set(ctx context.Context, chatID int64, threadID, maxID int) error
}

// ReadMarks is the PostgreSQL MarkStore over tg_read_marks.
type ReadMarks struct{ pool *pgxpool.Pool }

var _ MarkStore = (*ReadMarks)(nil)

// NewReadMarks returns a MarkStore backed by pool.
func NewReadMarks(pool *pgxpool.Pool) *ReadMarks { return &ReadMarks{pool: pool} }

const sqlGetReadMark = `SELECT max_id FROM tg_read_marks WHERE chat_id = $1 AND thread_id = $2`

// Get returns the stored max_id; ok is false when there is no mark.
func (r *ReadMarks) Get(ctx context.Context, chatID int64, threadID int) (int, bool, error) {
	var maxID int
	err := r.pool.QueryRow(ctx, sqlGetReadMark, chatID, threadID).Scan(&maxID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("get read mark chat %d thread %d: %w", chatID, threadID, err)
	}
	return maxID, true, nil
}

const sqlSetReadMark = `
INSERT INTO tg_read_marks (chat_id, thread_id, max_id, updated_at) VALUES ($1, $2, $3, now())
ON CONFLICT (chat_id, thread_id) DO UPDATE
SET max_id = GREATEST(tg_read_marks.max_id, EXCLUDED.max_id), updated_at = now()`

// Set records maxID, never lowering an existing mark.
func (r *ReadMarks) Set(ctx context.Context, chatID int64, threadID, maxID int) error {
	if _, err := r.pool.Exec(ctx, sqlSetReadMark, chatID, threadID, maxID); err != nil {
		return fmt.Errorf("set read mark chat %d thread %d: %w", chatID, threadID, err)
	}
	return nil
}

// ReadFlushInterval is how often Reader.Run sends the pending read marks.
const ReadFlushInterval = 3 * time.Second

type markKey struct {
	chatID   int64
	threadID int
}

type pendingMark struct {
	t     jobs.ReadTarget
	maxID int
}

// Reader marks Telegram history as read. MarkRead only records the highest
// message id per (chat, thread); Flush sends at most one read RPC per
// changed (chat, thread), so a burst or a backfill costs one RPC per chat,
// not one per message.
type Reader struct {
	api    API
	marks  MarkStore
	logger *slog.Logger

	mu      sync.Mutex
	pending map[markKey]pendingMark
}

// NewReader returns a Reader.
func NewReader(api API, marks MarkStore, logger *slog.Logger) *Reader {
	return &Reader{
		api: api, marks: marks, logger: logger.With(logx.Component("reader")),
		pending: map[markKey]pendingMark{},
	}
}

func (r *Reader) skip(ctx context.Context, chatID int64, reason string) {
	r.logger.DebugContext(ctx, "mark read skipped", logx.ChatID(chatID), slog.String("reason", reason))
}

// MarkRead records that messages up to maxID in the chat described by t
// (spec §6) are to be marked read by the next Flush. Only an unknown peer
// kind is an error.
func (r *Reader) MarkRead(ctx context.Context, chatID int64, t jobs.ReadTarget, maxID int) error {
	switch t.PeerKind {
	case jobs.PeerUser, jobs.PeerSelf, jobs.PeerChat, jobs.PeerChannel:
	default:
		return fmt.Errorf("unknown peer kind %q", t.PeerKind)
	}
	if t.IsMin {
		r.skip(ctx, chatID, "min_peer")
		return nil
	}
	if (t.PeerKind == jobs.PeerUser || t.PeerKind == jobs.PeerChannel) && t.AccessHash == 0 {
		r.skip(ctx, chatID, "no_access_hash")
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.add(markKey{chatID, 0}, pendingMark{t, maxID})
	if t.PeerKind == jobs.PeerChannel && t.ThreadID > 0 {
		r.add(markKey{chatID, t.ThreadID}, pendingMark{t, maxID})
	}
	return nil
}

// add keeps the highest pending maxID of k; r.mu must be held.
func (r *Reader) add(k markKey, p pendingMark) {
	if cur, ok := r.pending[k]; !ok || p.maxID > cur.maxID {
		r.pending[k] = p
	}
}

// Run calls Flush every interval until ctx is cancelled. It does not flush
// on return: the shutdown calls Flush once the workers have stopped.
func (r *Reader) Run(ctx context.Context, interval time.Duration) error {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			_ = r.Flush(ctx)
		}
	}
}

// Flush sends the pending marks, chat marks before thread marks; a thread is
// marked only after its chat mark succeeded. A failed mark stays pending
// (merged with newer ones) unless Telegram rejected the request itself
// (4xx other than FLOOD_WAIT); a FLOOD_WAIT stops the flush. Failures are
// logged; the first one is returned.
func (r *Reader) Flush(ctx context.Context) error {
	r.mu.Lock()
	batch := r.pending
	r.pending = map[markKey]pendingMark{}
	r.mu.Unlock()

	keys := slices.SortedFunc(maps.Keys(batch), func(a, b markKey) int {
		return cmp.Or(cmp.Compare(a.threadID, b.threadID), cmp.Compare(a.chatID, b.chatID))
	})
	var (
		first  error
		failed = map[int64]bool{}
	)
	for i, k := range keys {
		p := batch[k]
		if k.threadID > 0 && failed[k.chatID] {
			r.keep(k, p)
			continue
		}
		err := r.send(ctx, k, p)
		if err == nil {
			continue
		}
		if first == nil {
			first = err
		}
		failed[k.chatID] = true
		if d, ok := tgerr.AsFloodWait(err); ok {
			r.logger.WarnContext(ctx, "flood wait", logx.ChatID(k.chatID), slog.Int64("wait_s", int64(d/time.Second)))
			for _, rest := range keys[i:] {
				r.keep(rest, batch[rest])
			}
			return first
		}
		if ctx.Err() == nil { // a flush cut by shutdown is retried by the final Flush
			r.logger.WarnContext(ctx, "mark read failed", logx.ChatID(k.chatID), slog.Int("thread_id", k.threadID), logx.Err(err))
		}
		if rpcErr, ok := tgerr.As(err); !ok || rpcErr.Code >= 500 {
			r.keep(k, p)
		}
	}
	return first
}

func (r *Reader) keep(k markKey, p pendingMark) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.add(k, p)
}

func (r *Reader) send(ctx context.Context, k markKey, p pendingMark) error {
	if k.threadID == 0 {
		return r.markOnce(ctx, k.chatID, 0, p.maxID, func() (string, error) { return r.readChat(ctx, k.chatID, p.t, p.maxID) })
	}
	return r.markOnce(ctx, k.chatID, k.threadID, p.maxID, func() (string, error) {
		_, err := r.api.MessagesReadDiscussion(ctx, &tg.MessagesReadDiscussionRequest{
			Peer:      &tg.InputPeerChannel{ChannelID: p.t.PeerID, AccessHash: p.t.AccessHash},
			MsgID:     k.threadID,
			ReadMaxID: p.maxID,
		})
		return "messages.readDiscussion", wrapRPC("messages.readDiscussion", k.chatID, k.threadID, err)
	})
}

// markOnce runs call unless (chatID, threadID) is already marked at or beyond
// maxID, and records the mark after the call succeeds.
func (r *Reader) markOnce(ctx context.Context, chatID int64, threadID, maxID int, call func() (string, error)) error {
	cur, ok, err := r.marks.Get(ctx, chatID, threadID)
	if err != nil {
		return fmt.Errorf("read mark lookup chat %d thread %d: %w", chatID, threadID, err)
	}
	if ok && maxID <= cur {
		r.skip(ctx, chatID, "already_read")
		return nil
	}
	start := time.Now()
	method, err := call()
	if err != nil {
		return err
	}
	if err := r.marks.Set(ctx, chatID, threadID, maxID); err != nil {
		return fmt.Errorf("read mark store chat %d thread %d: %w", chatID, threadID, err)
	}
	r.logger.DebugContext(ctx, "marked read", logx.ChatID(chatID), slog.Int("thread_id", threadID),
		slog.Int("max_id", maxID), slog.String("method", method), logx.Duration(time.Since(start)))
	return nil
}

func (r *Reader) readChat(ctx context.Context, chatID int64, t jobs.ReadTarget, maxID int) (string, error) {
	switch t.PeerKind {
	case jobs.PeerChannel:
		_, err := r.api.ChannelsReadHistory(ctx, &tg.ChannelsReadHistoryRequest{
			Channel: &tg.InputChannel{ChannelID: t.PeerID, AccessHash: t.AccessHash},
			MaxID:   maxID,
		})
		return "channels.readHistory", wrapRPC("channels.readHistory", chatID, 0, err)
	default:
		var peer tg.InputPeerClass
		switch t.PeerKind {
		case jobs.PeerUser:
			peer = &tg.InputPeerUser{UserID: t.PeerID, AccessHash: t.AccessHash}
		case jobs.PeerSelf:
			peer = &tg.InputPeerSelf{}
		default:
			peer = &tg.InputPeerChat{ChatID: t.PeerID}
		}
		_, err := r.api.MessagesReadHistory(ctx, &tg.MessagesReadHistoryRequest{Peer: peer, MaxID: maxID})
		return "messages.readHistory", wrapRPC("messages.readHistory", chatID, 0, err)
	}
}

// wrapRPC adds the method and numeric ids to a Telegram error, keeping it
// unwrappable for tgerr.AsFloodWait.
func wrapRPC(method string, chatID int64, threadID int, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s chat %d thread %d: %w", method, chatID, threadID, err)
}
