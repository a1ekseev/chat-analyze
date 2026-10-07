package telegram

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/gotd/td/tg"
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

// Reader marks Telegram history as read.
type Reader struct {
	api    API
	marks  MarkStore
	logger *slog.Logger
}

// NewReader returns a Reader.
func NewReader(api API, marks MarkStore, logger *slog.Logger) *Reader {
	return &Reader{api: api, marks: marks, logger: logger.With(logx.Component("reader"))}
}

func (r *Reader) skip(ctx context.Context, chatID int64, reason string) {
	r.logger.DebugContext(ctx, "mark read skipped", logx.ChatID(chatID), slog.String("reason", reason))
}

// MarkRead marks messages up to maxID read in the chat described by t (spec §6).
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

	if err := r.markOnce(ctx, chatID, 0, maxID, func() (string, error) { return r.readChat(ctx, chatID, t, maxID) }); err != nil {
		return err
	}
	if t.PeerKind == jobs.PeerChannel && t.ThreadID > 0 {
		return r.markOnce(ctx, chatID, t.ThreadID, maxID, func() (string, error) {
			_, err := r.api.MessagesReadDiscussion(ctx, &tg.MessagesReadDiscussionRequest{
				Peer:      &tg.InputPeerChannel{ChannelID: t.PeerID, AccessHash: t.AccessHash},
				MsgID:     t.ThreadID,
				ReadMaxID: maxID,
			})
			return "messages.readDiscussion", wrapRPC("messages.readDiscussion", chatID, t.ThreadID, err)
		})
	}
	return nil
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
