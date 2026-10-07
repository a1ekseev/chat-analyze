package message

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DBTX is satisfied by *pgxpool.Pool, pgx.Tx and *pgx.Conn.
type DBTX interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Repository persists messages and forum topics.
type Repository struct{ db DBTX }

// NewRepository wraps db.
func NewRepository(db DBTX) *Repository { return &Repository{db: db} }

const upsertSQL = `
INSERT INTO messages (
  chat_id, message_id, chat_type, chat_name, chat_username, sender_id, sender_name,
  out, post, message, raw_text, entities, media_type, views, forwards, grouped_id,
  reply_to_msg_id, topic_id, thread_top_id, fwd_from_chat_id, fwd_from_msg_id,
  is_service, service_action, created_at, updated_at
) VALUES (
  $1, $2, $3, $4, $5, $6, $7,
  $8, $9, $10, $11, $12, $13, $14, $15, $16,
  $17, $18, $19, $20, $21,
  $22, $23, $24, $25
)
ON CONFLICT (chat_id, message_id) DO UPDATE SET
  chat_type = EXCLUDED.chat_type,
  chat_name = COALESCE(EXCLUDED.chat_name, messages.chat_name),
  chat_username = COALESCE(EXCLUDED.chat_username, messages.chat_username),
  sender_name = COALESCE(EXCLUDED.sender_name, messages.sender_name),
  message = EXCLUDED.message, raw_text = EXCLUDED.raw_text, entities = EXCLUDED.entities,
  media_type = EXCLUDED.media_type,
  views = GREATEST(messages.views, EXCLUDED.views), forwards = GREATEST(messages.forwards, EXCLUDED.forwards),
  updated_at = COALESCE(EXCLUDED.updated_at, messages.updated_at)
WHERE COALESCE(EXCLUDED.updated_at, '-infinity'::timestamptz) >= COALESCE(messages.updated_at, '-infinity'::timestamptz)`

// Upsert inserts the message or applies an edit; an older version (an older
// edit, or the unedited original) never overwrites a newer one.
//
// Deliberate deviation from the literal spec §7 SQL: the spec's WHERE
// ("messages.updated_at IS NULL OR EXCLUDED.updated_at IS NULL OR ...") let a
// late original (updated_at NULL) overwrite a stored edit, contradicting the
// stated goal of that very condition. A NULL updated_at is treated as
// -infinity, so versions are ordered by edit time and a repeated original
// still applies (NULL >= NULL). chat_name/chat_username keep the known value
// when the peer lookup found nothing (NULL).
func (r *Repository) Upsert(ctx context.Context, m Message) error {
	var entities any
	if len(m.Entities) > 0 {
		entities = []byte(m.Entities)
	}
	_, err := r.db.Exec(ctx, upsertSQL,
		m.ChatID, m.MessageID, m.ChatType, m.ChatName, m.ChatUsername, m.SenderID, m.SenderName,
		m.Out, m.Post, m.Text, m.RawText, entities, m.MediaType, m.Views, m.Forwards, m.GroupedID,
		m.ReplyToMsgID, m.TopicID, m.ThreadTopID, m.FwdFromChatID, m.FwdFromMsgID,
		m.IsService, m.ServiceAction, m.CreatedAt, m.UpdatedAt)
	if err != nil {
		return fmt.Errorf("upsert message chat_id=%d message_id=%d: %w", m.ChatID, m.MessageID, err)
	}
	return nil
}

const updateViewsSQL = `
UPDATE messages SET views = GREATEST(views, $3)
WHERE chat_id = $1 AND message_id = $2`

// UpdateViews raises the view counter (never lowers it).
func (r *Repository) UpdateViews(ctx context.Context, chatID int64, messageID, views int) error {
	if _, err := r.db.Exec(ctx, updateViewsSQL, chatID, messageID, views); err != nil {
		return fmt.Errorf("update views chat_id=%d message_id=%d: %w", chatID, messageID, err)
	}
	return nil
}

const upsertTopicSQL = `
INSERT INTO tg_forum_topics (chat_id, topic_id, title) VALUES ($1, $2, $3)
ON CONFLICT (chat_id, topic_id) DO UPDATE SET title = EXCLUDED.title, updated_at = now()`

// UpsertTopic stores or renames a forum topic.
func (r *Repository) UpsertTopic(ctx context.Context, chatID int64, topicID int, title string) error {
	if _, err := r.db.Exec(ctx, upsertTopicSQL, chatID, topicID, title); err != nil {
		return fmt.Errorf("upsert topic chat_id=%d topic_id=%d: %w", chatID, topicID, err)
	}
	return nil
}

const lastMessageIDSQL = `SELECT max(message_id) FROM messages WHERE chat_id = $1`

// LastMessageID returns the highest stored message id of the chat.
func (r *Repository) LastMessageID(ctx context.Context, chatID int64) (int, bool, error) {
	var id *int
	if err := r.db.QueryRow(ctx, lastMessageIDSQL, chatID).Scan(&id); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return 0, false, fmt.Errorf("last message id chat_id=%d: %w", chatID, err)
	}
	if id == nil {
		return 0, false, nil
	}
	return *id, true, nil
}
