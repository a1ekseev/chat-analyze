// Package message is the stored chat message model and its Postgres repository.
package message

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"
	"unicode/utf8"
)

// Chat types stored in messages.chat_type.
const (
	ChatTypeChannel = "channel"
	ChatTypeGroup   = "group"
	ChatTypePrivate = "private_chat"
)

// Message mirrors the messages table 1:1 (nullable columns are pointers).
// It hides its content from logs and fmt: see LogValue, String and Format.
type Message struct {
	ChatID        int64
	MessageID     int
	ChatType      string
	ChatName      *string
	ChatUsername  *string
	SenderID      *int64
	SenderName    *string
	Out           bool
	Post          bool
	Text          *string `json:"message"`
	RawText       *string
	Entities      json.RawMessage
	MediaType     *string
	Views         int
	Forwards      int
	GroupedID     *int64
	ReplyToMsgID  *int
	TopicID       *int
	ThreadTopID   *int
	FwdFromChatID *int64
	FwdFromMsgID  *int
	IsService     bool
	ServiceAction *string
	CreatedAt     time.Time
	UpdatedAt     *time.Time

	// TopicTitle is not a column: the forum topic title carried along for
	// UpsertTopic by the worker.
	TopicTitle *string `json:"topic_title,omitempty"`
}

func (m Message) entitiesCount() int {
	if len(m.Entities) == 0 {
		return 0
	}
	var items []json.RawMessage
	if err := json.Unmarshal(m.Entities, &items); err != nil {
		return 0
	}
	return len(items)
}

// LogValue implements slog.LogValuer with metadata only.
func (m Message) LogValue() slog.Value {
	textLen := 0
	if m.Text != nil {
		textLen = utf8.RuneCountInString(*m.Text)
	}
	attrs := []slog.Attr{
		slog.Int64("chat_id", m.ChatID),
		slog.Int("message_id", m.MessageID),
		slog.String("chat_type", m.ChatType),
		slog.Int("text_len", textLen),
		slog.Int("entities_count", m.entitiesCount()),
		slog.Bool("is_service", m.IsService),
		slog.Bool("is_edit", m.UpdatedAt != nil),
	}
	if m.MediaType != nil {
		attrs = append(attrs, slog.String("media_type", *m.MediaType))
	}
	if m.TopicID != nil {
		attrs = append(attrs, slog.Int("topic_id", *m.TopicID))
	}
	return slog.GroupValue(attrs...)
}

// String renders identifiers only.
func (m Message) String() string {
	return fmt.Sprintf("message{chat_id=%d message_id=%d}", m.ChatID, m.MessageID)
}

// Format implements fmt.Formatter so every verb (%v %+v %#v %s) is safe.
func (m Message) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte(m.String()))
}
