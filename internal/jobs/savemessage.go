// Package jobs defines queue job kinds and their payloads.
package jobs

import (
	"fmt"
	"log/slog"
	"strconv"

	"github.com/a1ekseev/chat-analyze/internal/message"
)

// KindSaveMessage is the queue kind of SaveMessage jobs.
const KindSaveMessage = "save_message"

// Peer kinds of ReadTarget.
const (
	PeerUser    = "user"
	PeerSelf    = "self"
	PeerChat    = "chat"
	PeerChannel = "channel"
)

// DedupKey is the queue dedup key of a message: "<chat>:<msg>".
func DedupKey(chatID int64, messageID int) string {
	return strconv.FormatInt(chatID, 10) + ":" + strconv.Itoa(messageID)
}

// ReadTarget says how to mark a saved message as read in Telegram.
type ReadTarget struct {
	PeerKind   string `json:"peer_kind"`
	PeerID     int64  `json:"peer_id"`
	AccessHash int64  `json:"access_hash"`
	IsMin      bool   `json:"is_min"`
	ThreadID   int    `json:"thread_id"`
}

// SaveMessage is the payload of a save_message job. It hides message content
// and the access hash from logs and fmt.
type SaveMessage struct {
	Message message.Message `json:"message"`
	Read    ReadTarget      `json:"read"`
}

// LogValue implements slog.LogValuer with metadata only.
func (s SaveMessage) LogValue() slog.Value {
	attrs := s.Message.LogValue().Group()
	attrs = append(attrs,
		slog.String("peer_kind", s.Read.PeerKind),
		slog.Int("thread_id", s.Read.ThreadID),
	)
	return slog.GroupValue(attrs...)
}

// String renders identifiers only.
func (s SaveMessage) String() string {
	return fmt.Sprintf("save_message{chat_id=%d message_id=%d}", s.Message.ChatID, s.Message.MessageID)
}

// Format implements fmt.Formatter so every verb is safe.
func (s SaveMessage) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte(s.String()))
}
