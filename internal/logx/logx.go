// Package logx is the single entry point for logging: logger construction,
// typed safe attributes, error sanitising and the gotd logging adapter.
// Nothing here ever emits message content, personal data or secrets.
package logx

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"
)

// New builds a slog logger writing to w. format is "json" or "text"
// (anything else falls back to json).
func New(w io.Writer, level slog.Level, format string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: level}
	if strings.EqualFold(format, "text") {
		return slog.New(slog.NewTextHandler(w, opts))
	}
	return slog.New(slog.NewJSONHandler(w, opts))
}

// ParseLevel parses debug|info|warn|error (case-insensitive).
func ParseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("parse log level %q: want debug|info|warn|error", s)
	}
}

// Component is the logging component name attribute.
func Component(name string) slog.Attr { return slog.String("component", name) }

// ChatID is the chat identifier attribute.
func ChatID(id int64) slog.Attr { return slog.Int64("chat_id", id) }

// MsgID is the message identifier attribute.
func MsgID(id int) slog.Attr { return slog.Int("message_id", id) }

// JobID is the job identifier attribute.
func JobID(id int64) slog.Attr { return slog.Int64("job_id", id) }

// TopicID is the forum topic identifier attribute.
func TopicID(id int) slog.Attr { return slog.Int("topic_id", id) }

// Attempt is the job attempt number attribute.
func Attempt(n int) slog.Attr { return slog.Int("attempt", n) }

// Duration is a duration in whole milliseconds.
func Duration(d time.Duration) slog.Attr { return slog.Int64("duration_ms", d.Milliseconds()) }
