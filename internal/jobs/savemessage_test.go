package jobs_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/a1ekseev/chat-analyze/internal/jobs"
	"github.com/a1ekseev/chat-analyze/internal/message"
)

func sample() jobs.SaveMessage {
	return jobs.SaveMessage{
		Message: message.Message{
			ChatID:     -1001234,
			MessageID:  5,
			ChatType:   message.ChatTypeChannel,
			Text:       new("CANARY"),
			RawText:    new("CANARY"),
			SenderName: new("CANARY"),
			Entities:   json.RawMessage(`[{"type":"bold"}]`),
			TopicID:    new(9),
			TopicTitle: new("CANARY"),
			CreatedAt:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		Read: jobs.ReadTarget{PeerKind: jobs.PeerChannel, PeerID: 1234, AccessHash: 42, IsMin: true, ThreadID: 9},
	}
}

func TestDedupKey(t *testing.T) {
	require.Equal(t, "-1001234:5", jobs.DedupKey(-1001234, 5))
	require.Equal(t, "save_message", jobs.KindSaveMessage)
}

func TestSaveMessage_JSONRoundTrip(t *testing.T) {
	in := sample()
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out jobs.SaveMessage
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, in.Read, out.Read)
	require.Equal(t, in.Message.ChatID, out.Message.ChatID)
	require.Equal(t, *in.Message.Text, *out.Message.Text)
	require.Equal(t, *in.Message.TopicTitle, *out.Message.TopicTitle)
	require.True(t, in.Message.CreatedAt.Equal(out.Message.CreatedAt))
	require.JSONEq(t, string(in.Message.Entities), string(out.Message.Entities))
}

func TestSaveMessage_LogValueHidesText(t *testing.T) {
	m := sample()
	var buf bytes.Buffer
	l := slog.New(slog.NewJSONHandler(&buf, nil))
	l.Info("x", slog.Any("job", m), slog.Any("ptr", &m))
	l.Info("y", m.LogValue().Group()[0])
	out := buf.String()
	require.NotContains(t, out, "CANARY")
	require.Contains(t, out, `"peer_kind":"channel"`)
	require.Contains(t, out, `"thread_id":9`)
	require.Contains(t, out, `"text_len":6`)
	for _, s := range []string{fmt.Sprintf("%+v", m), fmt.Sprintf("%#v", m), m.String()} {
		require.NotContains(t, s, "CANARY")
		require.NotContains(t, s, "42")
	}
}
