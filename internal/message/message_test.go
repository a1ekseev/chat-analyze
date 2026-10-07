package message_test

import (
	"bytes"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/a1ekseev/chat-analyze/internal/message"
)

func TestMessage_LogValueHidesText(t *testing.T) {
	m := message.Message{
		ChatID:     -1001,
		MessageID:  7,
		ChatType:   message.ChatTypeGroup,
		Text:       new("CANARY"),
		RawText:    new("CANARY"),
		ChatName:   new("CANARY"),
		SenderName: new("CANARY"),
		Entities:   []byte(`[{"type":"bold"},{"type":"italic"}]`),
		TopicTitle: new("CANARY"),
	}

	out := fmt.Sprintf("%v %+v %s %#v", m, m, m, m)
	require.NotContains(t, out, "CANARY")
	require.Contains(t, out, "chat_id=-1001")
	require.Contains(t, out, "message_id=7")

	var buf bytes.Buffer
	l := slog.New(slog.NewJSONHandler(&buf, nil))
	l.Info("x", slog.Any("m", m))
	require.NotContains(t, buf.String(), "CANARY")
	require.Contains(t, buf.String(), `"text_len":6`)
	require.Contains(t, buf.String(), `"entities_count":2`)
}
