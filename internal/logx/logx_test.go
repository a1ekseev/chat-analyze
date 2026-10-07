package logx_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/gotd/log"
	"github.com/gotd/td/tgerr"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/a1ekseev/chat-analyze/internal/logx"
)

func canaryPgErr() *pgconn.PgError {
	return &pgconn.PgError{
		Code:           "23514",
		ConstraintName: "messages_chat_type_check",
		TableName:      "messages",
		ColumnName:     "chat_type",
		Message:        "CANARY",
		Detail:         "Failing row contains (CANARY)",
		Where:          "CANARY",
		Hint:           "CANARY",
	}
}

func TestSanitizeError_PgErrorDropsDetail(t *testing.T) {
	err := fmt.Errorf("upsert: %w", canaryPgErr())
	s := logx.SanitizeError(err)
	assert.Contains(t, s, "23514")
	assert.Contains(t, s, "messages_chat_type_check")
	assert.NotContains(t, s, "CANARY")
}

func TestSanitizeError_TgError(t *testing.T) {
	s := logx.SanitizeError(fmt.Errorf("call: %w", tgerr.New(420, "FLOOD_WAIT_5")))
	assert.Contains(t, s, "FLOOD_WAIT")
	assert.Contains(t, s, "420")
}

func TestSanitizeError_Plain(t *testing.T) {
	assert.Equal(t, "boom", logx.SanitizeError(errors.New("boom")))
	assert.Empty(t, logx.SanitizeError(nil))
}

func TestErrAttr_JSON(t *testing.T) {
	var buf bytes.Buffer
	l := logx.New(&buf, slog.LevelInfo, "json")
	l.LogAttrs(context.Background(), slog.LevelError, "failed", logx.Err(fmt.Errorf("x: %w", canaryPgErr())))
	assert.NotContains(t, buf.String(), "CANARY")

	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
	e, ok := rec["error"].(map[string]any)
	require.True(t, ok)
	pg, ok := e["pg"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "23514", pg["code"])
	assert.Equal(t, "messages_chat_type_check", pg["constraint"])
	assert.Equal(t, "messages", pg["table"])
	assert.Equal(t, "chat_type", pg["column"])
	assert.Contains(t, e["msg"], "23514")
}

func TestErrAttr_Tg(t *testing.T) {
	var buf bytes.Buffer
	l := logx.New(&buf, slog.LevelInfo, "json")
	l.LogAttrs(context.Background(), slog.LevelError, "failed", logx.Err(tgerr.New(420, "FLOOD_WAIT_5")))
	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
	e := rec["error"].(map[string]any)
	assert.Equal(t, "FLOOD_WAIT", e["tg_type"])
	assert.InDelta(t, 420, e["tg_code"], 0)
}

func TestSafeAttrs(t *testing.T) {
	var buf bytes.Buffer
	l := logx.New(&buf, slog.LevelDebug, "json")
	l.LogAttrs(context.Background(), slog.LevelInfo, "evt",
		logx.Component("worker"), logx.ChatID(-100), logx.MsgID(7), logx.JobID(3),
		logx.TopicID(2), logx.Attempt(1), logx.Duration(1500*1000*1000))
	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
	assert.Equal(t, "worker", rec["component"])
	assert.InDelta(t, -100, rec["chat_id"], 0)
	assert.InDelta(t, 7, rec["message_id"], 0)
	assert.InDelta(t, 3, rec["job_id"], 0)
	assert.InDelta(t, 2, rec["topic_id"], 0)
	assert.InDelta(t, 1, rec["attempt"], 0)
	assert.InDelta(t, 1500, rec["duration_ms"], 0)
}

func TestNew_Text(t *testing.T) {
	var buf bytes.Buffer
	l := logx.New(&buf, slog.LevelInfo, "text")
	l.InfoContext(context.Background(), "hello")
	assert.Contains(t, buf.String(), "msg=hello")
	l.DebugContext(context.Background(), "hidden")
	assert.NotContains(t, buf.String(), "hidden")
}

func TestGotdLogger_FiltersAttrs(t *testing.T) {
	var buf bytes.Buffer
	gl := logx.NewGotdLogger(logx.New(&buf, slog.LevelDebug, "json"), slog.LevelWarn)
	ctx := context.Background()

	assert.False(t, gl.Enabled(ctx, log.LevelDebug))
	assert.True(t, gl.Enabled(ctx, log.LevelWarn))

	gl.Log(ctx, log.LevelWarn, "x",
		log.String("message", "CANARY"),
		log.String("error", "bad thing"),
		log.Int64("channel_id", 5),
		log.Any("state", "CANARY"),
		log.Group("count", log.String("k", "CANARY")),
	)
	out := buf.String()
	assert.Contains(t, out, `"channel_id":5`)
	assert.Contains(t, out, `"component":"gotd"`)
	assert.NotContains(t, out, "CANARY")
	assert.NotContains(t, out, `"message"`)
	assert.NotContains(t, out, `"state"`)
	assert.NotContains(t, out, `"k"`)
}

func TestGotdLogger_TruncatesError(t *testing.T) {
	var buf bytes.Buffer
	gl := logx.NewGotdLogger(logx.New(&buf, slog.LevelDebug, "json"), slog.LevelWarn)
	long := make([]byte, 500)
	for i := range long {
		long[i] = 'a'
	}
	gl.Log(context.Background(), log.LevelError, "x", log.String("error", string(long)))
	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
	assert.Len(t, rec["error"], 200)

	buf.Reset()
	gl.Log(context.Background(), log.LevelError, "x", log.Error(tgerr.New(420, "FLOOD_WAIT_5")))
	assert.Contains(t, buf.String(), "FLOOD_WAIT")
}

func TestParseLevel(t *testing.T) {
	tests := []struct {
		in      string
		want    slog.Level
		wantErr bool
	}{
		{"debug", slog.LevelDebug, false},
		{"info", slog.LevelInfo, false},
		{"warn", slog.LevelWarn, false},
		{"error", slog.LevelError, false},
		{"INFO", slog.LevelInfo, false},
		{"verbose", 0, true},
		{"", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := logx.ParseLevel(tt.in)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

type fakeError string

func (e fakeError) Error() string { return string(e) }

func TestSanitizeError_JoinOfPgErrors(t *testing.T) {
	a := &pgconn.PgError{Code: "23514", ConstraintName: "a_chk", Message: "CANARY-A"}
	b := &pgconn.PgError{Code: "23505", ConstraintName: "b_key", Message: "CANARY-B", Detail: "CANARY-D"}
	s := logx.SanitizeError(fmt.Errorf("op: %w", errors.Join(a, b)))
	assert.NotContains(t, s, "CANARY")
	assert.Contains(t, s, "a_chk")
	assert.Contains(t, s, "b_key")
}

func TestSanitizeError_SameErrorWrappedTwice(t *testing.T) {
	p := canaryPgErr()
	s := logx.SanitizeError(fmt.Errorf("x: %w, y: %w", p, p))
	assert.NotContains(t, s, "CANARY")
	assert.Contains(t, s, "23514")
}

func TestSanitizeError_TgInsidePgChain(t *testing.T) {
	tg := tgerr.New(400, "PEER_ID_INVALID")
	s := logx.SanitizeError(fmt.Errorf("a: %w", errors.Join(canaryPgErr(), fmt.Errorf("b: %w", tg))))
	assert.NotContains(t, s, "CANARY")
	assert.Contains(t, s, "400")
	assert.Contains(t, s, "PEER_ID_INVALID")
	assert.Contains(t, s, "23514")
}

func TestSanitizeError_FailClosed(t *testing.T) {
	// Wrapper whose text does not embed the inner error text.
	err := &opaqueWrapError{inner: canaryPgErr()}
	s := logx.SanitizeError(err)
	assert.NotContains(t, s, "CANARY")
	assert.Contains(t, s, "23514")
}

type opaqueWrapError struct{ inner error }

func (o *opaqueWrapError) Error() string { return "wrapped CANARY text" }
func (o *opaqueWrapError) Unwrap() error { return o.inner }

func TestSanitizeError_PgxEncodeError(t *testing.T) {
	m := pgtype.NewMap()
	// Real pgx unsupported-value error: value rendered with %#v.
	type secret struct{ Body string }
	buf, err := m.Encode(pgtype.Int4OID, pgtype.TextFormatCode, secret{"CANARY-VALUE"}, nil)
	require.Nil(t, buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "CANARY-VALUE", "precondition: raw pgx error carries the value")

	s := logx.SanitizeError(fmt.Errorf("failed to encode args[3]: %w", err))
	assert.NotContains(t, s, "CANARY")
	assert.Contains(t, s, "failed to encode args[3]")
	assert.Contains(t, s, "[REDACTED]")

	s = logx.SanitizeError(fmt.Errorf("insert: %w", err))
	assert.NotContains(t, s, "CANARY")
	assert.Contains(t, s, "[REDACTED]")
}

func TestSanitizeError_PgxMarkers(t *testing.T) {
	for _, in := range []string{
		"can't scan into dest[2] (col: text): cannot scan text (OID 25) into *int: CANARY",
		"x: unable to assign CANARY",
		"cannot find encode plan CANARY",
		"cannot scan CANARY",
	} {
		s := logx.SanitizeError(errors.New(in))
		assert.NotContains(t, s, "CANARY", in)
		assert.Contains(t, s, "[REDACTED]", in)
	}
	assert.Equal(t, "can't scan into dest[2]: [REDACTED]",
		logx.SanitizeError(fakeError("can't scan into dest[2] (col: text): CANARY")))
}
