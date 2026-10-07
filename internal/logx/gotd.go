package logx

import (
	"context"
	"log/slog"

	"github.com/gotd/log"
)

const maxGotdErrorLen = 200

// gotdAllowed is the allowlist of gotd attribute keys that may reach our logs.
var gotdAllowed = map[string]struct{}{
	"type_id": {}, "msg_id": {}, "channel_id": {}, "pts": {}, "qts": {}, "seq": {},
	"dc": {}, "error": {}, "count": {}, "date": {}, "state": {}, "timeout": {},
	"duration": {},
}

type gotdLogger struct {
	l   *slog.Logger
	min slog.Level
}

// NewGotdLogger adapts slog to the gotd logging interface. Only allowlisted
// scalar attributes are forwarded; "error" strings are truncated.
func NewGotdLogger(l *slog.Logger, minLevel slog.Level) log.Logger {
	return &gotdLogger{l: l.With(Component("gotd")), min: minLevel}
}

func (g *gotdLogger) Enabled(ctx context.Context, lvl log.Level) bool {
	return slog.Level(lvl) >= g.min && g.l.Enabled(ctx, slog.Level(lvl))
}

func (g *gotdLogger) Log(ctx context.Context, lvl log.Level, msg string, attrs ...log.Attr) {
	if !g.Enabled(ctx, lvl) {
		return
	}
	out := make([]slog.Attr, 0, len(attrs))
	for _, a := range attrs {
		if _, ok := gotdAllowed[a.Key]; !ok {
			continue
		}
		if sa, ok := convertGotdAttr(a); ok {
			out = append(out, sa)
		}
	}
	// msg is a library-provided static string; gotd does not put user data in it.
	//nolint:sloglint // adapter forwards gotd's own static message; attrs are allowlisted above
	g.l.LogAttrs(ctx, slog.Level(lvl), msg, out...)
}

func convertGotdAttr(a log.Attr) (slog.Attr, bool) {
	switch a.Value.Kind() {
	case log.KindInt64:
		return slog.Int64(a.Key, a.Value.Int64()), true
	case log.KindUint64:
		return slog.Uint64(a.Key, a.Value.Uint64()), true
	case log.KindBool:
		return slog.Bool(a.Key, a.Value.Bool()), true
	case log.KindDuration:
		return slog.Duration(a.Key, a.Value.Duration()), true
	case log.KindFloat64:
		return slog.Float64(a.Key, a.Value.Float64()), true
	case log.KindString:
		if a.Key == "error" {
			return slog.String(a.Key, truncate(a.Value.String(), maxGotdErrorLen)), true
		}
	case log.KindError:
		if a.Key == "error" {
			return slog.String(a.Key, truncate(SanitizeError(a.Value.Error()), maxGotdErrorLen)), true
		}
	case log.KindAny, log.KindTime, log.KindGroup:
	}
	return slog.Attr{}, false
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}
