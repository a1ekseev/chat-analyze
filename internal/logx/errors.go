package logx

import (
	"errors"
	"log/slog"
	"sort"
	"strconv"
	"strings"

	"github.com/gotd/td/tgerr"
	"github.com/jackc/pgx/v5/pgconn"
)

// pgxValueMarkers are fragments of pgx error texts that are followed by
// value-bearing detail (%#v of an argument, scanned value, ...).
var pgxValueMarkers = []string{
	"failed to encode args[",
	"can't scan into dest[",
	"unable to encode",
	"cannot scan",
	"cannot find encode plan",
	"unable to assign",
}

// SanitizeError renders err without any data-bearing parts: every Postgres
// error in the tree keeps only code and constraint (never Message/Detail/
// Where/Hint), every Telegram RPC error only type and code, and pgx
// value-bearing error clauses are cut off.
func SanitizeError(err error) string {
	if err == nil {
		return ""
	}
	var reps []replacement
	collectRedactions(err, &reps)
	s := err.Error()
	if len(reps) > 0 {
		// Longest first so that nested texts are replaced as a whole.
		sort.SliceStable(reps, func(i, j int) bool { return len(reps[i].from) > len(reps[j].from) })
		replaced := false
		for _, r := range reps {
			if r.from != "" && strings.Contains(s, r.from) {
				s = strings.ReplaceAll(s, r.from, r.to)
				replaced = true
			}
		}
		if !replaced {
			// Fail closed: the chain text does not embed the inner error text.
			s = reps[0].to
		}
	}
	return redactPgxValues(s)
}

type replacement struct{ from, to string }

// collectRedactions walks the whole error tree (single and multi unwrap).
func collectRedactions(err error, out *[]replacement) {
	if err == nil {
		return
	}
	switch e := err.(type) { //nolint:errorlint // intentional: walking the tree node by node
	case *pgconn.PgError:
		*out = append(*out, replacement{e.Error(), "pg error " + e.Code + " (" + e.ConstraintName + ")"})
	case *tgerr.Error:
		*out = append(*out, replacement{e.Error(), "tg error " + e.Type + " " + strconv.Itoa(e.Code)})
	}
	switch u := err.(type) { //nolint:errorlint // intentional: walking the tree node by node
	case interface{ Unwrap() error }:
		collectRedactions(u.Unwrap(), out)
	case interface{ Unwrap() []error }:
		for _, c := range u.Unwrap() {
			collectRedactions(c, out)
		}
	}
}

// redactPgxValues keeps the text up to the end of the earliest pgx
// value-bearing marker clause and replaces the remainder.
func redactPgxValues(s string) string {
	idx, end := -1, 0
	for _, m := range pgxValueMarkers {
		i := strings.Index(s, m)
		if i < 0 || (idx >= 0 && i >= idx) {
			continue
		}
		idx, end = i, i+len(m)
		if strings.HasSuffix(m, "[") {
			if j := strings.IndexByte(s[end:], ']'); j >= 0 {
				end += j + 1
			} else {
				end = len(s)
			}
		}
	}
	if idx < 0 {
		return s
	}
	return s[:end] + ": [REDACTED]"
}

// Err returns a sanitised error attribute: group "error" with "msg" and,
// when applicable, "pg" {code, constraint, table, column} or tg_type/tg_code.
func Err(err error) slog.Attr {
	if err == nil {
		return slog.Attr{}
	}
	args := []any{slog.String("msg", SanitizeError(err))}
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		args = append(args, slog.Group("pg",
			slog.String("code", pgErr.Code),
			slog.String("constraint", pgErr.ConstraintName),
			slog.String("table", pgErr.TableName),
			slog.String("column", pgErr.ColumnName),
		))
	}
	if tgErr, ok := errors.AsType[*tgerr.Error](err); ok {
		args = append(args,
			slog.String("tg_type", tgErr.Type),
			slog.Int("tg_code", tgErr.Code),
		)
	}
	return slog.Group("error", args...)
}
