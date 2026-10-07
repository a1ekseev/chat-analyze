package telegram

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"unicode"
	"unicode/utf16"

	"github.com/gotd/td/tg"
)

// insertion is one marker placed at a UTF-16 position of the text.
type insertion struct {
	pos   int
	open  bool
	start int // entity start (for ordering)
	end   int // entity end (for ordering)
	idx   int // entity index (for ordering)
	s     string
}

// Markdown renders text with Telegram entities as Telethon-compatible markdown.
// Entity offsets are UTF-16 code units. Unsupported entity kinds produce no markup.
func Markdown(text string, entities []tg.MessageEntityClass) string {
	if len(entities) == 0 || text == "" {
		return text
	}
	u := utf16.Encode([]rune(text))
	n := len(u)

	var ins []insertion
	for i, e := range entities {
		if e == nil {
			continue
		}
		open, closing, ok := markers(e)
		if !ok {
			continue
		}
		start, end := clampRange(e.GetOffset(), e.GetLength(), n)
		if start >= end {
			continue
		}
		start, end = skipPair(u, start), skipPair(u, end)
		ins = append(ins,
			insertion{pos: start, open: true, start: start, end: end, idx: i, s: open},
			insertion{pos: end, open: false, start: start, end: end, idx: i, s: closing})
	}
	if len(ins) == 0 {
		return text
	}
	sort.SliceStable(ins, func(a, b int) bool {
		x, y := ins[a], ins[b]
		if x.pos != y.pos {
			return x.pos < y.pos
		}
		if x.open != y.open {
			return !x.open // closers first
		}
		if x.open {
			// outer (longer) first, then entity order
			if x.end != y.end {
				return x.end > y.end
			}
			return x.idx < y.idx
		}
		// inner (opened later) closes first
		if x.start != y.start {
			return x.start > y.start
		}
		return x.idx > y.idx
	})

	out := make([]uint16, 0, n+len(ins)*4)
	prev := 0
	for _, in := range ins {
		out = append(out, u[prev:in.pos]...)
		for _, r := range in.s {
			out = utf16.AppendRune(out, r)
		}
		prev = in.pos
	}
	out = append(out, u[prev:]...)
	return string(utf16.Decode(out))
}

func markers(e tg.MessageEntityClass) (open, closing string, ok bool) {
	switch v := e.(type) {
	case *tg.MessageEntityBold:
		return "**", "**", true
	case *tg.MessageEntityItalic:
		return "__", "__", true
	case *tg.MessageEntityStrike:
		return "~~", "~~", true
	case *tg.MessageEntityCode:
		return "`", "`", true
	case *tg.MessageEntityPre:
		return "```" + v.Language + "\n", "\n```", true
	case *tg.MessageEntityTextURL:
		return "[", "](" + v.URL + ")", true
	case *tg.MessageEntityMentionName:
		return "[", fmt.Sprintf("](tg://user?id=%d)", v.UserID), true
	}
	return "", "", false
}

func clampRange(offset, length, n int) (int, int) {
	if offset < 0 {
		offset = 0
	}
	if offset > n {
		offset = n
	}
	if length < 0 {
		length = 0
	}
	if length > n-offset {
		length = n - offset
	}
	return offset, offset + length
}

// skipPair moves pos past a surrogate pair when it would split one.
func skipPair(u []uint16, pos int) int {
	if pos > 0 && pos < len(u) && utf16.IsSurrogate(rune(u[pos-1])) && u[pos-1] < 0xDC00 && u[pos] >= 0xDC00 && u[pos] < 0xE000 {
		return pos + 1
	}
	return pos
}

type entityJSON struct {
	Type       string `json:"type"`
	Offset     int    `json:"offset"`
	Length     int    `json:"length"`
	URL        string `json:"url,omitempty"`
	UserID     *int64 `json:"user_id,omitempty"`
	Language   string `json:"language,omitempty"`
	DocumentID *int64 `json:"document_id,omitempty"`
}

// EntitiesJSON serialises all entities (offsets in UTF-16 units); nil when empty.
func EntitiesJSON(entities []tg.MessageEntityClass) json.RawMessage {
	out := make([]entityJSON, 0, len(entities))
	for _, e := range entities {
		if e == nil {
			continue
		}
		j := entityJSON{Type: entityTypeName(e), Offset: e.GetOffset(), Length: e.GetLength()}
		switch v := e.(type) {
		case *tg.MessageEntityTextURL:
			j.URL = v.URL
		case *tg.MessageEntityMentionName:
			id := v.UserID
			j.UserID = &id
		case *tg.MessageEntityPre:
			j.Language = v.Language
		case *tg.MessageEntityCustomEmoji:
			id := v.DocumentID
			j.DocumentID = &id
		}
		out = append(out, j)
	}
	if len(out) == 0 {
		return nil
	}
	b, err := json.Marshal(out)
	if err != nil {
		return nil
	}
	return b
}

// entityTypeName turns *tg.MessageEntityTextURL into "text_url".
func entityTypeName(e tg.MessageEntityClass) string {
	return snakeCase(strings.TrimPrefix(reflect.TypeOf(e).Elem().Name(), "MessageEntity"))
}

// snakeCase turns "TextURL" into "text_url".
func snakeCase(name string) string {
	rs := []rune(name)
	var b strings.Builder
	for i, r := range rs {
		if unicode.IsUpper(r) {
			// boundary: lower->Upper, or end of an acronym run (URL|Foo)
			if i > 0 && (unicode.IsLower(rs[i-1]) || unicode.IsDigit(rs[i-1]) ||
				(unicode.IsUpper(rs[i-1]) && i+1 < len(rs) && unicode.IsLower(rs[i+1]))) {
				b.WriteByte('_')
			}
			r = unicode.ToLower(r)
		}
		b.WriteRune(r)
	}
	return b.String()
}
