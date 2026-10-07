package telegram

import (
	"encoding/json"
	"testing"
	"unicode/utf8"

	"github.com/gotd/td/tg"
)

func TestMarkdown(t *testing.T) {
	tests := []struct {
		name string
		text string
		ents []tg.MessageEntityClass
		want string
	}{
		{
			name: "bold link mention with emoji",
			text: "Bold 😀 link alice x",
			ents: []tg.MessageEntityClass{
				&tg.MessageEntityBold{Offset: 0, Length: 4},
				&tg.MessageEntityTextURL{Offset: 8, Length: 4, URL: "https://ex.com"},
				&tg.MessageEntityMentionName{Offset: 13, Length: 5, UserID: 2001},
			},
			want: "**Bold** 😀 [link](https://ex.com) [alice](tg://user?id=2001) x",
		},
		{
			name: "nested",
			text: "hello",
			ents: []tg.MessageEntityClass{
				&tg.MessageEntityBold{Offset: 0, Length: 5},
				&tg.MessageEntityItalic{Offset: 0, Length: 5},
			},
			want: "**__hello__**",
		},
		{
			name: "pre with language",
			text: "x := 1",
			ents: []tg.MessageEntityClass{&tg.MessageEntityPre{Offset: 0, Length: 6, Language: "go"}},
			want: "```go\nx := 1\n```",
		},
		{
			name: "code and strike",
			text: "a b",
			ents: []tg.MessageEntityClass{
				&tg.MessageEntityCode{Offset: 0, Length: 1},
				&tg.MessageEntityStrike{Offset: 2, Length: 1},
			},
			want: "`a` ~~b~~",
		},
		{
			name: "unsupported kinds untouched",
			text: "abcd",
			ents: []tg.MessageEntityClass{
				&tg.MessageEntityUnderline{Offset: 0, Length: 1},
				&tg.MessageEntitySpoiler{Offset: 1, Length: 1},
				&tg.MessageEntityBlockquote{Offset: 2, Length: 1},
				&tg.MessageEntityCustomEmoji{Offset: 3, Length: 1, DocumentID: 5},
			},
			want: "abcd",
		},
		{name: "no entities", text: "plain", want: "plain"},
		{name: "empty text", text: "", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Markdown(tc.text, tc.ents); got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}

func TestMarkdown_MalformedEntities(t *testing.T) {
	text := "a😀b"
	cases := [][]tg.MessageEntityClass{
		{&tg.MessageEntityBold{Offset: 100, Length: 5}},
		{&tg.MessageEntityBold{Offset: 0, Length: -3}},
		{&tg.MessageEntityBold{Offset: -4, Length: 2}},
		{&tg.MessageEntityBold{Offset: 2, Length: 1}}, // inside the surrogate pair
		{&tg.MessageEntityItalic{Offset: 1, Length: 1}},
		{&tg.MessageEntityBold{Offset: 0, Length: 2}, &tg.MessageEntityItalic{Offset: 1, Length: 3}}, // crossing
		{&tg.MessageEntityBold{Offset: 0, Length: int(^uint(0) >> 1)}},
		{nil},
	}
	for i, ents := range cases {
		if got := Markdown(text, ents); !utf8.ValidString(got) {
			t.Fatalf("case %d: invalid UTF-8 %q", i, got)
		}
	}
}

func TestEntitiesJSON(t *testing.T) {
	if got := EntitiesJSON(nil); got != nil {
		t.Fatalf("want nil, got %s", got)
	}
	got := EntitiesJSON([]tg.MessageEntityClass{&tg.MessageEntityTextURL{Offset: 0, Length: 4, URL: "https://ex.com"}})
	want := `[{"type":"text_url","offset":0,"length":4,"url":"https://ex.com"}]`
	if string(got) != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestEntitiesJSON_Kinds(t *testing.T) {
	got := EntitiesJSON([]tg.MessageEntityClass{
		&tg.MessageEntityMentionName{Offset: 1, Length: 2, UserID: 7},
		&tg.MessageEntityPre{Offset: 0, Length: 1, Language: "go"},
		&tg.MessageEntityPre{Offset: 0, Length: 1},
		&tg.MessageEntityCustomEmoji{Offset: 0, Length: 2, DocumentID: 9},
		&tg.MessageEntityBankCard{Offset: 0, Length: 2},
	})
	var arr []map[string]any
	if err := json.Unmarshal(got, &arr); err != nil {
		t.Fatal(err)
	}
	types := []string{"mention_name", "pre", "pre", "custom_emoji", "bank_card"}
	for i, ty := range types {
		if arr[i]["type"] != ty {
			t.Fatalf("%d: type %v want %s", i, arr[i]["type"], ty)
		}
	}
	if arr[0]["user_id"] != float64(7) || arr[1]["language"] != "go" || arr[3]["document_id"] != float64(9) {
		t.Fatalf("bad fields: %s", got)
	}
	if _, ok := arr[2]["language"]; ok {
		t.Fatalf("empty language must be omitted")
	}
}
