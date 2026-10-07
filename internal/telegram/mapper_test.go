package telegram

import (
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/a1ekseev/chat-analyze/internal/jobs"
	"github.com/a1ekseev/chat-analyze/internal/message"
)

const (
	tSelfID  = int64(1000)
	tAliceID = int64(2001)
	tBasicID = int64(3001)
	tSuperID = int64(4001)
	tForumID = int64(5001)
	tChanID  = int64(6001)
)

var (
	tSelf  = PeerInfo{ChatID: tSelfID, ID: tSelfID, Kind: jobs.PeerUser, Name: "Me", IsSelf: true}
	tPeers = func() map[int64]PeerInfo {
		m := map[int64]PeerInfo{}
		for _, p := range []PeerInfo{
			tSelf,
			PeerFromUser(&tg.User{ID: tAliceID, FirstName: "Alice", LastName: "A", Username: "alice", AccessHash: 222}),
			PeerFromChat(&tg.Chat{ID: tBasicID, Title: "Basic"}),
			PeerFromChannel(&tg.Channel{ID: tSuperID, Title: "Super", Username: "sg", AccessHash: 444, Megagroup: true}),
			PeerFromChannel(&tg.Channel{ID: tForumID, Title: "Forum", AccessHash: 555, Megagroup: true, Forum: true}),
			PeerFromChannel(&tg.Channel{ID: tChanID, Title: "News", Username: "news", AccessHash: 666, Broadcast: true}),
		} {
			m[p.ChatID] = p
		}
		return m
	}()
)

func tLookup(id int64) (PeerInfo, bool) { p, ok := tPeers[id]; return p, ok }

type mopt func(*tg.Message)

func tmsg(id int, peer, from tg.PeerClass, text string, opts ...mopt) *tg.Message {
	m := &tg.Message{ID: id, PeerID: peer, FromID: from, Message: text, Date: 1700000000 + id}
	for _, o := range opts {
		o(m)
	}
	m.SetFlags()
	return m
}

func withReply(h *tg.MessageReplyHeader) mopt {
	return func(m *tg.Message) { h.SetFlags(); m.ReplyTo = h }
}

func pu(id int64) tg.PeerClass  { return &tg.PeerUser{UserID: id} }
func pch(id int64) tg.PeerClass { return &tg.PeerChat{ChatID: id} }
func pc(id int64) tg.PeerClass  { return &tg.PeerChannel{ChannelID: id} }

func TestMap(t *testing.T) {
	superMarked := -1000000000000 - tSuperID
	chanMarked := -1000000000000 - tChanID

	tests := []struct {
		name  string
		msg   tg.MessageClass
		check func(t *testing.T, got jobs.SaveMessage)
	}{
		{"private incoming", tmsg(1, pu(tAliceID), pu(tAliceID), "hi"), func(t *testing.T, g jobs.SaveMessage) {
			assert.Equal(t, tAliceID, g.Message.ChatID)
			assert.Equal(t, message.ChatTypePrivate, g.Message.ChatType)
			assert.Equal(t, new("Alice A"), g.Message.ChatName)
			assert.Equal(t, new("alice"), g.Message.ChatUsername)
			assert.Equal(t, new(tAliceID), g.Message.SenderID)
			assert.Equal(t, new("Alice A"), g.Message.SenderName)
			assert.Equal(t, jobs.ReadTarget{PeerKind: jobs.PeerUser, PeerID: tAliceID, AccessHash: 222}, g.Read)
			assert.Equal(t, new("hi"), g.Message.Text)
			assert.Equal(t, new("hi"), g.Message.RawText)
			assert.Equal(t, time.Unix(1700000001, 0).UTC(), g.Message.CreatedAt)
		}},
		{"private outgoing", tmsg(2, pu(tAliceID), nil, "yo", func(m *tg.Message) { m.Out = true }), func(t *testing.T, g jobs.SaveMessage) {
			assert.Equal(t, new(tSelfID), g.Message.SenderID)
			assert.True(t, g.Message.Out)
			assert.Equal(t, new("Alice A"), g.Message.ChatName)
			assert.Equal(t, new("Me"), g.Message.SenderName)
		}},
		{"saved messages", tmsg(3, pu(tSelfID), pu(tSelfID), "note", func(m *tg.Message) { m.Out = true }), func(t *testing.T, g jobs.SaveMessage) {
			assert.Equal(t, new("Me"), g.Message.ChatName)
			assert.Equal(t, jobs.ReadTarget{PeerKind: jobs.PeerSelf, PeerID: tSelfID}, g.Read)
		}},
		{"basic group", tmsg(4, pch(tBasicID), pu(tAliceID), "g"), func(t *testing.T, g jobs.SaveMessage) {
			assert.Equal(t, -tBasicID, g.Message.ChatID)
			assert.Equal(t, message.ChatTypeGroup, g.Message.ChatType)
			assert.Equal(t, new("Basic"), g.Message.ChatName)
			assert.Equal(t, jobs.ReadTarget{PeerKind: jobs.PeerChat, PeerID: tBasicID}, g.Read)
		}},
		{"supergroup", tmsg(5, pc(tSuperID), pu(tAliceID), "s"), func(t *testing.T, g jobs.SaveMessage) {
			assert.Equal(t, superMarked, g.Message.ChatID)
			assert.Equal(t, message.ChatTypeGroup, g.Message.ChatType)
			assert.Equal(t, new("sg"), g.Message.ChatUsername)
			assert.Equal(t, jobs.ReadTarget{PeerKind: jobs.PeerChannel, PeerID: tSuperID, AccessHash: 444}, g.Read)
			assert.Nil(t, g.Message.TopicID)
		}},
		{"forum general", tmsg(6, pc(tForumID), pu(tAliceID), "f"), func(t *testing.T, g jobs.SaveMessage) {
			assert.Equal(t, new(1), g.Message.TopicID)
			assert.Equal(t, 0, g.Read.ThreadID)
		}},
		{"forum top-level in topic", tmsg(7, pc(tForumID), pu(tAliceID), "f", withReply(&tg.MessageReplyHeader{ForumTopic: true, ReplyToMsgID: 200})), func(t *testing.T, g jobs.SaveMessage) {
			assert.Equal(t, new(200), g.Message.TopicID)
			assert.Equal(t, 200, g.Read.ThreadID)
			assert.Nil(t, g.Message.ThreadTopID)
			assert.Equal(t, new(200), g.Message.ReplyToMsgID)
		}},
		{"forum reply in topic", tmsg(8, pc(tForumID), pu(tAliceID), "f", withReply(&tg.MessageReplyHeader{ForumTopic: true, ReplyToMsgID: 250, ReplyToTopID: 200})), func(t *testing.T, g jobs.SaveMessage) {
			assert.Equal(t, new(200), g.Message.TopicID)
			assert.Equal(t, new(250), g.Message.ReplyToMsgID)
			assert.Nil(t, g.Message.ThreadTopID)
			assert.Equal(t, 200, g.Read.ThreadID)
		}},
		{"topic create", &tg.MessageService{ID: 200, PeerID: pc(tForumID), FromID: pu(tAliceID), Date: 1700000200, Action: &tg.MessageActionTopicCreate{Title: "T"}}, func(t *testing.T, g jobs.SaveMessage) {
			assert.True(t, g.Message.IsService)
			assert.Equal(t, new("topic_create"), g.Message.ServiceAction)
			assert.Equal(t, new(200), g.Message.TopicID)
			assert.Equal(t, new("T"), g.Message.TopicTitle)
			assert.Equal(t, 200, g.Read.ThreadID)
			assert.Nil(t, g.Message.Text)
		}},
		{"topic edit empty title", &tg.MessageService{ID: 201, PeerID: pc(tForumID), Date: 1, Action: &tg.MessageActionTopicEdit{}, ReplyTo: func() tg.MessageReplyHeaderClass {
			h := &tg.MessageReplyHeader{ForumTopic: true, ReplyToMsgID: 200}
			h.SetFlags()
			return h
		}()}, func(t *testing.T, g jobs.SaveMessage) {
			assert.Equal(t, new("topic_edit"), g.Message.ServiceAction)
			assert.Nil(t, g.Message.TopicTitle)
			assert.Equal(t, new(200), g.Message.TopicID)
			assert.Equal(t, new(200), g.Message.ReplyToMsgID)
		}},
		{"private reply has no thread", tmsg(30, pu(tAliceID), pu(tAliceID), "r", withReply(&tg.MessageReplyHeader{ReplyToMsgID: 5})), func(t *testing.T, g jobs.SaveMessage) {
			assert.Equal(t, 0, g.Read.ThreadID)
			assert.Equal(t, new(5), g.Message.ThreadTopID)
		}},
		{"basic group reply has no thread", tmsg(31, pch(tBasicID), pu(tAliceID), "r", withReply(&tg.MessageReplyHeader{ReplyToMsgID: 5})), func(t *testing.T, g jobs.SaveMessage) {
			assert.Equal(t, 0, g.Read.ThreadID)
		}},
		{"forward without ids", tmsg(32, pc(tSuperID), pu(tAliceID), "fw", func(m *tg.Message) {
			m.FwdFrom = tg.MessageFwdHeader{SavedFromPeer: pc(tChanID)}
			m.FwdFrom.SetFlags()
		}), func(t *testing.T, g jobs.SaveMessage) {
			assert.NotNil(t, g.Message.FwdFromChatID)
			assert.Nil(t, g.Message.FwdFromMsgID)
		}},
		{"service chat_add_user", &tg.MessageService{ID: 9, PeerID: pch(tBasicID), Date: 1, Action: &tg.MessageActionChatAddUser{}}, func(t *testing.T, g jobs.SaveMessage) {
			assert.Equal(t, new("chat_add_user"), g.Message.ServiceAction)
			assert.Nil(t, g.Message.SenderID)
		}},
		{"channel post", tmsg(10, pc(tChanID), nil, "news", func(m *tg.Message) { m.Post = true; m.Views = 42; m.Forwards = 3 }), func(t *testing.T, g jobs.SaveMessage) {
			assert.Equal(t, message.ChatTypeChannel, g.Message.ChatType)
			assert.Equal(t, 42, g.Message.Views)
			assert.Equal(t, 3, g.Message.Forwards)
			assert.True(t, g.Message.Post)
			assert.Equal(t, new(chanMarked), g.Message.SenderID)
			assert.Equal(t, new("News"), g.Message.SenderName)
		}},
		{"auto-forward root", tmsg(11, pc(tSuperID), nil, "root", func(m *tg.Message) {
			m.FwdFrom = tg.MessageFwdHeader{SavedFromPeer: pc(tChanID), SavedFromMsgID: 7}
			m.FwdFrom.SetFlags()
		}), func(t *testing.T, g jobs.SaveMessage) {
			assert.Equal(t, new(chanMarked), g.Message.FwdFromChatID)
			assert.Equal(t, new(7), g.Message.FwdFromMsgID)
		}},
		{"forward without saved_from", tmsg(12, pc(tSuperID), pu(tAliceID), "fw", func(m *tg.Message) {
			m.FwdFrom = tg.MessageFwdHeader{FromID: pc(tChanID), ChannelPost: 9}
			m.FwdFrom.SetFlags()
		}), func(t *testing.T, g jobs.SaveMessage) {
			assert.Equal(t, new(chanMarked), g.Message.FwdFromChatID)
			assert.Equal(t, new(9), g.Message.FwdFromMsgID)
		}},
		{"comment", tmsg(13, pc(tSuperID), pu(tAliceID), "c", withReply(&tg.MessageReplyHeader{ReplyToMsgID: 10})), func(t *testing.T, g jobs.SaveMessage) {
			assert.Equal(t, new(10), g.Message.ThreadTopID)
			assert.Equal(t, new(10), g.Message.ReplyToMsgID)
			assert.Equal(t, 10, g.Read.ThreadID)
			assert.Nil(t, g.Message.TopicID)
		}},
		{"reply to comment", tmsg(14, pc(tSuperID), pu(tAliceID), "c", withReply(&tg.MessageReplyHeader{ReplyToMsgID: 11, ReplyToTopID: 10})), func(t *testing.T, g jobs.SaveMessage) {
			assert.Equal(t, new(10), g.Message.ThreadTopID)
			assert.Equal(t, new(11), g.Message.ReplyToMsgID)
			assert.Equal(t, 10, g.Read.ThreadID)
		}},
		{"album photo", tmsg(15, pc(tSuperID), pu(tAliceID), "", func(m *tg.Message) {
			m.GroupedID = 77
			m.Media = &tg.MessageMediaPhoto{}
		}), func(t *testing.T, g jobs.SaveMessage) {
			assert.Equal(t, new(int64(77)), g.Message.GroupedID)
			assert.Equal(t, new("photo"), g.Message.MediaType)
			assert.Nil(t, g.Message.Text)
			assert.Nil(t, g.Message.RawText)
			assert.Nil(t, g.Message.Entities)
		}},
		{"video with caption", tmsg(16, pc(tSuperID), pu(tAliceID), "see bold", func(m *tg.Message) {
			m.Media = &tg.MessageMediaDocument{Document: &tg.Document{Attributes: []tg.DocumentAttributeClass{&tg.DocumentAttributeVideo{}}}}
			m.Entities = []tg.MessageEntityClass{&tg.MessageEntityBold{Offset: 4, Length: 4}}
		}), func(t *testing.T, g jobs.SaveMessage) {
			assert.Equal(t, new("video"), g.Message.MediaType)
			assert.Equal(t, new("see **bold**"), g.Message.Text)
			assert.Equal(t, new("see bold"), g.Message.RawText)
			assert.JSONEq(t, `[{"type":"bold","offset":4,"length":4}]`, string(g.Message.Entities))
		}},
		{"edit", tmsg(17, pc(tSuperID), pu(tAliceID), "e", func(m *tg.Message) { m.EditDate = 1700001000 }), func(t *testing.T, g jobs.SaveMessage) {
			require.NotNil(t, g.Message.UpdatedAt)
			assert.Equal(t, time.Unix(1700001000, 0).UTC(), *g.Message.UpdatedAt)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, skip := Map(tc.msg, tSelf, tLookup)
			require.Empty(t, skip)
			tc.check(t, got)
		})
	}
}

func TestMap_Skips(t *testing.T) {
	_, skip := Map(&tg.MessageEmpty{ID: 1}, tSelf, tLookup)
	assert.Equal(t, SkipEmpty, skip)
	_, skip = Map(tmsg(1, nil, nil, "x"), tSelf, tLookup)
	assert.Equal(t, SkipNoPeer, skip)
	_, skip = Map(nil, tSelf, tLookup)
	assert.Equal(t, SkipUnsupported, skip)
}

func TestMap_UnknownPeer(t *testing.T) {
	none := func(int64) (PeerInfo, bool) { return PeerInfo{}, false }
	got, skip := Map(tmsg(1, pc(tSuperID), pu(tAliceID), "x"), tSelf, none)
	require.Empty(t, skip)
	assert.Nil(t, got.Message.ChatName)
	assert.Nil(t, got.Message.SenderName)
	assert.Equal(t, message.ChatTypeGroup, got.Message.ChatType)
	assert.Equal(t, int64(0), got.Read.AccessHash)

	got, skip = Map(tmsg(2, pu(tAliceID), pu(tAliceID), "x"), tSelf, none)
	require.Empty(t, skip)
	assert.Nil(t, got.Message.ChatName)
	assert.Equal(t, jobs.ReadTarget{PeerKind: jobs.PeerUser, PeerID: tAliceID}, got.Read)
}

func TestMap_NameFallbacks(t *testing.T) {
	look := func(id int64) (PeerInfo, bool) {
		switch id {
		case 1:
			return PeerInfo{Kind: jobs.PeerUser, Username: "only_user"}, true
		case 2:
			return PeerInfo{Kind: jobs.PeerUser}, true
		}
		return PeerInfo{}, false
	}
	got, _ := Map(tmsg(1, pu(1), pu(1), "x"), tSelf, look)
	assert.Equal(t, new("only_user"), got.Message.ChatName)
	got, _ = Map(tmsg(1, pu(2), pu(2), "x"), tSelf, look)
	assert.Equal(t, new("unknown"), got.Message.ChatName)
}

func TestMap_MediaTypes(t *testing.T) {
	doc := func(attrs ...tg.DocumentAttributeClass) tg.MessageMediaClass {
		return &tg.MessageMediaDocument{Document: &tg.Document{Attributes: attrs}}
	}
	tests := []struct {
		name  string
		media tg.MessageMediaClass
		want  *string
	}{
		{"nil", nil, nil},
		{"empty", &tg.MessageMediaEmpty{}, nil},
		{"photo", &tg.MessageMediaPhoto{}, new("photo")},
		{"sticker", doc(&tg.DocumentAttributeSticker{}), new("sticker")},
		{"gif", doc(&tg.DocumentAttributeVideo{}, &tg.DocumentAttributeAnimated{}), new("gif")},
		{"video", doc(&tg.DocumentAttributeVideo{}), new("video")},
		{"voice", doc(&tg.DocumentAttributeAudio{Voice: true}), new("voice")},
		{"audio", doc(&tg.DocumentAttributeAudio{}), new("audio")},
		{"document", doc(&tg.DocumentAttributeFilename{FileName: "a"}), new("document")},
		{"webpage", &tg.MessageMediaWebPage{}, new("webpage")},
		{"geo", &tg.MessageMediaGeo{}, new("geo")},
		{"geo live", &tg.MessageMediaGeoLive{}, new("geo")},
		{"venue", &tg.MessageMediaVenue{}, new("geo")},
		{"contact", &tg.MessageMediaContact{}, new("contact")},
		{"poll", &tg.MessageMediaPoll{}, new("poll")},
		{"dice", &tg.MessageMediaDice{}, new("dice")},
		{"other", &tg.MessageMediaUnsupported{}, new("other")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, skip := Map(tmsg(1, pu(tAliceID), pu(tAliceID), "x", func(m *tg.Message) { m.Media = tc.media }), tSelf, tLookup)
			require.Empty(t, skip)
			assert.Equal(t, tc.want, got.Message.MediaType)
		})
	}
}

func TestSnakeCase(t *testing.T) {
	assert.Equal(t, "topic_create", snakeCase("TopicCreate"))
	assert.Equal(t, "chat_add_user", snakeCase("ChatAddUser"))
	assert.Equal(t, "text_url", snakeCase("TextURL"))
}
