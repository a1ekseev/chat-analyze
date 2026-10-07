package telegram

import (
	"fmt"
	"strings"
	"time"

	"github.com/gotd/td/constant"
	"github.com/gotd/td/tg"

	"github.com/a1ekseev/chat-analyze/internal/jobs"
	"github.com/a1ekseev/chat-analyze/internal/message"
)

// PeerLookup finds a known peer by marked chat id.
type PeerLookup func(chatID int64) (PeerInfo, bool)

// Skip reasons returned by Map.
const (
	SkipEmpty       = "empty"
	SkipNoPeer      = "no_peer"
	SkipUnsupported = "unsupported"
)

// markedID returns the marked (TDLib) id of a peer.
func markedID(p tg.PeerClass) (int64, bool) {
	var id constant.TDLibPeerID
	switch v := p.(type) {
	case *tg.PeerUser:
		id.User(v.UserID)
	case *tg.PeerChat:
		id.Chat(v.ChatID)
	case *tg.PeerChannel:
		id.Channel(v.ChannelID)
	default:
		return 0, false
	}
	return int64(id), true
}

// displayName follows the spec §5 name rules; nil when the peer is unknown.
func displayName(p PeerInfo, known bool) *string {
	if !known {
		return nil
	}
	name := p.Name
	if name == "" {
		name = p.Username
	}
	if name == "" {
		name = "unknown"
	}
	return &name
}

func nonEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// header is the part shared by tg.Message and tg.MessageService.
type header struct {
	id      int
	peer    tg.PeerClass
	from    tg.PeerClass
	out     bool
	post    bool
	date    int
	replyTo tg.MessageReplyHeaderClass
}

// Map converts a gotd message into the save_message payload. A non-empty
// second result is the reason the message must not be saved.
func Map(msg tg.MessageClass, self PeerInfo, lookup PeerLookup) (jobs.SaveMessage, string) {
	var (
		h  header
		m  *tg.Message
		sv *tg.MessageService
	)
	switch v := msg.(type) {
	case *tg.Message:
		m = v
		h = header{v.ID, v.PeerID, v.FromID, v.Out, v.Post, v.Date, v.ReplyTo}
	case *tg.MessageService:
		sv = v
		h = header{v.ID, v.PeerID, v.FromID, v.Out, v.Post, v.Date, v.ReplyTo}
	case *tg.MessageEmpty:
		return jobs.SaveMessage{}, SkipEmpty
	default:
		return jobs.SaveMessage{}, SkipUnsupported
	}
	chatID, ok := markedID(h.peer)
	if !ok {
		return jobs.SaveMessage{}, SkipNoPeer
	}

	find := func(id int64) (PeerInfo, bool) {
		if p, ok := lookup(id); ok {
			return p, true
		}
		if id == self.ID && self.Kind == jobs.PeerUser && self.ID != 0 {
			return self, true
		}
		return PeerInfo{}, false
	}
	chat, chatKnown := find(chatID)

	out := jobs.SaveMessage{}
	out.Message = message.Message{
		ChatID:    chatID,
		MessageID: h.id,
		Out:       h.out,
		Post:      h.post,
		CreatedAt: time.Unix(int64(h.date), 0).UTC(),
	}
	rd := &out.Read
	switch p := h.peer.(type) {
	case *tg.PeerUser:
		out.Message.ChatType = message.ChatTypePrivate
		out.Message.ChatName = displayName(chat, chatKnown)
		rd.PeerID = p.UserID
		if p.UserID == self.ID {
			rd.PeerKind = jobs.PeerSelf
		} else {
			rd.PeerKind = jobs.PeerUser
			rd.AccessHash, rd.IsMin = chat.AccessHash, chat.IsMin
		}
	case *tg.PeerChat:
		out.Message.ChatType = message.ChatTypeGroup
		out.Message.ChatName = displayName(chat, chatKnown)
		rd.PeerKind, rd.PeerID = jobs.PeerChat, p.ChatID
	case *tg.PeerChannel:
		out.Message.ChatType = message.ChatTypeGroup
		if chatKnown && chat.IsBroadcast {
			out.Message.ChatType = message.ChatTypeChannel
		}
		out.Message.ChatName = displayName(chat, chatKnown)
		rd.PeerKind, rd.PeerID = jobs.PeerChannel, p.ChannelID
		rd.AccessHash, rd.IsMin = chat.AccessHash, chat.IsMin
	}
	if chatKnown {
		out.Message.ChatUsername = nonEmpty(chat.Username)
	}

	// Sender.
	var senderID int64
	hasSender := true
	switch {
	case h.from != nil:
		senderID, hasSender = markedID(h.from)
	case h.out:
		senderID = self.ID
	case h.post:
		senderID = chatID
	default:
		if _, isUser := h.peer.(*tg.PeerUser); isUser {
			senderID = chatID
		} else {
			hasSender = false
		}
	}
	if hasSender {
		out.Message.SenderID = &senderID
		sender, known := find(senderID)
		out.Message.SenderName = displayName(sender, known)
	}

	// Reply, forum topic and comment thread.
	isForum := chatKnown && chat.IsForum
	var topicID, threadTop, replyTo *int
	if rh, ok := h.replyTo.(*tg.MessageReplyHeader); ok {
		to, _ := rh.GetReplyToMsgID()
		top, hasTop := rh.GetReplyToTopID()
		switch {
		case rh.ForumTopic:
			isForum = true
			if hasTop {
				topicID, replyTo = &top, &to
			} else {
				topicID, replyTo = &to, &to // top-level message in a topic: reply target is the topic root
			}
		default:
			if to != 0 {
				replyTo = &to
			}
			if !isForum {
				if hasTop {
					threadTop = &top
				} else if to != 0 {
					threadTop = &to
				}
			}
		}
	}
	if topicID == nil && isForum {
		general := 1
		topicID = &general
	}

	if sv != nil {
		out.Message.IsService = true
		if sv.Action != nil {
			action := snakeCase(strings.TrimPrefix(fmt.Sprintf("%T", sv.Action), "*tg.MessageAction"))
			out.Message.ServiceAction = &action
		}
		switch a := sv.Action.(type) {
		case *tg.MessageActionTopicCreate:
			id := h.id
			topicID = &id
			out.Message.TopicTitle = nonEmpty(a.Title)
		case *tg.MessageActionTopicEdit:
			out.Message.TopicTitle = nonEmpty(a.Title)
		}
	}
	out.Message.TopicID, out.Message.ThreadTopID, out.Message.ReplyToMsgID = topicID, threadTop, replyTo
	if rd.PeerKind == jobs.PeerChannel { // readDiscussion is channel-only
		switch {
		case topicID != nil && *topicID > 1:
			rd.ThreadID = *topicID
		case threadTop != nil:
			rd.ThreadID = *threadTop
		}
	}

	if m != nil {
		fillContent(&out.Message, m)
	}
	return out, ""
}

func fillContent(dst *message.Message, m *tg.Message) {
	dst.RawText = nonEmpty(m.Message)
	dst.Text = nonEmpty(Markdown(m.Message, m.Entities))
	dst.Entities = EntitiesJSON(m.Entities)
	dst.Views, _ = m.GetViews()
	dst.Forwards, _ = m.GetForwards()
	dst.MediaType = mediaType(m.Media)
	if g, ok := m.GetGroupedID(); ok {
		dst.GroupedID = &g
	}
	if ed, ok := m.GetEditDate(); ok {
		t := time.Unix(int64(ed), 0).UTC()
		dst.UpdatedAt = &t
	}
	if fwd, ok := m.GetFwdFrom(); ok {
		if p, ok := fwd.GetSavedFromPeer(); ok {
			if id, ok := markedID(p); ok {
				dst.FwdFromChatID = &id
				if msgID, ok := fwd.GetSavedFromMsgID(); ok && msgID != 0 {
					dst.FwdFromMsgID = &msgID
				}
			}
		} else if p, ok := fwd.GetFromID(); ok {
			if id, ok := markedID(p); ok {
				dst.FwdFromChatID = &id
				if post, ok := fwd.GetChannelPost(); ok && post != 0 {
					dst.FwdFromMsgID = &post
				}
			}
		}
	}
}

func mediaType(media tg.MessageMediaClass) *string {
	var s string
	switch v := media.(type) {
	case nil, *tg.MessageMediaEmpty:
		return nil
	case *tg.MessageMediaPhoto:
		s = "photo"
	case *tg.MessageMediaDocument:
		s = documentType(v)
	case *tg.MessageMediaWebPage:
		s = "webpage"
	case *tg.MessageMediaGeo, *tg.MessageMediaGeoLive, *tg.MessageMediaVenue:
		s = "geo"
	case *tg.MessageMediaContact:
		s = "contact"
	case *tg.MessageMediaPoll:
		s = "poll"
	case *tg.MessageMediaDice:
		s = "dice"
	default:
		s = "other"
	}
	return &s
}

func documentType(v *tg.MessageMediaDocument) string {
	doc, ok := v.Document.(*tg.Document)
	if !ok {
		return "document"
	}
	var sticker, animated, video, audio, voice bool
	for _, a := range doc.Attributes {
		switch a := a.(type) {
		case *tg.DocumentAttributeSticker:
			sticker = true
		case *tg.DocumentAttributeAnimated:
			animated = true
		case *tg.DocumentAttributeVideo:
			video = true
		case *tg.DocumentAttributeAudio:
			audio, voice = true, a.Voice
		}
	}
	switch {
	case sticker:
		return "sticker"
	case animated:
		return "gif"
	case video:
		return "video"
	case voice:
		return "voice"
	case audio:
		return "audio"
	}
	return "document"
}
