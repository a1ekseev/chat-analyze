package telegram

import (
	"context"
	"log/slog"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/a1ekseev/chat-analyze/internal/jobs"
	"github.com/a1ekseev/chat-analyze/internal/logx"
)

const (
	dialogsPageLimit = 100
	// dialogsMaxPages bounds one folder (10 000 dialogs).
	dialogsMaxPages = 100
	topicsPageLimit = 100
	topicsMaxPages  = 10
	// syncUnreadMaxPages caps the unread messages fetched for a chat with no
	// stored history (1000 messages, newest first).
	syncUnreadMaxPages = 10
	archiveFolderID    = 1
)

// ReadMarker records messages to be marked read (Reader.MarkRead).
type ReadMarker interface {
	MarkRead(ctx context.Context, chatID int64, t jobs.ReadTarget, maxID int) error
}

// DialogSync brings the stored messages and the read state of every dialog
// (main list and archive) in line with Telegram: chats behind get a
// backfill_history job, chats with nothing to fetch but unread messages are
// marked read. It is the handler of the sync_dialogs job.
type DialogSync struct {
	l        *Listener
	reader   ReadMarker
	markRead bool
}

// NewDialogSync returns a DialogSync that uses the listener's queue, stores,
// API and pacing; with markRead false it only fetches.
func NewDialogSync(l *Listener, reader ReadMarker, markRead bool) *DialogSync {
	return &DialogSync{l: l, reader: reader, markRead: markRead}
}

// syncPass counts one pass for its summary log.
type syncPass struct {
	pages, dialogs, skipped, behind, msgsBehind, marked int
}

// SyncDialogs runs one pass. A pass where nothing is missing costs only the
// getDialogs pages (1s apart); per-dialog RPCs are made only for dialogs
// with unread state. Errors (FLOOD_WAIT, database) fail the pass, which is
// safe to re-run: enqueues are deduplicated and read marks never go back.
func (s *DialogSync) SyncDialogs(ctx context.Context) error {
	start := time.Now()
	p := &syncPass{}
	for _, folder := range []int{0, archiveFolderID} {
		if err := s.folder(ctx, folder, p); err != nil {
			return err
		}
	}
	s.l.log.InfoContext(ctx, "dialog sync done",
		slog.Int("dialogs", p.dialogs), slog.Int("skipped", p.skipped),
		slog.Int("chats_behind", p.behind), slog.Int("messages_behind", p.msgsBehind),
		slog.Int("chats_marked_read", p.marked), slog.Int("pages", p.pages),
		logx.Duration(time.Since(start)))
	return nil
}

// paced spaces paged RPCs (getDialogs, getForumTopics) like backfill pages.
func (s *DialogSync) paced(ctx context.Context, p *syncPass) error {
	if p.pages > 0 && s.l.pause > 0 {
		if err := s.l.sleep(ctx, s.l.pause); err != nil {
			return wrapf(err, "dialog sync pause")
		}
	}
	p.pages++
	return nil
}

func (s *DialogSync) folder(ctx context.Context, folder int, p *syncPass) error {
	self := s.l.peers.Self()
	req := &tg.MessagesGetDialogsRequest{FolderID: folder, OffsetPeer: &tg.InputPeerEmpty{}, Limit: dialogsPageLimit}
	seen := map[int64]bool{}
	for range dialogsMaxPages {
		if err := s.paced(ctx, p); err != nil {
			return err
		}
		res, err := s.l.api.MessagesGetDialogs(ctx, req)
		if err != nil {
			return wrapf(err, "get dialogs folder %d", folder)
		}
		var (
			dialogs []tg.DialogClass
			msgs    []tg.MessageClass
			users   []tg.UserClass
			chats   []tg.ChatClass
			last    bool
		)
		switch v := res.(type) {
		case *tg.MessagesDialogs:
			dialogs, msgs, users, chats, last = v.Dialogs, v.Messages, v.Users, v.Chats, true
		case *tg.MessagesDialogsSlice:
			dialogs, msgs, users, chats = v.Dialogs, v.Messages, v.Users, v.Chats
		default: // not modified
			return nil
		}
		ents := entitiesOf(users, chats)
		found := PeersFromEntities(ents.Users, ents.Chats, ents.Channels)
		if err := s.l.peers.Save(ctx, self.ID, found); err != nil {
			return wrapf(err, "dialog sync save peers")
		}
		var tail *tg.Dialog
		fresh := 0
		for _, dc := range dialogs {
			d, ok := dc.(*tg.Dialog)
			if !ok {
				continue // dialogFolder: the archive entry of the main list
			}
			tail = d
			chatID, ok := markedID(d.Peer)
			if !ok || seen[chatID] {
				continue
			}
			seen[chatID] = true
			fresh++
			if err := s.dialog(ctx, d, chatID, ents, found, p); err != nil {
				return err
			}
		}
		if last || len(dialogs) < dialogsPageLimit || fresh == 0 || tail == nil {
			return nil
		}
		tailID, _ := markedID(tail.Peer)
		off, ok := inputPeer(found[tailID])
		if !ok {
			s.l.log.WarnContext(ctx, "dialog sync stopped", slog.String("reason", "no_offset_peer"), slog.Int("folder", folder))
			return nil
		}
		req.OffsetPeer, req.OffsetID, req.OffsetDate = off, tail.TopMessage, messageDate(msgs, tailID, tail.TopMessage)
	}
	s.l.log.WarnContext(ctx, "dialog sync truncated", slog.Int("folder", folder), slog.Int("max_pages", dialogsMaxPages))
	return nil
}

func (s *DialogSync) dialog(ctx context.Context, d *tg.Dialog, chatID int64, ents tg.Entities,
	found map[int64]PeerInfo, p *syncPass,
) error {
	p.dialogs++
	peer, reason, err := s.dialogPeer(ctx, d, chatID, ents, found)
	if err != nil {
		return err
	}
	if reason != "" {
		p.skipped++
		s.l.log.DebugContext(ctx, "dialog skipped", logx.ChatID(chatID), slog.String("reason", reason))
		return nil
	}
	input, _ := inputPeer(peer) // dialogPeer checked it
	stored, has, err := s.l.repo.LastMessageID(ctx, chatID)
	if err != nil {
		return wrapf(err, "dialog sync last message %d", chatID)
	}

	fetch := jobs.BackfillHistory{ChatID: chatID}
	fetching := false
	switch {
	case has && d.TopMessage > stored:
		fetching, fetch.MinID = true, stored
		p.msgsBehind += d.TopMessage - stored
	case !has && d.UnreadCount > 0:
		// No history: fetch only the unread messages, never older history.
		n := min(d.UnreadCount, syncUnreadMaxPages*backfillPageLimit)
		fetching, fetch.MinID = true, d.ReadInboxMaxID
		fetch.MaxPages = (n + backfillPageLimit - 1) / backfillPageLimit
		p.msgsBehind += n
	}
	if fetching {
		p.behind++
		res, err := s.l.q.EnqueueKeepMin(ctx, jobs.KindBackfillHistory, jobs.BackfillDedupKey(chatID), fetch,
			jobs.BackfillMinIDField)
		if err != nil {
			return wrapf(err, "dialog sync enqueue backfill %d", chatID)
		}
		s.l.jobEnqueued(ctx, jobs.KindBackfillHistory, res)
	}
	if !s.markRead {
		return nil
	}
	marked, err := s.markDialog(ctx, d, peer, input, fetching, stored, has, p)
	if marked {
		p.marked++
	}
	return err
}

// dialogPeer returns the peer of a dialog, or why it is skipped: chats the
// account left or was kicked from (ChatForbidden/ChannelForbidden, left,
// deactivated) and peers without a full access hash.
func (s *DialogSync) dialogPeer(ctx context.Context, d *tg.Dialog, chatID int64, ents tg.Entities,
	found map[int64]PeerInfo,
) (PeerInfo, string, error) {
	switch v := d.Peer.(type) {
	case *tg.PeerChat:
		c, ok := ents.Chats[v.ChatID]
		if !ok {
			return PeerInfo{}, "forbidden", nil
		}
		if c.Left || c.Deactivated {
			return PeerInfo{}, "left", nil
		}
	case *tg.PeerChannel:
		c, ok := ents.Channels[v.ChannelID]
		if !ok {
			return PeerInfo{}, "forbidden", nil
		}
		if c.Left {
			return PeerInfo{}, "left", nil
		}
	case *tg.PeerUser:
		if _, ok := ents.Users[v.UserID]; !ok {
			return PeerInfo{}, SkipNoPeer, nil
		}
	}
	p := found[chatID]
	if _, ok := inputPeer(p); ok && !p.IsMin {
		return p, "", nil
	}
	// A min entity (or one without a hash): use the stored full hash.
	stored, ok, err := s.l.peers.Get(ctx, s.l.peers.Self().ID, chatID)
	if err != nil {
		return PeerInfo{}, "", wrapf(err, "dialog sync get peer %d", chatID)
	}
	if !ok || stored.AccessHash == 0 {
		return PeerInfo{}, "no_access_hash", nil
	}
	p.AccessHash, p.IsMin = stored.AccessHash, false
	return p, "", nil
}

// markDialog clears the unread state of a dialog. Only stored messages are
// marked here: what a backfill still fetches is marked by its save_message
// jobs. It reports whether anything was marked.
func (s *DialogSync) markDialog(ctx context.Context, d *tg.Dialog, peer PeerInfo, input tg.InputPeerClass,
	fetching bool, stored int, has bool, p *syncPass,
) (bool, error) {
	chatID := peer.ChatID
	marked := false
	// unread_count, not read_inbox_max_id < top_message: the latter also
	// holds when the last message is outgoing, and would cost one useless
	// readHistory per such chat.
	if !fetching && d.UnreadCount > 0 {
		if err := s.reader.MarkRead(ctx, chatID, readTarget(peer), d.TopMessage); err != nil {
			return false, wrapf(err, "dialog sync mark read %d", chatID)
		}
		marked = true
	}
	if d.UnreadMark {
		_, err := s.l.api.MessagesMarkDialogUnread(ctx, &tg.MessagesMarkDialogUnreadRequest{
			Peer: &tg.InputDialogPeer{Peer: input},
		})
		if err := s.callDone(ctx, "messages.markDialogUnread", chatID, err); err != nil {
			return marked, err
		}
		marked = true
	}
	if !fetching && d.UnreadMentionsCount > 0 {
		_, err := s.l.api.MessagesReadMentions(ctx, &tg.MessagesReadMentionsRequest{Peer: input})
		if err := s.callDone(ctx, "messages.readMentions", chatID, err); err != nil {
			return marked, err
		}
	}
	if !fetching && d.UnreadReactionsCount > 0 {
		_, err := s.l.api.MessagesReadReactions(ctx, &tg.MessagesReadReactionsRequest{Peer: input})
		if err := s.callDone(ctx, "messages.readReactions", chatID, err); err != nil {
			return marked, err
		}
	}
	if peer.IsForum && has && d.UnreadCount > 0 {
		n, err := s.topics(ctx, peer, input, stored, p)
		if n > 0 {
			marked = true
		}
		if err != nil {
			return marked, err
		}
	}
	return marked, nil
}

// topics marks read the unread forum topics whose messages are all stored
// (top_message <= stored); topics with newer messages are marked by the
// save_message jobs of the chat backfill. Topic 1 (General) has no thread:
// it is read with the chat. It returns the number of topics marked.
func (s *DialogSync) topics(ctx context.Context, peer PeerInfo, input tg.InputPeerClass, stored int, p *syncPass) (int, error) {
	req := &tg.MessagesGetForumTopicsRequest{Peer: input, Limit: topicsPageLimit}
	seen := map[int]bool{}
	marked := 0
	for range topicsMaxPages {
		if err := s.paced(ctx, p); err != nil {
			return marked, err
		}
		res, err := s.l.api.MessagesGetForumTopics(ctx, req)
		if err := s.callDone(ctx, "messages.getForumTopics", peer.ChatID, err); err != nil || res == nil {
			return marked, err
		}
		var tail *tg.ForumTopic
		fresh := 0
		for _, tc := range res.Topics {
			t, ok := tc.(*tg.ForumTopic)
			if !ok {
				continue // forumTopicDeleted
			}
			tail = t
			if seen[t.ID] {
				continue
			}
			seen[t.ID] = true
			fresh++
			if t.TopMessage > stored || (t.UnreadCount == 0 && t.ReadInboxMaxID >= t.TopMessage) {
				continue
			}
			target := readTarget(peer)
			if t.ID > 1 {
				target.ThreadID = t.ID
			}
			if err := s.reader.MarkRead(ctx, peer.ChatID, target, t.TopMessage); err != nil {
				return marked, wrapf(err, "dialog sync mark topic read %d", peer.ChatID)
			}
			marked++
		}
		if len(res.Topics) < topicsPageLimit || fresh == 0 || tail == nil {
			return marked, nil
		}
		req.OffsetTopic, req.OffsetID = tail.ID, tail.TopMessage
		req.OffsetDate = messageDate(res.Messages, peer.ChatID, tail.TopMessage)
	}
	return marked, nil
}

// callDone handles the error of a per-dialog RPC: FLOOD_WAIT and shutdown
// fail the pass (the queue retries it), anything else is logged and the
// pass goes on (the next pass retries that dialog).
func (s *DialogSync) callDone(ctx context.Context, method string, chatID int64, err error) error {
	if err == nil {
		return nil
	}
	if _, flood := tgerr.AsFloodWait(err); flood || ctx.Err() != nil {
		return wrapRPC(method, chatID, 0, err)
	}
	s.l.log.WarnContext(ctx, "dialog sync call failed", logx.ChatID(chatID), slog.String("method", method), logx.Err(err))
	return nil
}

// readTarget is the chat-level ReadTarget of p (as Map builds it).
func readTarget(p PeerInfo) jobs.ReadTarget {
	if p.IsSelf {
		return jobs.ReadTarget{PeerKind: jobs.PeerSelf, PeerID: p.ID}
	}
	return jobs.ReadTarget{PeerKind: p.Kind, PeerID: p.ID, AccessHash: p.AccessHash}
}

func entitiesOf(users []tg.UserClass, chats []tg.ChatClass) tg.Entities {
	return tg.Entities{
		Users:    tg.UserClassArray(users).NotEmptyToMap(),
		Chats:    tg.ChatClassArray(chats).ChatToMap(),
		Channels: tg.ChatClassArray(chats).ChannelToMap(),
	}
}

// messageDate is the date of message id of chatID in msgs (0 if absent),
// the offset_date of the next dialogs or topics page.
func messageDate(msgs []tg.MessageClass, chatID int64, id int) int {
	for _, m := range msgs {
		var (
			peer tg.PeerClass
			date int
		)
		switch v := m.(type) {
		case *tg.Message:
			peer, date = v.PeerID, v.Date
		case *tg.MessageService:
			peer, date = v.PeerID, v.Date
		default:
			continue
		}
		if c, ok := markedID(peer); ok && c == chatID && messageID(m) == id {
			return date
		}
	}
	return 0
}
