package telegram

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/constant"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/a1ekseev/chat-analyze/internal/jobs"
	"github.com/a1ekseev/chat-analyze/internal/logx"
	"github.com/a1ekseev/chat-analyze/internal/queue"
)

const (
	backfillPageLimit = 100
	backfillMaxPages  = 1000

	// backfillFloodWaitCap bounds the FLOOD_WAIT time one Backfill call
	// spends waiting in total; a longer wait goes back to the queue.
	backfillFloodWaitCap = 30 * time.Minute

	// backfillPagePause spaces history pages: 1000 pages take ~17 minutes
	// plus RTT (worker.BackfillLease leaves room for that and the cap above).
	backfillPagePause = time.Second

	tooLongEnqueueTimeout = 10 * time.Second
)

// Enqueuer puts jobs into the queue.
type Enqueuer interface {
	Enqueue(ctx context.Context, kind, dedupKey string, payload any) (queue.EnqueueResult, error)
	EnqueueKeepMin(ctx context.Context, kind, dedupKey string, payload any, field string) (queue.EnqueueResult, error)
}

// MessageRepo is the part of the message repository the listener needs.
type MessageRepo interface {
	UpdateViews(ctx context.Context, chatID int64, messageID, views int) error
	LastMessageID(ctx context.Context, chatID int64) (int, bool, error)
}

// PeerStore persists peers seen in updates.
type PeerStore interface {
	Save(ctx context.Context, selfID int64, ps map[int64]PeerInfo) error
	Get(ctx context.Context, selfID, chatID int64) (PeerInfo, bool, error)
	Self() PeerInfo
}

// Listener turns Telegram updates into queued save_message jobs and channel
// gaps into backfill_channel jobs.
type Listener struct {
	q     Enqueuer
	repo  MessageRepo
	peers PeerStore
	api   API
	log   *slog.Logger
	qlog  *slog.Logger // component=queue (job enqueued)
	base  atomic.Pointer[context.Context]
	sleep func(context.Context, time.Duration) error // FLOOD_WAIT wait and page pause in Backfill
	pause time.Duration                              // between Backfill pages
	// chat ids already warned about: no full access hash stored
	noHash sync.Map
}

// NewListener returns a Listener.
func NewListener(q Enqueuer, repo MessageRepo, peers PeerStore, api API, logger *slog.Logger) *Listener {
	return &Listener{
		q: q, repo: repo, peers: peers, api: api,
		log:   logger.With(logx.Component("telegram")),
		qlog:  logger.With(logx.Component("queue")),
		sleep: sleepCtx,
		pause: backfillPagePause,
	}
}

// sleepCtx waits d or until ctx is done.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("sleep: %w", ctx.Err())
	case <-t.C:
		return nil
	}
}

// Start remembers the context used by OnChannelTooLong, which gotd calls
// without one.
func (l *Listener) Start(ctx context.Context) { l.base.Store(&ctx) }

func (l *Listener) baseCtx() context.Context {
	if c := l.base.Load(); c != nil {
		return *c
	}
	return context.Background()
}

// guard recovers a handler panic; only the panic type is logged, never its
// value (it may carry message data).
func (l *Listener) guard(ctx context.Context) {
	if r := recover(); r != nil {
		l.log.ErrorContext(ctx, "panic recovered", slog.String("panic", fmt.Sprintf("%T", r)))
	}
}

// Register installs the update handlers. d must be addressable because the
// fallback handler is stored in the dispatcher value itself. Enqueue errors
// are logged by handle and never fail the update.
func (l *Listener) Register(d *tg.UpdateDispatcher) {
	d.OnNewMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateNewMessage) error {
		defer l.guard(ctx)
		_ = l.handle(ctx, e, u.Message, false, u.TypeName())
		return nil
	})
	d.OnNewChannelMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateNewChannelMessage) error {
		defer l.guard(ctx)
		_ = l.handle(ctx, e, u.Message, false, u.TypeName())
		return nil
	})
	d.OnEditMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateEditMessage) error {
		defer l.guard(ctx)
		_ = l.handle(ctx, e, u.Message, true, u.TypeName())
		return nil
	})
	d.OnEditChannelMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateEditChannelMessage) error {
		defer l.guard(ctx)
		_ = l.handle(ctx, e, u.Message, true, u.TypeName())
		return nil
	})
	d.OnChannelMessageViews(func(ctx context.Context, _ tg.Entities, u *tg.UpdateChannelMessageViews) error {
		defer l.guard(ctx)
		chatID := channelChatID(u.ChannelID)
		if uerr := l.repo.UpdateViews(ctx, chatID, u.ID, u.Views); uerr != nil {
			l.log.ErrorContext(ctx, "update views failed", logx.ChatID(chatID), logx.MsgID(u.ID),
				slog.String("error", logx.SanitizeError(uerr)))
		}
		return nil
	})
	d.OnFallback(func(ctx context.Context, _ tg.Entities, u tg.UpdateClass) error {
		defer l.guard(ctx)
		l.log.DebugContext(ctx, "update skipped", slog.String("update_type", updateType(u)),
			slog.String("reason", "unsupported"))
		return nil
	})
}

func updateType(u tg.UpdateClass) string {
	if u == nil {
		return "nil"
	}
	return u.TypeName()
}

// channelChatID is the marked (-100…) chat id of a channel.
func channelChatID(channelID int64) int64 {
	var id constant.TDLibPeerID
	id.Channel(channelID)
	return int64(id)
}

// handle maps one message and enqueues it. The returned enqueue error has
// already been logged.
func (l *Listener) handle(ctx context.Context, e tg.Entities, msg tg.MessageClass, isEdit bool, updType string) error {
	found := PeersFromEntities(e.Users, e.Chats, e.Channels)
	self := l.peers.Self()
	if len(found) > 0 {
		if err := l.peers.Save(ctx, self.ID, found); err != nil {
			l.log.ErrorContext(ctx, "save peers failed", slog.String("error", logx.SanitizeError(err)))
		}
	}
	lookup := func(chatID int64) (PeerInfo, bool) {
		if p, ok := found[chatID]; ok {
			return p, true
		}
		p, ok, err := l.peers.Get(ctx, self.ID, chatID)
		if err != nil || !ok {
			return PeerInfo{}, false
		}
		return p, true
	}
	payload, skip := Map(msg, self, lookup)
	if skip != "" {
		l.log.DebugContext(ctx, "update skipped", slog.String("update_type", updType),
			slog.String("reason", skip), slog.Bool("is_edit", isEdit))
		return nil
	}
	m := payload.Message
	l.fullAccessHash(ctx, self.ID, m.ChatID, &payload.Read)
	l.log.DebugContext(ctx, "update received", slog.String("update_type", updType),
		logx.ChatID(m.ChatID), logx.MsgID(m.MessageID), slog.String("chat_type", m.ChatType),
		slog.Bool("is_edit", isEdit), slog.Bool("is_service", m.IsService))
	res, err := l.q.Enqueue(ctx, jobs.KindSaveMessage, jobs.DedupKey(m.ChatID, m.MessageID), payload)
	if err != nil {
		l.log.ErrorContext(ctx, "enqueue failed", logx.ChatID(m.ChatID), logx.MsgID(m.MessageID),
			slog.String("error", logx.SanitizeError(err)))
		return fmt.Errorf("enqueue message %d:%d: %w", m.ChatID, m.MessageID, err)
	}
	l.jobEnqueued(ctx, jobs.KindSaveMessage, res)
	return nil
}

// fullAccessHash replaces the min (or missing) access hash of a user/channel
// read target with the full one stored for our account: update entities are
// often min constructors, whose hash readHistory rejects. Without a stored
// hash the target stays as is (Reader skips it) and this is warned once per chat.
func (l *Listener) fullAccessHash(ctx context.Context, selfID, chatID int64, t *jobs.ReadTarget) {
	if t.PeerKind != jobs.PeerUser && t.PeerKind != jobs.PeerChannel || !t.IsMin && t.AccessHash != 0 {
		return
	}
	p, ok, err := l.peers.Get(ctx, selfID, chatID)
	switch {
	case err != nil:
		l.log.ErrorContext(ctx, "get peer failed", logx.ChatID(chatID), slog.String("error", logx.SanitizeError(err)))
	case ok && p.AccessHash != 0:
		t.AccessHash, t.IsMin = p.AccessHash, false
	default:
		if _, warned := l.noHash.LoadOrStore(chatID, struct{}{}); !warned {
			l.log.WarnContext(ctx, "mark read skipped", slog.String("reason", "no_full_access_hash"),
				logx.ChatID(chatID), slog.Bool("is_min", t.IsMin))
		}
	}
}

func (l *Listener) jobEnqueued(ctx context.Context, kind string, res queue.EnqueueResult) {
	l.qlog.DebugContext(ctx, "job enqueued", logx.JobID(res.ID), slog.String("job_kind", kind),
		slog.Int("version", res.Version), slog.Bool("inserted", res.Inserted))
}

// OnChannelTooLong is called by gotd when a channel gap cannot be recovered
// through updates (gotd has already moved the channel pts past it). It
// enqueues a durable backfill_channel job with min_id = the last stored
// message id of the channel (0 when none), so FLOOD_WAIT, errors and restarts
// are retried by the queue. Re-enqueueing a pending backfill keeps the
// smaller non-zero min_id (queue.EnqueueKeepMin; 0 never wins over a real
// bound).
func (l *Listener) OnChannelTooLong(channelID int64) {
	// gotd has already moved the pts: the job must be stored even while the
	// service is shutting down, so the base ctx only bounds it in time.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(l.baseCtx()), tooLongEnqueueTimeout)
	defer cancel()
	defer l.guard(ctx)
	chatID := channelChatID(channelID)
	l.log.WarnContext(ctx, "channel too long", logx.ChatID(chatID))

	last, _, err := l.repo.LastMessageID(ctx, chatID)
	if err != nil {
		l.log.ErrorContext(ctx, "backfill enqueue failed", logx.ChatID(chatID),
			slog.String("error", logx.SanitizeError(err)))
		return
	}
	res, err := l.q.EnqueueKeepMin(ctx, jobs.KindBackfillChannel, jobs.BackfillDedupKey(channelID),
		jobs.BackfillChannel{ChannelID: channelID, MinID: last}, jobs.BackfillMinIDField)
	if err != nil {
		l.log.ErrorContext(ctx, "backfill enqueue failed", logx.ChatID(chatID),
			slog.String("error", logx.SanitizeError(err)))
		return
	}
	l.jobEnqueued(ctx, jobs.KindBackfillChannel, res)
}

// Backfill fetches the channel messages newer than minID and enqueues them as
// save_message jobs, oldest first, only after every page was fetched; pages
// are backfillPagePause apart. It is
// the handler of backfill_channel jobs: any error makes the queue retry the
// whole job with the same minID. A FLOOD_WAIT_X is waited out in place (X+1s)
// and the same page is fetched again, up to backfillFloodWaitCap of waiting
// in total; past the cap the FLOOD_WAIT error is returned (see
// tgerr.AsFloodWait) and the queue schedules the retry.
// minID 0 (nothing of the channel stored) fetches only the latest page.
func (l *Listener) Backfill(ctx context.Context, channelID int64, minID int) error {
	started := time.Now()
	chatID := channelChatID(channelID)
	self := l.peers.Self()

	peer, ok, err := l.peers.Get(ctx, self.ID, chatID)
	if err != nil {
		return wrapf(err, "backfill get peer %d", chatID)
	}
	if !ok || peer.AccessHash == 0 {
		l.log.WarnContext(ctx, "backfill skipped", slog.String("reason", "no_access_hash"), logx.ChatID(chatID))
		return nil
	}
	input := &tg.InputPeerChannel{ChannelID: channelID, AccessHash: peer.AccessHash}

	maxPages := backfillMaxPages
	if minID == 0 {
		maxPages = 1
		l.log.WarnContext(ctx, "backfill limited", slog.String("reason", "no_history"), logx.ChatID(chatID),
			slog.Int("max_pages", maxPages))
	}

	type item struct {
		msg tg.MessageClass
		ent tg.Entities
		id  int
	}
	var (
		items     []item
		offset    int
		truncated = true
		waited    time.Duration
	)
	for page := range maxPages {
		if err := ctx.Err(); err != nil {
			return wrapf(err, "backfill %d", chatID)
		}
		if page > 0 && l.pause > 0 {
			if err := l.sleep(ctx, l.pause); err != nil {
				return wrapf(err, "backfill page pause %d", chatID)
			}
		}
		res, err := l.fetchHistory(ctx, chatID, &tg.MessagesGetHistoryRequest{
			Peer: input, OffsetID: offset, MinID: minID, Limit: backfillPageLimit,
		}, &waited)
		if err != nil {
			return err
		}
		msgs, e := historyPage(res)
		smallest := 0
		for _, m := range msgs {
			if mid := messageID(m); mid > minID {
				items = append(items, item{m, e, mid})
				if smallest == 0 || mid < smallest {
					smallest = mid
				}
			}
		}
		if smallest == 0 {
			truncated = false
			break
		}
		offset = smallest
	}
	// Enqueue only after every page was fetched, oldest first.
	slices.SortStableFunc(items, func(x, y item) int { return cmp.Compare(x.id, y.id) })
	for _, it := range items {
		if err := l.handle(ctx, it.ent, it.msg, false, "backfill"); err != nil {
			return wrapf(err, "backfill enqueue %d", chatID)
		}
	}
	attrs := []any{
		logx.ChatID(chatID), slog.Int("from_message_id", minID), slog.Int("fetched", len(items)),
		logx.Duration(time.Since(started)),
	}
	if truncated && minID != 0 {
		l.log.WarnContext(ctx, "backfill truncated", attrs...)
		return nil
	}
	l.log.InfoContext(ctx, "backfill done", attrs...)
	return nil
}

// fetchHistory fetches one history page. A FLOOD_WAIT_X is waited out (X+1s)
// and the same request is repeated while the total wait (*waited) stays
// within backfillFloodWaitCap and the wait ends before ctx's deadline (the
// job lease); otherwise the FLOOD_WAIT error is returned.
func (l *Listener) fetchHistory(ctx context.Context, chatID int64, req *tg.MessagesGetHistoryRequest,
	waited *time.Duration,
) (tg.MessagesMessagesClass, error) {
	for {
		res, err := l.api.MessagesGetHistory(ctx, req)
		if err == nil {
			return res, nil
		}
		d, isFlood := tgerr.AsFloodWait(err)
		wait := d + time.Second
		if !isFlood || *waited+wait > backfillFloodWaitCap || pastDeadline(ctx, wait) {
			return nil, wrapf(err, "backfill get history %d", chatID)
		}
		*waited += wait
		l.log.WarnContext(ctx, "flood wait", logx.ChatID(chatID), slog.Int64("wait_s", int64(d/time.Second)))
		if serr := l.sleep(ctx, wait); serr != nil {
			return nil, wrapf(serr, "backfill flood wait %d", chatID)
		}
	}
}

// pastDeadline reports whether waiting d would reach ctx's deadline.
func pastDeadline(ctx context.Context, d time.Duration) bool {
	dl, ok := ctx.Deadline()
	return ok && time.Until(dl) <= d
}

func messageID(m tg.MessageClass) int {
	switch v := m.(type) {
	case *tg.Message:
		return v.ID
	case *tg.MessageService:
		return v.ID
	case *tg.MessageEmpty:
		return v.ID
	}
	return 0
}

// historyPage extracts messages and entities from a getHistory response.
func historyPage(res tg.MessagesMessagesClass) ([]tg.MessageClass, tg.Entities) {
	var (
		msgs  []tg.MessageClass
		users []tg.UserClass
		chats []tg.ChatClass
	)
	switch v := res.(type) {
	case *tg.MessagesMessages:
		msgs, users, chats = v.Messages, v.Users, v.Chats
	case *tg.MessagesMessagesSlice:
		msgs, users, chats = v.Messages, v.Users, v.Chats
	case *tg.MessagesChannelMessages:
		msgs, users, chats = v.Messages, v.Users, v.Chats
	default: // MessagesMessagesNotModified or nil
		return nil, tg.Entities{}
	}
	return msgs, tg.Entities{
		Users:    tg.UserClassArray(users).NotEmptyToMap(),
		Chats:    tg.ChatClassArray(chats).ChatToMap(),
		Channels: tg.ChatClassArray(chats).ChannelToMap(),
	}
}

func wrapf(err error, format string, args ...any) error {
	return fmt.Errorf(format+": %w", append(args, err)...)
}
