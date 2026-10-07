package telegram

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/gotd/td/constant"
	"github.com/gotd/td/tg"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/a1ekseev/chat-analyze/internal/jobs"
)

// PeerInfo is what the service remembers about a user, chat or channel.
type PeerInfo struct {
	ChatID     int64  // marked id
	Kind       string // jobs.PeerUser | jobs.PeerChat | jobs.PeerChannel
	ID         int64  // raw id
	Name       string // "" when unknown
	Username   string
	AccessHash int64

	IsMin, IsBroadcast, IsForum, IsSelf bool
}

func activeUsername(primary string, all []tg.Username) string {
	if primary != "" {
		return primary
	}
	for _, u := range all {
		if u.Active && u.Username != "" {
			return u.Username
		}
	}
	return ""
}

// PeerFromUser converts a user entity.
func PeerFromUser(u *tg.User) PeerInfo {
	username := activeUsername(u.Username, u.Usernames)
	name := strings.TrimSpace(u.FirstName + " " + u.LastName)
	if name == "" {
		name = username
	}
	return PeerInfo{
		ChatID: u.ID, Kind: jobs.PeerUser, ID: u.ID, Name: name, Username: username,
		AccessHash: u.AccessHash, IsMin: u.Min, IsSelf: u.Self,
	}
}

// PeerFromChat converts a basic group entity.
func PeerFromChat(c *tg.Chat) PeerInfo {
	var id constant.TDLibPeerID
	id.Chat(c.ID)
	return PeerInfo{ChatID: int64(id), Kind: jobs.PeerChat, ID: c.ID, Name: c.Title}
}

// PeerFromChannel converts a channel or supergroup entity.
func PeerFromChannel(c *tg.Channel) PeerInfo {
	var id constant.TDLibPeerID
	id.Channel(c.ID)
	return PeerInfo{
		ChatID: int64(id), Kind: jobs.PeerChannel, ID: c.ID, Name: c.Title,
		Username:   activeUsername(c.Username, c.Usernames),
		AccessHash: c.AccessHash, IsMin: c.Min, IsBroadcast: c.Broadcast, IsForum: c.Forum,
	}
}

// PeersFromEntities converts update entities, keyed by marked chat id.
func PeersFromEntities(users map[int64]*tg.User, chats map[int64]*tg.Chat, channels map[int64]*tg.Channel) map[int64]PeerInfo {
	out := make(map[int64]PeerInfo, len(users)+len(chats)+len(channels))
	for _, u := range users {
		p := PeerFromUser(u)
		out[p.ChatID] = p
	}
	for _, c := range chats {
		p := PeerFromChat(c)
		out[p.ChatID] = p
	}
	for _, c := range channels {
		p := PeerFromChannel(c)
		out[p.ChatID] = p
	}
	return out
}

// PeerCache persists peers and their non-min access hashes in PostgreSQL.
type PeerCache struct {
	pool *pgxpool.Pool
	self atomic.Pointer[PeerInfo]
}

// NewPeerCache returns a PeerCache.
func NewPeerCache(pool *pgxpool.Pool) *PeerCache {
	return &PeerCache{pool: pool}
}

// SetSelf remembers the current account in memory.
func (c *PeerCache) SetSelf(u *tg.User) {
	p := PeerFromUser(u)
	c.self.Store(&p)
}

// Self returns the current account, or a zero PeerInfo before SetSelf.
func (c *PeerCache) Self() PeerInfo {
	if p := c.self.Load(); p != nil {
		return *p
	}
	return PeerInfo{}
}

const (
	sqlUpsertPeer = `
INSERT INTO tg_peers (chat_id, kind, name, username, is_broadcast, is_forum, is_self, updated_at)
VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, ''), $5, $6, $7, now())
ON CONFLICT (chat_id) DO UPDATE SET
  kind = EXCLUDED.kind,
  name = COALESCE(NULLIF(EXCLUDED.name, ''), tg_peers.name),
  username = COALESCE(NULLIF(EXCLUDED.username, ''), tg_peers.username),
  is_broadcast = EXCLUDED.is_broadcast, is_forum = EXCLUDED.is_forum, is_self = EXCLUDED.is_self,
  updated_at = now()`
	sqlInsertMinPeer = `
INSERT INTO tg_peers (chat_id, kind, name, username, is_broadcast, is_forum, is_self, updated_at)
VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, ''), $5, $6, $7, now())
ON CONFLICT (chat_id) DO NOTHING`
	sqlUpsertHash = `
INSERT INTO tg_access_hash (user_id, kind, peer_id, access_hash) VALUES ($1, $2, $3, $4)
ON CONFLICT (user_id, kind, peer_id) DO UPDATE SET access_hash = EXCLUDED.access_hash`
	sqlGetPeer = `
SELECT p.kind, COALESCE(p.name, ''), COALESCE(p.username, ''), p.is_broadcast, p.is_forum, p.is_self,
       COALESCE(h.access_hash, 0)
FROM tg_peers p
LEFT JOIN tg_access_hash h
  ON h.user_id = $2 AND h.kind = p.kind AND h.peer_id = $3 AND p.kind IN ('user', 'channel')
WHERE p.chat_id = $1`
)

// Save upserts peers and their non-min access hashes in one transaction, so it
// is all-or-nothing. Min entities never overwrite an existing row, and their
// hashes are not stored. Empty names/usernames never blank known values.
// Peers are written in chat id order to avoid deadlocks between concurrent calls.
func (c *PeerCache) Save(ctx context.Context, selfID int64, ps map[int64]PeerInfo) error {
	if len(ps) == 0 {
		return nil
	}
	list := make([]PeerInfo, 0, len(ps))
	for _, p := range ps {
		list = append(list, p)
	}
	slices.SortFunc(list, func(a, b PeerInfo) int {
		switch {
		case a.ChatID < b.ChatID:
			return -1
		case a.ChatID > b.ChatID:
			return 1
		}
		return 0
	})
	err := pgx.BeginFunc(ctx, c.pool, func(tx pgx.Tx) error {
		batch := &pgx.Batch{}
		for _, p := range list {
			q := sqlUpsertPeer
			if p.IsMin {
				q = sqlInsertMinPeer
			}
			batch.Queue(q, p.ChatID, p.Kind, p.Name, p.Username, p.IsBroadcast, p.IsForum, p.IsSelf)
			if !p.IsMin && p.AccessHash != 0 && (p.Kind == jobs.PeerUser || p.Kind == jobs.PeerChannel) {
				batch.Queue(sqlUpsertHash, selfID, p.Kind, p.ID, p.AccessHash)
			}
		}
		if err := tx.SendBatch(ctx, batch).Close(); err != nil {
			return fmt.Errorf("write batch: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("save %d peers: %w", len(list), err)
	}
	return nil
}

// Get returns a stored peer; AccessHash comes from tg_access_hash for users
// and channels (0 if absent). IsMin is not persisted and is always false.
func (c *PeerCache) Get(ctx context.Context, selfID, chatID int64) (PeerInfo, bool, error) {
	p := PeerInfo{ChatID: chatID, ID: constant.TDLibPeerID(chatID).ToPlain()}
	err := c.pool.QueryRow(ctx, sqlGetPeer, chatID, selfID, p.ID).
		Scan(&p.Kind, &p.Name, &p.Username, &p.IsBroadcast, &p.IsForum, &p.IsSelf, &p.AccessHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return PeerInfo{}, false, nil
	}
	if err != nil {
		return PeerInfo{}, false, fmt.Errorf("get peer %d: %w", chatID, err)
	}
	return p, true, nil
}
