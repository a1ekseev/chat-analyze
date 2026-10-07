package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/telegram/updates"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	_ session.Storage             = (*SessionStorage)(nil)
	_ updates.StateStorage        = (*StateStorage)(nil)
	_ updates.ChannelAccessHasher = (*StateStorage)(nil)
	_ updates.UserAccessHasher    = (*StateStorage)(nil)
	_ peers.Storage               = (*PeerKV)(nil)
)

// SessionStorage stores the gotd session blob in tg_session.
type SessionStorage struct {
	pool *pgxpool.Pool
	name string
}

// NewSessionStorage returns a session.Storage keyed by name.
func NewSessionStorage(pool *pgxpool.Pool, name string) *SessionStorage {
	return &SessionStorage{pool: pool, name: name}
}

const sqlLoadSession = `SELECT data FROM tg_session WHERE name = $1`

// LoadSession implements session.Storage; a missing row is session.ErrNotFound.
func (s *SessionStorage) LoadSession(ctx context.Context) ([]byte, error) {
	var data []byte
	err := s.pool.QueryRow(ctx, sqlLoadSession, s.name).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, session.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("load telegram session: %w", err)
	}
	if len(data) == 0 {
		return nil, session.ErrNotFound
	}
	return data, nil
}

const sqlStoreSession = `
INSERT INTO tg_session (name, data, updated_at) VALUES ($1, $2, now())
ON CONFLICT (name) DO UPDATE SET data = EXCLUDED.data, updated_at = now()`

// StoreSession implements session.Storage.
func (s *SessionStorage) StoreSession(ctx context.Context, data []byte) error {
	if _, err := s.pool.Exec(ctx, sqlStoreSession, s.name, data); err != nil {
		return fmt.Errorf("store telegram session: %w", err)
	}
	return nil
}

// StateStorage keeps gotd updates state, channel pts and access hashes.
type StateStorage struct {
	pool *pgxpool.Pool
}

// NewStateStorage returns the PostgreSQL updates storage.
func NewStateStorage(pool *pgxpool.Pool) *StateStorage {
	return &StateStorage{pool: pool}
}

const sqlGetState = `SELECT pts, qts, date, seq FROM tg_update_state WHERE user_id = $1`

// GetState implements updates.StateStorage.
func (s *StateStorage) GetState(ctx context.Context, userID int64) (updates.State, bool, error) {
	var st updates.State
	err := s.pool.QueryRow(ctx, sqlGetState, userID).Scan(&st.Pts, &st.Qts, &st.Date, &st.Seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return updates.State{}, false, nil
	}
	if err != nil {
		return updates.State{}, false, fmt.Errorf("get update state for user %d: %w", userID, err)
	}
	return st, true, nil
}

const (
	sqlSetState = `
INSERT INTO tg_update_state (user_id, pts, qts, date, seq) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (user_id) DO UPDATE SET pts = EXCLUDED.pts, qts = EXCLUDED.qts, date = EXCLUDED.date, seq = EXCLUDED.seq`
	sqlClearChannelPts = `DELETE FROM tg_channel_pts WHERE user_id = $1`
)

// SetState implements updates.StateStorage. Like gotd's in-memory storage it
// resets the user's channel pts, in the same transaction.
func (s *StateStorage) SetState(ctx context.Context, userID int64, st updates.State) error {
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, sqlSetState, userID, st.Pts, st.Qts, st.Date, st.Seq); err != nil {
			return fmt.Errorf("upsert: %w", err)
		}
		if _, err := tx.Exec(ctx, sqlClearChannelPts, userID); err != nil {
			return fmt.Errorf("clear channel pts: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("set update state for user %d: %w", userID, err)
	}
	return nil
}

// updateState runs an UPDATE of tg_update_state and fails if no row matched.
func (s *StateStorage) updateState(ctx context.Context, userID int64, sql string, args ...any) error {
	tag, err := s.pool.Exec(ctx, sql, append([]any{userID}, args...)...)
	if err != nil {
		return fmt.Errorf("update state for user %d: %w", userID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("state for user %d not found", userID)
	}
	return nil
}

// SetPts implements updates.StateStorage.
func (s *StateStorage) SetPts(ctx context.Context, userID int64, pts int) error {
	return s.updateState(ctx, userID, `UPDATE tg_update_state SET pts = $2 WHERE user_id = $1`, pts)
}

// SetQts implements updates.StateStorage.
func (s *StateStorage) SetQts(ctx context.Context, userID int64, qts int) error {
	return s.updateState(ctx, userID, `UPDATE tg_update_state SET qts = $2 WHERE user_id = $1`, qts)
}

// SetDate implements updates.StateStorage.
func (s *StateStorage) SetDate(ctx context.Context, userID int64, date int) error {
	return s.updateState(ctx, userID, `UPDATE tg_update_state SET date = $2 WHERE user_id = $1`, date)
}

// SetSeq implements updates.StateStorage.
func (s *StateStorage) SetSeq(ctx context.Context, userID int64, seq int) error {
	return s.updateState(ctx, userID, `UPDATE tg_update_state SET seq = $2 WHERE user_id = $1`, seq)
}

// SetDateSeq implements updates.StateStorage.
func (s *StateStorage) SetDateSeq(ctx context.Context, userID int64, date, seq int) error {
	return s.updateState(ctx, userID, `UPDATE tg_update_state SET date = $2, seq = $3 WHERE user_id = $1`, date, seq)
}

const sqlGetChannelPts = `SELECT pts FROM tg_channel_pts WHERE user_id = $1 AND channel_id = $2`

// GetChannelPts implements updates.StateStorage.
func (s *StateStorage) GetChannelPts(ctx context.Context, userID, channelID int64) (int, bool, error) {
	var pts int
	err := s.pool.QueryRow(ctx, sqlGetChannelPts, userID, channelID).Scan(&pts)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("get channel pts for user %d channel %d: %w", userID, channelID, err)
	}
	return pts, true, nil
}

const sqlSetChannelPts = `
INSERT INTO tg_channel_pts (user_id, channel_id, pts) VALUES ($1, $2, $3)
ON CONFLICT (user_id, channel_id) DO UPDATE SET pts = EXCLUDED.pts`

// SetChannelPts implements updates.StateStorage.
func (s *StateStorage) SetChannelPts(ctx context.Context, userID, channelID int64, pts int) error {
	if _, err := s.pool.Exec(ctx, sqlSetChannelPts, userID, channelID, pts); err != nil {
		return fmt.Errorf("set channel pts for user %d channel %d: %w", userID, channelID, err)
	}
	return nil
}

const sqlListChannelPts = `SELECT channel_id, pts FROM tg_channel_pts WHERE user_id = $1 ORDER BY channel_id`

type channelPts struct {
	id  int64
	pts int
}

// ForEachChannels implements updates.StateStorage. Rows are read fully before
// f runs so f may use the storage without holding a connection.
func (s *StateStorage) ForEachChannels(ctx context.Context, userID int64, f func(ctx context.Context, channelID int64, pts int) error) error {
	rows, err := s.pool.Query(ctx, sqlListChannelPts, userID)
	if err != nil {
		return fmt.Errorf("list channel pts for user %d: %w", userID, err)
	}
	items, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (channelPts, error) {
		var c channelPts
		if err := r.Scan(&c.id, &c.pts); err != nil {
			return c, fmt.Errorf("scan channel pts: %w", err)
		}
		return c, nil
	})
	if err != nil {
		return fmt.Errorf("list channel pts for user %d: %w", userID, err)
	}
	for _, c := range items {
		if err := f(ctx, c.id, c.pts); err != nil {
			return err
		}
	}
	return nil
}

const (
	kindUser    = "user"
	kindChannel = "channel"

	sqlGetAccessHash = `SELECT access_hash FROM tg_access_hash WHERE user_id = $1 AND kind = $2 AND peer_id = $3`
	sqlSetAccessHash = `
INSERT INTO tg_access_hash (user_id, kind, peer_id, access_hash) VALUES ($1, $2, $3, $4)
ON CONFLICT (user_id, kind, peer_id) DO UPDATE SET access_hash = EXCLUDED.access_hash`
)

func (s *StateStorage) getHash(ctx context.Context, userID int64, kind string, peerID int64) (int64, bool, error) {
	var h int64
	err := s.pool.QueryRow(ctx, sqlGetAccessHash, userID, kind, peerID).Scan(&h)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("get %s access hash for user %d peer %d: %w", kind, userID, peerID, err)
	}
	return h, true, nil
}

func (s *StateStorage) setHash(ctx context.Context, userID int64, kind string, peerID, hash int64) error {
	if _, err := s.pool.Exec(ctx, sqlSetAccessHash, userID, kind, peerID, hash); err != nil {
		return fmt.Errorf("set %s access hash for user %d peer %d: %w", kind, userID, peerID, err)
	}
	return nil
}

// SetChannelAccessHash implements updates.ChannelAccessHasher.
func (s *StateStorage) SetChannelAccessHash(ctx context.Context, userID, channelID, accessHash int64) error {
	return s.setHash(ctx, userID, kindChannel, channelID, accessHash)
}

// GetChannelAccessHash implements updates.ChannelAccessHasher.
func (s *StateStorage) GetChannelAccessHash(ctx context.Context, userID, channelID int64) (int64, bool, error) {
	return s.getHash(ctx, userID, kindChannel, channelID)
}

// SetUserAccessHash implements updates.UserAccessHasher.
func (s *StateStorage) SetUserAccessHash(ctx context.Context, userID, targetUserID, accessHash int64) error {
	return s.setHash(ctx, userID, kindUser, targetUserID, accessHash)
}

// GetUserAccessHash implements updates.UserAccessHasher.
func (s *StateStorage) GetUserAccessHash(ctx context.Context, userID, targetUserID int64) (int64, bool, error) {
	return s.getHash(ctx, userID, kindUser, targetUserID)
}

// PeerKV implements peers.Storage over tg_peer_kv (JSONB values).
type PeerKV struct {
	pool *pgxpool.Pool
}

// NewPeerKV returns the PostgreSQL peers.Storage.
func NewPeerKV(pool *pgxpool.Pool) *PeerKV {
	return &PeerKV{pool: pool}
}

const (
	keyContactsHash = "contacts_hash"

	sqlGetKV = `SELECT value FROM tg_peer_kv WHERE key = $1`
	sqlSetKV = `
INSERT INTO tg_peer_kv (key, value) VALUES ($1, $2)
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`
)

func peerKey(k peers.Key) string { return "peer:" + k.Prefix + strconv.FormatInt(k.ID, 10) }

func (p *PeerKV) get(ctx context.Context, key string, dst any) (bool, error) {
	var raw []byte
	err := p.pool.QueryRow(ctx, sqlGetKV, key).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("get peer kv: %w", err)
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return false, errors.New("decode peer kv value")
	}
	return true, nil
}

func (p *PeerKV) set(ctx context.Context, key string, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return errors.New("encode peer kv value")
	}
	if _, err := p.pool.Exec(ctx, sqlSetKV, key, raw); err != nil {
		return fmt.Errorf("set peer kv: %w", err)
	}
	return nil
}

// Save implements peers.Storage.
func (p *PeerKV) Save(ctx context.Context, key peers.Key, value peers.Value) error {
	return p.set(ctx, peerKey(key), value)
}

// Find implements peers.Storage.
func (p *PeerKV) Find(ctx context.Context, key peers.Key) (peers.Value, bool, error) {
	var v peers.Value
	found, err := p.get(ctx, peerKey(key), &v)
	if err != nil || !found {
		return peers.Value{}, false, err
	}
	return v, true, nil
}

// SavePhone implements peers.Storage: only the phone -> key mapping is stored.
func (p *PeerKV) SavePhone(ctx context.Context, phone string, key peers.Key) error {
	return p.set(ctx, "phone:"+phone, key)
}

// FindPhone implements peers.Storage: it resolves the key, then the peer's
// current value; an unsaved peer value means not found.
func (p *PeerKV) FindPhone(ctx context.Context, phone string) (peers.Key, peers.Value, bool, error) {
	var k peers.Key
	found, err := p.get(ctx, "phone:"+phone, &k)
	if err != nil || !found {
		return peers.Key{}, peers.Value{}, false, err
	}
	v, found, err := p.Find(ctx, k)
	if err != nil || !found {
		return peers.Key{}, peers.Value{}, false, err
	}
	return k, v, true, nil
}

// GetContactsHash implements peers.Storage; 0 when unset.
func (p *PeerKV) GetContactsHash(ctx context.Context) (int64, error) {
	var h int64
	if _, err := p.get(ctx, keyContactsHash, &h); err != nil {
		return 0, err
	}
	return h, nil
}

// SaveContactsHash implements peers.Storage.
func (p *PeerKV) SaveContactsHash(ctx context.Context, hash int64) error {
	return p.set(ctx, keyContactsHash, hash)
}
