-- +goose Up
CREATE TABLE tg_session (name TEXT PRIMARY KEY, data BYTEA NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE tg_update_state (user_id BIGINT PRIMARY KEY, pts INT NOT NULL, qts INT NOT NULL, date INT NOT NULL, seq INT NOT NULL);
CREATE TABLE tg_channel_pts (user_id BIGINT NOT NULL, channel_id BIGINT NOT NULL, pts INT NOT NULL, PRIMARY KEY (user_id, channel_id));
CREATE TABLE tg_access_hash (user_id BIGINT NOT NULL, kind TEXT NOT NULL CHECK (kind IN ('user','channel')), peer_id BIGINT NOT NULL, access_hash BIGINT NOT NULL, PRIMARY KEY (user_id, kind, peer_id));
CREATE TABLE tg_peer_kv (key TEXT PRIMARY KEY, value JSONB NOT NULL);          -- peers.Storage (Key prefix+id, phone, contacts hash)
CREATE TABLE tg_peers (
  chat_id      BIGINT PRIMARY KEY,               -- marked id
  kind         TEXT   NOT NULL CHECK (kind IN ('user','chat','channel')),
  name         TEXT,
  username     TEXT,
  is_broadcast BOOLEAN NOT NULL DEFAULT false,
  is_forum     BOOLEAN NOT NULL DEFAULT false,
  is_self      BOOLEAN NOT NULL DEFAULT false,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE tg_forum_topics (chat_id BIGINT NOT NULL, topic_id INT NOT NULL, title TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (chat_id, topic_id));
CREATE TABLE tg_read_marks (chat_id BIGINT NOT NULL, thread_id INT NOT NULL DEFAULT 0, max_id INT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (chat_id, thread_id));

-- +goose Down
DROP TABLE tg_read_marks;
DROP TABLE tg_forum_topics;
DROP TABLE tg_peers;
DROP TABLE tg_peer_kv;
DROP TABLE tg_access_hash;
DROP TABLE tg_channel_pts;
DROP TABLE tg_update_state;
DROP TABLE tg_session;
