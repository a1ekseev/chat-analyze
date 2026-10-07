-- +goose Up
CREATE TABLE messages (
  chat_id          BIGINT      NOT NULL,
  message_id       INT         NOT NULL,
  chat_type        TEXT        NOT NULL CHECK (chat_type IN ('channel','group','private_chat')),
  chat_name        TEXT,
  chat_username    TEXT,
  sender_id        BIGINT,
  sender_name      TEXT,
  out              BOOLEAN     NOT NULL DEFAULT false,
  post             BOOLEAN     NOT NULL DEFAULT false,
  message          TEXT,
  raw_text         TEXT,
  entities         JSONB,
  media_type       TEXT,
  views            INT         NOT NULL DEFAULT 0,
  forwards         INT         NOT NULL DEFAULT 0,
  grouped_id       BIGINT,
  reply_to_msg_id  INT,
  topic_id         INT,
  thread_top_id    INT,
  fwd_from_chat_id BIGINT,
  fwd_from_msg_id  INT,
  is_service       BOOLEAN     NOT NULL DEFAULT false,
  service_action   TEXT,
  created_at       TIMESTAMPTZ NOT NULL,
  updated_at       TIMESTAMPTZ,
  stored_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (chat_id, message_id)
);
CREATE INDEX messages_created_at_idx ON messages (created_at);
CREATE INDEX messages_chat_topic_idx ON messages (chat_id, topic_id) WHERE topic_id IS NOT NULL;
CREATE INDEX messages_chat_thread_idx ON messages (chat_id, thread_top_id) WHERE thread_top_id IS NOT NULL;

-- +goose Down
DROP TABLE messages;
