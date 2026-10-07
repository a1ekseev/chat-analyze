-- +goose Up
CREATE TABLE jobs (
  id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  kind         TEXT        NOT NULL,
  dedup_key    TEXT        NOT NULL,
  payload      JSONB       NOT NULL,
  status       TEXT        NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','running','done','dead')),
  version      INT         NOT NULL DEFAULT 1,
  attempts     INT         NOT NULL DEFAULT 0,
  max_attempts INT         NOT NULL,
  run_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  locked_until TIMESTAMPTZ,
  last_error   TEXT,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (kind, dedup_key)
);
CREATE INDEX jobs_pending_idx ON jobs (kind, run_at) WHERE status = 'pending';
CREATE INDEX jobs_running_idx ON jobs (locked_until) WHERE status = 'running';

-- +goose Down
DROP TABLE jobs;
