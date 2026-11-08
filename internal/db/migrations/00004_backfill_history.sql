-- +goose Up
-- backfill_channel {channel_id, min_id} → backfill_history {chat_id (marked), min_id}.
UPDATE jobs SET kind = 'backfill_history',
  dedup_key = (-1000000000000 - (payload ->> 'channel_id')::bigint)::text,
  payload = jsonb_build_object(
    'chat_id', -1000000000000 - (payload ->> 'channel_id')::bigint,
    'min_id', COALESCE((payload ->> 'min_id')::bigint, 0))
WHERE kind = 'backfill_channel';

-- +goose Down
DELETE FROM jobs WHERE kind = 'sync_dialogs'
   OR (kind = 'backfill_history' AND (payload ->> 'chat_id')::bigint >= -1000000000000);
UPDATE jobs SET kind = 'backfill_channel',
  dedup_key = (-1000000000000 - (payload ->> 'chat_id')::bigint)::text,
  payload = jsonb_build_object(
    'channel_id', -1000000000000 - (payload ->> 'chat_id')::bigint,
    'min_id', COALESCE((payload ->> 'min_id')::bigint, 0))
WHERE kind = 'backfill_history';
