package jobs

import "strconv"

// KindBackfillHistory is the queue kind of BackfillHistory jobs. It replaced
// backfill_channel (migration 00004 converts queued legacy jobs).
const KindBackfillHistory = "backfill_history"

// KindSyncDialogs is the queue kind of the dialog sync pass (payload {}).
const KindSyncDialogs = "sync_dialogs"

// SyncDialogsDedupKey is the single dedup key of the dialog sync job.
const SyncDialogsDedupKey = "all"

// BackfillMinIDField is the payload field that EnqueueKeepMin preserves
// (the smaller value wins) when a pending backfill is re-enqueued.
const BackfillMinIDField = "min_id"

// BackfillHistory is the payload of a backfill_history job: fetch the
// history of any chat (user, group, channel; ChatID is the marked id) newer
// than MinID and enqueue it as save_message jobs.
// MinID is fixed when the job is first enqueued, so retries never skip the
// gap even after live messages moved the last stored id past it. MinID 0
// means nothing of the chat was stored: only the latest page is fetched.
// MaxPages bounds the fetch (0: the backfill default of 1000 pages).
// The payload holds identifiers only and is safe to log.
type BackfillHistory struct {
	ChatID   int64 `json:"chat_id"`
	MinID    int   `json:"min_id"`
	MaxPages int   `json:"max_pages,omitempty"`
}

// BackfillDedupKey is the queue dedup key of a chat backfill (marked id).
func BackfillDedupKey(chatID int64) string { return strconv.FormatInt(chatID, 10) }
