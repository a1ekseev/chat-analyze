package jobs

import "strconv"

// KindBackfillChannel is the queue kind of BackfillChannel jobs.
const KindBackfillChannel = "backfill_channel"

// BackfillMinIDField is the payload field that EnqueueKeepMin preserves
// (the smaller value wins) when a pending backfill is re-enqueued.
const BackfillMinIDField = "min_id"

// BackfillChannel is the payload of a backfill_channel job: fetch the
// channel history newer than MinID and enqueue it as save_message jobs.
// MinID is fixed when the job is first enqueued, so retries never skip the
// gap even after live messages moved the last stored id past it. MinID 0
// means nothing of the channel was stored: only the latest page is fetched.
// The payload holds identifiers only and is safe to log.
type BackfillChannel struct {
	ChannelID int64 `json:"channel_id"`
	MinID     int   `json:"min_id"`
}

// BackfillDedupKey is the queue dedup key of a channel backfill.
func BackfillDedupKey(channelID int64) string { return strconv.FormatInt(channelID, 10) }
