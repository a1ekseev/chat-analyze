package queue

import "time"

const maxBackoff = 10 * time.Minute

// Backoff returns the retry delay for the given attempt number (1-based):
// min(1s * 2^(attempt-1), 10m). attempt < 1 yields 1s.
func Backoff(attempt int) time.Duration {
	if attempt < 1 {
		return time.Second
	}
	if attempt > 20 { // 1s<<20 already exceeds the cap; avoids overflow
		return maxBackoff
	}
	return min(time.Second<<(attempt-1), maxBackoff)
}
