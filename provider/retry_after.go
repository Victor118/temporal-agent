package provider

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	// maxRetryAfter caps the wait an answer asks for: a turn's next attempt
	// should not wait longer than the retry policy's longest interval.
	maxRetryAfter = 2 * time.Minute
	// absurdRetryAfter is a wait no API means for a single request (a clock
	// far off, a broken proxy): ignored, the retry policy applies.
	absurdRetryAfter = time.Hour
)

// parseRetryAfter reads a Retry-After header: a number of seconds, or an
// HTTP date, compared with now. It reports no wait for a value that is
// missing, malformed, not in the future, or absurd; a long one is capped at
// maxRetryAfter.
func parseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	var d time.Duration
	if secs, err := strconv.ParseInt(value, 10, 64); err == nil {
		if secs <= 0 || secs > int64(absurdRetryAfter/time.Second) {
			return 0, false
		}
		d = time.Duration(secs) * time.Second
	} else if at, err := http.ParseTime(value); err == nil {
		d = at.Sub(now)
		if d <= 0 || d > absurdRetryAfter {
			return 0, false
		}
	} else {
		return 0, false
	}
	return min(d, maxRetryAfter), true
}
