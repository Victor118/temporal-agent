package provider

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	date := func(d time.Duration) string { return now.Add(d).Format(http.TimeFormat) }
	for _, c := range []struct {
		value string
		want  time.Duration // 0: no wait
	}{
		{"7", 7 * time.Second},
		{" 30 ", 30 * time.Second},
		{date(45 * time.Second), 45 * time.Second},
		// Long: capped.
		{"600", maxRetryAfter},
		{date(10 * time.Minute), maxRetryAfter},
		// Absurd, malformed, or no wait at all: the retry policy applies.
		{"86400", 0},
		{"99999999999999999999", 0},
		{date(48 * time.Hour), 0},
		{date(-time.Minute), 0},
		{"0", 0},
		{"-5", 0},
		{"1.5", 0},
		{"soon", 0},
		{"", 0},
	} {
		got, ok := parseRetryAfter(c.value, now)
		if got != c.want || ok != (c.want > 0) {
			t.Errorf("parseRetryAfter(%q) = %s, %v; want %s", c.value, got, ok, c.want)
		}
	}
}

// A passing error that says when to come back carries the wait; one that
// does not stays a plain error; a refused request stays permanent, whatever
// its headers say.
func TestAnthropicProvider_RetryAfter(t *testing.T) {
	for _, c := range []struct {
		status     int
		retryAfter string
		wantDelay  time.Duration
		permanent  bool
	}{
		{429, "7", 7 * time.Second, false},
		{529, "20", 20 * time.Second, false},
		{503, "", 0, false},
		{400, "7", 0, true},
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if c.retryAfter != "" {
				w.Header().Set("Retry-After", c.retryAfter)
			}
			w.WriteHeader(c.status)
			w.Write([]byte(`{"type":"error"}`))
		}))
		p := NewAnthropicProvider("key", "model")
		p.url = srv.URL
		_, err := p.Chat(context.Background(), ChatRequest{MaxTokens: 10})
		srv.Close()

		var wait *RetryAfterError
		var perm *PermanentAPIError
		gotDelay := time.Duration(0)
		if errors.As(err, &wait) {
			gotDelay = wait.Delay
		}
		if err == nil || gotDelay != c.wantDelay || errors.As(err, &perm) != c.permanent {
			t.Errorf("status %d, Retry-After %q: %v (delay %s); want delay %s, permanent %v", c.status, c.retryAfter, err, gotDelay, c.wantDelay, c.permanent)
		}
	}
}
