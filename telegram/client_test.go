package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/victor/temporal-agent/activity"
)

func TestSplitMessage(t *testing.T) {
	if got := splitMessage("short", 10); len(got) != 1 || got[0] != "short" {
		t.Errorf("short: %q", got)
	}

	// Accented letters are two bytes: a byte cut would split one.
	text := strings.Repeat("é", 25)
	got := splitMessage(text, 10)
	if strings.Join(got, "") != text {
		t.Fatalf("lost text: %q", got)
	}
	for _, c := range got {
		if !utf8.ValidString(c) || len([]rune(c)) > 10 {
			t.Errorf("chunk %q", c)
		}
	}

	// An emoji is two UTF-16 units, which is what Telegram counts.
	for _, c := range splitMessage(strings.Repeat("😀", 7), 10) {
		if n := len(utf16.Encode([]rune(c))); n > 10 {
			t.Errorf("chunk of %d units", n)
		}
	}

	// A line break in the second half is where the cut goes.
	got = splitMessage("aaaaaaa\nbbbbbbb", 10)
	if len(got) != 2 || got[0] != "aaaaaaa\n" || got[1] != "bbbbbbb" {
		t.Errorf("line cut: %q", got)
	}
}

// Text the agent writes is sent as is: no parse mode an unpaired _ could
// break.
func TestSendMessage_PlainTextAndErrors(t *testing.T) {
	var sent []SendMessageRequest
	fail := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req SendMessageRequest
		json.NewDecoder(r.Body).Decode(&req)
		sent = append(sent, req)
		if fail {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"ok":false,"description":"Bad Request: chat not found"}`))
		}
	}))
	defer srv.Close()
	c := NewClient("tok")
	c.baseURL = srv.URL

	if err := c.SendMessage(context.Background(), "42", "run `go test` on my_var_name *now"); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 1 || sent[0].ParseMode != "" || sent[0].Text != "run `go test` on my_var_name *now" {
		t.Errorf("sent %+v", sent)
	}

	fail = true
	err := c.SendMessage(context.Background(), "42", "hi")
	if err == nil || !strings.Contains(err.Error(), "chat not found") {
		t.Errorf("error %v, want Telegram's reason", err)
	}
}

// A long answer goes out in several messages. Once one is out, a failure is
// a partial delivery, which nothing retries: the user would read the first
// part twice. A failure that may pass is retried message by message.
func TestSendMessage_RetriesEachPartAndNeverResendsOne(t *testing.T) {
	defer func(d time.Duration) { retryDelay = d }(retryDelay)
	retryDelay = time.Millisecond

	var texts []string
	failures := map[int]int{} // request number -> status to answer
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req SendMessageRequest
		json.NewDecoder(r.Body).Decode(&req)
		texts = append(texts, req.Text)
		if status, ok := failures[len(texts)]; ok {
			w.WriteHeader(status)
		}
	}))
	defer srv.Close()
	c := NewClient("tok")
	c.baseURL = srv.URL
	long := strings.Repeat("a", maxMessageUnits) + strings.Repeat("b", maxMessageUnits) + "c"

	// The second part fails once on a server error: it alone is sent again.
	failures = map[int]int{2: http.StatusBadGateway}
	if err := c.SendMessage(context.Background(), "42", long); err != nil {
		t.Fatal(err)
	}
	if len(texts) != 4 || texts[1] != texts[2] || texts[0] == texts[1] || texts[3] != "c" {
		t.Errorf("sent %d messages, want parts 1, 2, 2 again, 3", len(texts))
	}

	// The second part is refused for good: one part went out.
	texts, failures = nil, map[int]int{2: http.StatusBadRequest}
	err := c.SendMessage(context.Background(), "42", long)
	var partial *activity.PartialDelivery
	if !errors.As(err, &partial) || partial.Delivered != 1 || partial.Total != 3 {
		t.Errorf("error %v, want a partial delivery of 1 part out of 3", err)
	}
	if len(texts) != 2 {
		t.Errorf("sent %d messages, want 2: a refusal is not retried", len(texts))
	}

	// Nothing went out: a plain error, which the activity may retry.
	texts, failures = nil, map[int]int{1: http.StatusBadRequest}
	if err := c.SendMessage(context.Background(), "42", long); err == nil || errors.As(err, &partial) {
		t.Errorf("error %v, want a plain failure", err)
	}
}

// A part after the first is only started if the activity has the time to
// finish it, retries included: one that times out with the beginning sent is
// retried, and sends the beginning again.
func TestSendMessage_StartsNoPartItCannotFinish(t *testing.T) {
	var sent int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { sent++ }))
	defer srv.Close()
	c := NewClient("tok")
	c.baseURL = srv.URL
	long := strings.Repeat("a", maxMessageUnits) + "b"
	if worst := c.worstSend(); worst != 18*time.Second {
		t.Errorf("worst case for one message = %s, want 18s (3 tries of 5s, 1s and 2s between)", worst)
	}

	short, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := c.SendMessage(short, "42", long)
	var partial *activity.PartialDelivery
	if !errors.As(err, &partial) || partial.Delivered != 1 || sent != 1 {
		t.Errorf("with 10s left: %v, %d sent; want the first part alone, as a partial delivery", err, sent)
	}

	sent = 0
	enough, cancel2 := context.WithTimeout(context.Background(), time.Minute)
	defer cancel2()
	if err := c.SendMessage(enough, "42", long); err != nil || sent != 2 {
		t.Errorf("with a minute left: %v, %d sent", err, sent)
	}
}
