package telegram

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf16"
	"unicode/utf8"
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

	if err := c.SendMessage("42", "run `go test` on my_var_name *now"); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 1 || sent[0].ParseMode != "" || sent[0].Text != "run `go test` on my_var_name *now" {
		t.Errorf("sent %+v", sent)
	}

	fail = true
	err := c.SendMessage("42", "hi")
	if err == nil || !strings.Contains(err.Error(), "chat not found") {
		t.Errorf("error %v, want Telegram's reason", err)
	}
}
