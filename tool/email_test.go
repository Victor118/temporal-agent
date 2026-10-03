package tool

import (
	"slices"
	"strings"
	"testing"
)

// headers returns the message's header block.
func headers(t *testing.T, msg []byte) string {
	t.Helper()
	h, _, ok := strings.Cut(string(msg), "\r\n\r\n")
	if !ok {
		t.Fatalf("no header block in %q", msg)
	}
	return h
}

func TestBuildEmail_WritesTheHeaders(t *testing.T) {
	msg, rcpt, err := buildEmail("agent@example.com", emailParams{
		To:      []string{"alice@example.com", "Bob <bob@example.com>"},
		CC:      []string{"carol@example.com"},
		ReplyTo: "victor@example.com",
		Subject: "Weekly report",
		Body:    "Line 1\r\nLine 2",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Join([]string{
		"From: agent@example.com",
		"To: <alice@example.com>, \"Bob\" <bob@example.com>",
		"Cc: <carol@example.com>",
		"Reply-To: <victor@example.com>",
		"Subject: Weekly report",
		"MIME-Version: 1.0",
		"Content-Type: text/plain; charset=\"utf-8\"",
	}, "\r\n")
	if got := headers(t, msg); got != want {
		t.Errorf("headers:\n%s\nwant:\n%s", got, want)
	}
	if !strings.HasSuffix(string(msg), "\r\n\r\nLine 1\r\nLine 2") {
		t.Errorf("body not kept: %q", msg)
	}
	if want := []string{"alice@example.com", "bob@example.com", "carol@example.com"}; !slices.Equal(rcpt, want) {
		t.Errorf("recipients %v, want %v", rcpt, want)
	}
}

// A header is ASCII: a subject or a name that is not goes encoded.
func TestBuildEmail_EncodesNonASCII(t *testing.T) {
	msg, _, err := buildEmail("agent@example.com", emailParams{
		To:      []string{"Zoé <zoe@example.com>"},
		Subject: "Résumé de la réunion",
		Body:    "é",
	})
	if err != nil {
		t.Fatal(err)
	}
	h := headers(t, msg)
	if !strings.Contains(h, "\r\nSubject: =?utf-8?q?R=C3=A9sum=C3=A9_de_la_r=C3=A9union?=") {
		t.Errorf("subject not encoded:\n%s", h)
	}
	if !strings.Contains(h, "To: =?utf-8?q?Zo=C3=A9?= <zoe@example.com>") {
		t.Errorf("name not encoded:\n%s", h)
	}
	for _, r := range h {
		if r > 127 {
			t.Fatalf("non-ASCII header:\n%s", h)
		}
	}
}

// A line break in any header value would add headers or start the body:
// refused, as is a value that is not one address.
func TestBuildEmail_RefusesHeaderInjection(t *testing.T) {
	ok := emailParams{To: []string{"alice@example.com"}, Subject: "Hi", Body: "b"}
	for name, mutate := range map[string]func(*emailParams){
		"subject CRLF":      func(p *emailParams) { p.Subject = "Hi\r\nBcc: eve@example.com" },
		"subject LF":        func(p *emailParams) { p.Subject = "Hi\nContent-Type: text/html" },
		"subject CR":        func(p *emailParams) { p.Subject = "Hi\rX: y" },
		"to CRLF":           func(p *emailParams) { p.To = []string{"alice@example.com\r\nBcc: eve@example.com"} },
		"to folded":         func(p *emailParams) { p.To = []string{"Alice\r\n <alice@example.com>"} },
		"cc LF":             func(p *emailParams) { p.CC = []string{"carol@example.com\nX: y"} },
		"reply_to CRLF":     func(p *emailParams) { p.ReplyTo = "v@example.com\r\n\r\nforged body" },
		"to two in one":     func(p *emailParams) { p.To = []string{"alice@example.com, eve@example.com"} },
		"to not an address": func(p *emailParams) { p.To = []string{"alice"} },
		"reply_to not one":  func(p *emailParams) { p.ReplyTo = "nobody" },
		"no recipient":      func(p *emailParams) { p.To = nil },
		"no subject":        func(p *emailParams) { p.Subject = "" },
	} {
		t.Run(name, func(t *testing.T) {
			p := ok
			mutate(&p)
			if msg, _, err := buildEmail("agent@example.com", p); err == nil {
				t.Errorf("accepted, message %q", msg)
			}
		})
	}
	if _, _, err := buildEmail("agent@example.com", ok); err != nil {
		t.Errorf("a plain email was refused: %v", err)
	}
}
