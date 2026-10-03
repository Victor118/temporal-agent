package provider

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAnthropicProvider_ResolveModel(t *testing.T) {
	p := NewAnthropicProvider("key", "default-model")
	if m, err := p.resolveModel(""); err != nil || m != "default-model" {
		t.Errorf("empty request: got %q, %v", m, err)
	}
	if m, err := p.resolveModel("explicit"); err != nil || m != "explicit" {
		t.Errorf("explicit request: got %q, %v", m, err)
	}

	_, err := NewAnthropicProvider("key", "").resolveModel("")
	var perm *PermanentAPIError
	if !errors.As(err, &perm) {
		t.Errorf("no model at all: got %v, want PermanentAPIError", err)
	}
}

// A passing failure of the API is retried; a request it refuses is not, since
// it would be refused again.
func TestTransientStatus(t *testing.T) {
	for code, want := range map[int]bool{
		400: false, 401: false, 403: false, 404: false, 413: false,
		408: true, 429: true, 500: true, 502: true, 503: true, 504: true, 529: true,
	} {
		if got := transientStatus(code); got != want {
			t.Errorf("transientStatus(%d) = %v, want %v", code, got, want)
		}
	}
}

// A request the API refuses for its size is told apart, from the error's type
// and message: the session must be forked, not the call retried. Another bad
// request is not.
func TestAnthropicProvider_ContextTooLong(t *testing.T) {
	for _, c := range []struct {
		name    string
		status  int
		body    string
		tooLong bool
	}{
		{"prompt too long", 400, `{"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: 215123 tokens > 200000 maximum"}}`, true},
		{"request too large", 413, `{"type":"error","error":{"type":"request_too_large","message":"Request exceeds the maximum allowed number of bytes."}}`, true},
		{"413 without a body", 413, `<html>Too large</html>`, true},
		{"another bad request", 400, `{"type":"error","error":{"type":"invalid_request_error","message":"messages: roles must alternate"}}`, false},
		{"not JSON", 400, `bad request`, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(c.status)
				w.Write([]byte(c.body))
			}))
			defer srv.Close()
			p := NewAnthropicProvider("key", "model")
			p.url = srv.URL

			_, err := p.Chat(context.Background(), ChatRequest{Messages: []ChatMessage{{Role: "user", Content: []byte(`"hi"`)}}})
			var perm *PermanentAPIError
			if !errors.As(err, &perm) {
				t.Fatalf("got %v, want a PermanentAPIError", err)
			}
			if got := errors.Is(err, ErrContextTooLong); got != c.tooLong {
				t.Errorf("context too long: %v, want %v (%v)", got, c.tooLong, err)
			}
		})
	}
}
