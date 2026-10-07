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
		{"prompt and max_tokens over the window", 400, `{"type":"error","error":{"type":"invalid_request_error","message":"input length and ` + "`max_tokens`" + ` exceed context limit: 185000 + 16384 > 200000, decrease input length or ` + "`max_tokens`" + ` and try again"}}`, true},
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

// An answer says which model wrote it and what it took, cache included.
func TestAnthropicProvider_UsageAndModel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"model":"claude-sonnet-5-20260901","stop_reason":"end_turn","content":[{"type":"text","text":"hi"}],` +
			`"usage":{"input_tokens":12,"output_tokens":3,"cache_creation_input_tokens":100,"cache_read_input_tokens":2000}}`))
	}))
	defer srv.Close()
	p := NewAnthropicProvider("key", "model")
	p.url = srv.URL
	resp, err := p.Chat(context.Background(), ChatRequest{Messages: []ChatMessage{{Role: "user", Content: []byte(`"hi"`)}}})
	if err != nil {
		t.Fatal(err)
	}
	want := Usage{InputTokens: 12, OutputTokens: 3, CacheCreationInputTokens: 100, CacheReadInputTokens: 2000}
	if resp.Model != "claude-sonnet-5-20260901" || resp.Usage == nil || *resp.Usage != want || resp.Content != "hi" {
		t.Errorf("got %+v (usage %+v)", resp, resp.Usage)
	}
}

// A key refused, or an account with no credit left, is told apart from
// another refused request: a machine stops offering its model for it.
func TestAnthropicProvider_RefusedCredentials(t *testing.T) {
	for _, c := range []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{"key refused", 401, `{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}`, true},
		{"forbidden", 403, `{"type":"error","error":{"type":"permission_error","message":"no"}}`, true},
		{"no credit", 400, `{"type":"error","error":{"type":"invalid_request_error","message":"Your credit balance is too low to access the Anthropic API."}}`, true},
		{"billing", 400, `{"type":"error","error":{"type":"billing_error","message":"billing"}}`, true},
		{"bad request", 400, `{"type":"error","error":{"type":"invalid_request_error","message":"messages: roles must alternate"}}`, false},
		{"unknown model", 404, `{"type":"error","error":{"type":"not_found_error","message":"model: x"}}`, false},
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
			if !errors.As(err, &perm) || perm.Credentials != c.want {
				t.Errorf("got %#v, want credentials %v", err, c.want)
			}
		})
	}
}
