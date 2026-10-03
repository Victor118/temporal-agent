package provider

import (
	"errors"
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
