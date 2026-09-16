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
