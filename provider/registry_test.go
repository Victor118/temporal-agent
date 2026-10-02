package provider

import (
	"context"
	"testing"
)

type echo struct{ model string }

func (e echo) Chat(context.Context, ChatRequest) (ChatResponse, error) {
	return ChatResponse{Content: e.model}, nil
}

func TestNew(t *testing.T) {
	if p, err := New("anthropic", "k", "m"); err != nil || p == nil {
		t.Fatalf("anthropic: %v", err)
	}
	if _, err := New("nope", "k", "m"); err == nil {
		t.Error("an unknown provider was built")
	}
	Register("echo-test", func(_, model string) LLMProvider { return echo{model} })
	p, err := New("echo-test", "", "m1")
	if err != nil {
		t.Fatal(err)
	}
	if r, _ := p.Chat(context.Background(), ChatRequest{}); r.Content != "m1" {
		t.Errorf("got %q", r.Content)
	}
}
