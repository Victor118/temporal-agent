package connect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/victor/temporal-agent/machine"
	"github.com/victor/temporal-agent/provider"
)

// modelFunc is a provider answering with f.
type modelFunc func(provider.ChatRequest) (provider.ChatResponse, error)

func (f modelFunc) Chat(_ context.Context, r provider.ChatRequest) (provider.ChatResponse, error) {
	return f(r)
}

// The machine calls its own model with the server's request, once, and
// types its failures for the workflow; a refused key withdraws the model.
func TestModeler(t *testing.T) {
	var sent provider.ChatRequest
	var fail error
	m := &Modeler{ProviderName: "anthropic", Model: "mine", Provider: modelFunc(func(r provider.ChatRequest) (provider.ChatResponse, error) {
		sent = r
		if fail != nil {
			return provider.ChatResponse{}, fail
		}
		return provider.ChatResponse{Content: "hi", StopReason: "end_turn", Usage: &provider.Usage{InputTokens: 3}}, nil
	})}
	refreshed := 0
	m.OnRefused = func() { refreshed++ }
	in, _ := json.Marshal(provider.ChatRequest{Model: "the server's", System: "prompt", MaxTokens: 100})

	out, err := m.Call(context.Background(), in, nil)
	var resp provider.ChatResponse
	if err != nil || json.Unmarshal(out, &resp) != nil || resp.Model != "mine" || resp.Usage.InputTokens != 3 {
		t.Fatalf("answer %s %v", out, err)
	}
	if sent.Model != "mine" || sent.System != "prompt" || sent.MaxTokens != 100 {
		t.Errorf("sent %+v", sent)
	}
	if _, err := machine.CheckLLMOutput(out); err != nil {
		t.Errorf("the gateway would refuse it: %v", err)
	}

	for name, c := range map[string]struct {
		err  error
		want machine.LLMFailure
	}{
		"too long":    {&provider.PermanentAPIError{Err: fmt.Errorf("%w: x", provider.ErrContextTooLong)}, machine.LLMFailure{Type: machine.LLMFailContextTooLong}},
		"permanent":   {&provider.PermanentAPIError{Err: errors.New("bad request")}, machine.LLMFailure{Type: machine.LLMFailPermanent}},
		"retry after": {&provider.RetryAfterError{Err: errors.New("529"), Delay: 30 * time.Second}, machine.LLMFailure{Type: machine.LLMFailRetryAfter, RetryAfterMS: 30000}},
		"passing":     {errors.New("connection reset"), machine.LLMFailure{}},
	} {
		fail = c.err
		out, err := m.Call(context.Background(), in, nil)
		var got machine.LLMFailure
		if len(out) > 0 {
			json.Unmarshal(out, &got)
		}
		if err == nil || got != c.want {
			t.Errorf("%s: %s %v", name, out, err)
		}
	}
	if m.State() != machine.LLMStateOK || refreshed != 0 {
		t.Errorf("withdrawn too soon")
	}

	fail = &provider.PermanentAPIError{Err: errors.New("401"), Credentials: true}
	out, err = m.Call(context.Background(), in, nil)
	var got machine.LLMFailure
	json.Unmarshal(out, &got)
	if err == nil || got.Type != machine.LLMFailCredentials || m.State() != machine.LLMStateRefused || m.Offered() || refreshed != 1 {
		t.Errorf("refused key: %s %v, state %s, refreshed %d", out, err, m.State(), refreshed)
	}
	// Withdrawn: a call that still comes is turned down before anything.
	var refusal *Refusal
	if _, err := m.Call(context.Background(), in, nil); !errors.As(err, &refusal) {
		t.Errorf("after the refusal: %v", err)
	}
}
