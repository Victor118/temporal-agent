package connect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/victor/temporal-agent/machine"
	"github.com/victor/temporal-agent/provider"
)

// Modeler calls the model of its owner's turns (docs/design/machine-llm.md
// §6): the request the server built, with the owner's key and model, once.
// It does not retry: a passing failure goes back typed, and the turn's
// workflow decides. A key the provider refuses (or an account with no credit
// left) withdraws the model until agent connect starts again.
type Modeler struct {
	Provider provider.LLMProvider
	// ProviderName and Model are what the machine announces, Model what
	// every request is sent with, whatever it named.
	ProviderName string
	Model        string
	// OnRefused is called when the provider refused the key: the machine
	// tells the gateway it no longer offers its model (Client.Refresh).
	OnRefused func()

	refused atomic.Bool
}

// State is how the machine's model is (machine.LLMState*).
func (m *Modeler) State() string {
	if m.refused.Load() {
		return machine.LLMStateRefused
	}
	return machine.LLMStateOK
}

// Offered reports a model the machine offers: not refused.
func (m *Modeler) Offered() bool { return !m.refused.Load() }

// Call runs an llm directive: its input is the request. Its output is the
// answer, or, with an error, the failure's type (machine.LLMFailure).
func (m *Modeler) Call(ctx context.Context, input json.RawMessage, _ func(string)) (json.RawMessage, error) {
	if !m.Offered() {
		return nil, Refuse("this machine's model refused its key: restart agent connect with a valid AGENT_CONNECT_LLM_API_KEY")
	}
	var req provider.ChatRequest
	if err := json.Unmarshal(input, &req); err != nil {
		return nil, Refuse("the request is unreadable: %v", err)
	}
	// The machine pays: its model, not the one the request names.
	req.Model = m.Model
	resp, err := m.Provider.Chat(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return m.failure(err)
	}
	if resp.Model == "" {
		resp.Model = m.Model
	}
	out, err := json.Marshal(resp)
	if err != nil {
		return nil, err
	}
	if len(out) > machine.MaxLLMOutputBytes {
		return failure(machine.LLMFailure{}, fmt.Errorf("the answer weighs %d bytes, over %d", len(out), machine.MaxLLMOutputBytes))
	}
	return out, nil
}

// failure types a provider's error for the workflow.
func (m *Modeler) failure(err error) (json.RawMessage, error) {
	var waitErr *provider.RetryAfterError
	var permErr *provider.PermanentAPIError
	switch {
	case errors.Is(err, provider.ErrContextTooLong):
		return failure(machine.LLMFailure{Type: machine.LLMFailContextTooLong}, err)
	case errors.As(err, &permErr) && permErr.Credentials:
		if m.refused.CompareAndSwap(false, true) && m.OnRefused != nil {
			m.OnRefused()
		}
		return failure(machine.LLMFailure{Type: machine.LLMFailCredentials}, err)
	case errors.As(err, &permErr):
		return failure(machine.LLMFailure{Type: machine.LLMFailPermanent}, err)
	case errors.As(err, &waitErr):
		return failure(machine.LLMFailure{Type: machine.LLMFailRetryAfter, RetryAfterMS: waitErr.Delay.Milliseconds()}, err)
	}
	return nil, err
}

func failure(f machine.LLMFailure, err error) (json.RawMessage, error) {
	if f.Type == "" {
		return nil, err
	}
	out, _ := json.Marshal(f)
	return out, err
}
