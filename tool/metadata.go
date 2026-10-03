package tool

import (
	"encoding/json"
	"fmt"

	"github.com/victor/temporal-agent/store"
)

// PrivateInputs tells which tools keep their input from the session's members
// (Tool.PrivateInput).
type PrivateInputs interface {
	PrivateInput(toolName string) bool
}

// PrivateSet is PrivateInputs over a set of tool names.
type PrivateSet map[string]bool

func (s PrivateSet) PrivateInput(name string) bool { return s[name] }

// PrivateSetOf collects the tools with a private input among published ones.
func PrivateSetOf(records []store.ToolRecord) PrivateSet {
	s := PrivateSet{}
	for _, r := range records {
		if r.PrivateInput {
			s[r.Name] = true
		}
	}
	return s
}

// hiddenInput stands for the input of a tool call members may not see.
var hiddenInput = json.RawMessage(`{"content":"(private)"}`)

// DisplayInput is the input of a tool call as the session's members see it:
// hidden when the tool's input is private.
func DisplayInput(private bool, input json.RawMessage) json.RawMessage {
	if private {
		return hiddenInput
	}
	return input
}

// CallContext is what a workflow tool flagged NeedsCallContext receives
// besides the model's input: who called it, and where its user is.
type CallContext struct {
	AgentChain []string `json:"agent_chain,omitempty"`
	Channel    string   `json:"channel,omitempty"`
	ChannelID  string   `json:"channel_id,omitempty"`
	// Agent signs what the tool sends to the channel, as the agent's answer
	// is (AgentWorkflowInput.SignReply). Empty: unsigned.
	Agent string `json:"agent,omitempty"`
}

// WithCallContext adds cc's fields to a tool's input object. They are the
// caller's to set, never the model's: a value the model put under one of
// these names is replaced.
func WithCallContext(input json.RawMessage, cc CallContext) (json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(input, &fields); err != nil {
		return nil, fmt.Errorf("invalid input: %w", err)
	}
	if fields == nil { // "null"
		fields = map[string]json.RawMessage{}
	}
	delete(fields, "agent_chain")
	delete(fields, "channel")
	delete(fields, "channel_id")
	delete(fields, "agent")
	extra, _ := json.Marshal(cc)
	var ccFields map[string]json.RawMessage
	json.Unmarshal(extra, &ccFields)
	for k, v := range ccFields {
		fields[k] = v
	}
	return json.Marshal(fields)
}

// Result is what a workflow tool returns, so the calling agent reads every
// workflow tool's result the same way: the content the model reads, and
// whether it is an error. A tool's own output type may carry these two fields
// beside its others.
type Result struct {
	Content string `json:"content"`
	IsError bool   `json:"is_error,omitempty"`
}

// DecodeResult reads a workflow tool's result: a Result, or, from a workflow
// that returns plain text, a string. Anything else is passed on as raw JSON.
func DecodeResult(raw json.RawMessage) (content string, isError bool) {
	var r struct {
		Content *string `json:"content"`
		IsError bool    `json:"is_error"`
	}
	if json.Unmarshal(raw, &r) == nil && r.Content != nil {
		return *r.Content, r.IsError
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return text, false
	}
	return string(raw), false
}
