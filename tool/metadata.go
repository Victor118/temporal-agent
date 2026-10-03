package tool

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/victor/temporal-agent/store"
)

// PrivateInputs tells which tools keep their input, and their result, from the
// session's members (Tool.PrivateInput).
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
// hidden when the tool is private (Tool.PrivateInput: input and result).
func DisplayInput(private bool, input json.RawMessage) json.RawMessage {
	if private {
		return hiddenInput
	}
	return input
}

// hiddenResult stands for the result of a call to a private tool: it may
// repeat what the input carried.
const hiddenResult = "(private)"

// DisplayResult is the result of a tool call as the session's members see it:
// hidden when the tool is private (Tool.PrivateInput: input and result).
func DisplayResult(private bool, content string) string {
	if private {
		return hiddenResult
	}
	return content
}

// CallContext is what a tool flagged NeedsCallContext receives besides the
// model's input: what the run knows of the call and the model must not
// forge. Who called (the chain of agents), where its user is (the channel),
// how to sign, what the model read of the user's memory. Set by the workflow,
// never taken from the model's input (WithCallContext replaces any such
// field); a value the model could choose belongs in the tool's input instead.
type CallContext struct {
	AgentChain []string `json:"agent_chain,omitempty"`
	Channel    string   `json:"channel,omitempty"`
	ChannelID  string   `json:"channel_id,omitempty"`
	// Agent signs what the tool sends to the channel, as the agent's answer
	// is (AgentWorkflowInput.SignReply). Empty: unsigned.
	Agent string `json:"agent,omitempty"`
	// NotifyQueue is the calling turn's task queue, whose workers hold the
	// channels' notifiers: a workflow tool that writes to the channel sends
	// there, since its own queue's workers may have none (a coding worker).
	// Empty: the tool's own queue.
	NotifyQueue string `json:"notify_queue,omitempty"`
	// MemoryVersion is the version of the user's memory the model read in
	// the prompt of the call that made this one: what save_user_memory
	// replaces. Nil: the prompt held none.
	MemoryVersion *int64 `json:"memory_version,omitempty"`
	// MemoryUnread: the prompt was to hold the user's memory, and it could
	// not be read (MemoryVersion is nil). False with no version: the run is
	// given no memory, a sub-agent.
	MemoryUnread bool `json:"memory_unread,omitempty"`
}

// callContextKeys are the input keys CallContext's fields decode from: its
// JSON names, read from its tags so that a field added there is reserved too.
var callContextKeys = func() []string {
	var keys []string
	t := reflect.TypeFor[CallContext]()
	for i := range t.NumField() {
		name, _, _ := strings.Cut(t.Field(i).Tag.Get("json"), ",")
		if name != "" && name != "-" {
			keys = append(keys, name)
		}
	}
	return keys
}()

// WithCallContext adds cc's fields to a tool's input object. They are the
// caller's to set, never the model's: a key the model put that one of them
// decodes from is removed, whatever cc holds. encoding/json matches a key
// to a field regardless of case ("AGENT" fills Agent), so the keys go
// regardless of case too: a field cc leaves empty (omitempty, absent from
// what is added) must stay empty, not take the model's value.
func WithCallContext(input json.RawMessage, cc CallContext) (json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(input, &fields); err != nil {
		return nil, fmt.Errorf("invalid input: %w", err)
	}
	if fields == nil { // "null"
		fields = map[string]json.RawMessage{}
	}
	for k := range fields {
		if isCallContextKey(k) {
			delete(fields, k)
		}
	}
	extra, _ := json.Marshal(cc)
	var ccFields map[string]json.RawMessage
	json.Unmarshal(extra, &ccFields)
	for k, v := range ccFields {
		fields[k] = v
	}
	return json.Marshal(fields)
}

// isCallContextKey tells whether encoding/json would decode key into one of
// CallContext's fields: it folds case as strings.EqualFold does.
func isCallContextKey(key string) bool {
	for _, reserved := range callContextKeys {
		if strings.EqualFold(key, reserved) {
			return true
		}
	}
	return false
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
