package machine

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/victor/temporal-agent/provider"
)

// The model on a machine (docs/design/machine-llm.md): a turn's call to its
// model, built by a worker and run by the machine of the turn's author, with
// its owner's key and model.
const (
	// CapLLM: the machine calls a model for the turns of its owner (agent
	// connect --llm-provider).
	CapLLM = "llm"
	// KindLLM is one call to the model: the request goes in the directive,
	// never in the database nor in Temporal.
	KindLLM = "llm"
)

// Families of directives, each under its own cap on a machine and in the
// store: a machine that runs one analysis at a time still serves the model
// meanwhile.
const (
	FamilyCoding = "coding"
	FamilyLLM    = "llm"
)

// FamilyOf is the family a directive's kind counts in.
func FamilyOf(kind string) string {
	if kind == KindLLM {
		return FamilyLLM
	}
	return FamilyCoding
}

// What one call to the model may weigh, on the wire. MaxReadBytes is what
// either side reads of one message: a request (the worker's guard,
// LLM_MAX_CONTEXT_BYTES, is 2 MB by default) or an answer. Past
// MaxMessageBytes, only an llm directive's result is read by the gateway.
const (
	MaxReadBytes = 4 << 20
	// MaxLLMRequestBytes is the largest request a worker sends a machine:
	// the directive's message wraps it.
	MaxLLMRequestBytes = MaxReadBytes - 64<<10
	// MaxLLMOutputBytes bounds an answer as JSON (text, tool calls' inputs,
	// usage): it becomes the payload of the activity's completion, then the
	// input of the turn's rewrite, and Temporal refuses a payload past 2 MB.
	MaxLLMOutputBytes = 1536 << 10
	// MaxLLMContentBytes bounds an answer's text: a turn keeps its answers,
	// and returns them in its output, which Temporal bounds too.
	MaxLLMContentBytes = 256 << 10
	// MaxLLMToolCalls bounds the tool calls of one answer, and
	// MaxLLMModelBytes the name of the model.
	MaxLLMToolCalls  = 64
	MaxLLMModelBytes = 128
)

// The calls to the model a machine makes at once: DefaultMaxLLM unless its
// owner says, MaxLLM at most.
const (
	DefaultMaxLLM = 4
	MaxLLM        = 16
)

// LLM states a machine announces (hello, capabilities): its model answers,
// or refused its key (or its account has no credit left) and is withdrawn
// until agent connect starts again. "" = no model configured.
const (
	LLMStateOK      = "ok"
	LLMStateRefused = "refused"
)

// LLMHandoffTimeout bounds the handoff of an llm directive with its request
// (POST /internal/machines/directives): the gateway writes it to the machine
// within LLMWriteTimeout, and answers.
const (
	LLMWriteTimeout   = 60 * time.Second
	LLMHandoffTimeout = LLMWriteTimeout + 15*time.Second
)

// PromptMemory is what a call's prompt held of the user's memory
// (activity.PromptMemory): what the model read, so the version a
// save_user_memory it calls replaces.
type PromptMemory struct {
	// MemoryVersion: 0 for a memory never saved; nil when the prompt held
	// none, and a save would be blind.
	MemoryVersion *int64 `json:"memory_version,omitempty"`
	// MemoryUnread: the prompt was to hold the memory, which could not be
	// read (MemoryVersion nil). The next call reads it again.
	MemoryUnread bool `json:"memory_unread,omitempty"`
}

// LLMInput is an llm directive's input, as the database keeps it: what goes
// back to the workflow with the machine's answer, whoever completes it (the
// connection, or the sweep). Never the request.
type LLMInput struct {
	PromptMemory
}

// LLMResult is what an llm directive's activity returns: the machine's
// answer and what the prompt held of the memory (the shape of
// activity.LLMTurnResponse).
type LLMResult struct {
	provider.ChatResponse
	PromptMemory
}

// LLMFailure is the output of an llm directive the machine ends in error:
// what the workflow does next depends on it.
type LLMFailure struct {
	Type string `json:"type"`
	// RetryAfterMS: the provider said when to come back (LLMFailRetryAfter).
	RetryAfterMS int64 `json:"retry_after_ms,omitempty"`
}

// LLMFailure types.
const (
	// LLMFailRetryAfter: the provider is busy, and said when to come back.
	LLMFailRetryAfter = "retry_after"
	// LLMFailContextTooLong: the conversation no longer fits the machine's
	// model.
	LLMFailContextTooLong = "context_too_long"
	// LLMFailPermanent: the provider refused the request for good.
	LLMFailPermanent = "permanent"
	// LLMFailCredentials: the provider refused the machine's key, or its
	// account has no credit left; the machine withdraws its model.
	LLMFailCredentials = "credentials"
	// LLMFailBadResult: the gateway refused the machine's answer
	// (CheckLLMOutput), and wrote this failure in its place.
	LLMFailBadResult = "bad_result"
)

// RetryAfter is an LLMFailRetryAfter's wait.
func (f LLMFailure) RetryAfter() time.Duration {
	return time.Duration(f.RetryAfterMS) * time.Millisecond
}

// Temporal error types of an llm directive's activity (CallLLMOnMachine),
// besides the directives' own (ErrTypeRevoked, ErrTypeStopping…).
const (
	// ErrTypeUnreachable: the directive could not be handed to its machine
	// now (not connected, the gateway out of reach). Nothing ran.
	ErrTypeUnreachable = "MachineUnreachable"
	// ErrTypeContextTooLong, ErrTypePermanentAPI and ErrTypeRetryAfter are
	// the provider's errors, as CallLLM types them on the workers.
	ErrTypeContextTooLong = "ContextTooLong"
	ErrTypePermanentAPI   = "PermanentAPIError"
	ErrTypeRetryAfter     = "RetryAfterError"
	// ErrTypeBadResult: the machine's answer broke the rules (CheckLLMOutput).
	ErrTypeBadResult = "MachineBadResult"
	// ErrTypeBusy: the machine is at its cap of calls to the model (in the
	// store, or as it says itself): passing, the same machine later.
	ErrTypeBusy = "MachineBusy"
	// ErrTypeHandoffFailed: the gateway could not be reached with the call
	// (the server away): nothing ran, the machine is not to blame.
	ErrTypeHandoffFailed = "HandoffFailed"
	// ErrTypeNoGateway: the worker that took the call hands none to a
	// machine (no gateway configured on it: MACHINES_ENABLED, NOTIFY_URL,
	// INTERNAL_API_KEY on the worker of CallLLM's queue). The worker's
	// fault, not the machine's.
	ErrTypeNoGateway = "WorkerNoGateway"
)

// CodeBusy is a refused result's code when the machine was at its cap of
// directives of that family.
const CodeBusy = "busy"

// ErrUnreachable is a call to the model the gateway could not send to its
// machine now (not connected, or the write failed): the gateway closed it.
var ErrUnreachable = errors.New("machine unreachable")

// StopReasons are the stop reasons an answer may give.
var StopReasons = []string{"end_turn", "max_tokens", "stop_sequence", "tool_use", "pause_turn", "refusal", "model_context_window_exceeded"}

// CheckLLMOutput reads a machine's answer, refusing what breaks the rules
// (docs/design/machine-llm.md §9): the machine has the agent's authority over
// which tools the turn calls, but its answer must be one a turn can carry.
// A tool outside the agent's allowlist is no error here: the turn refuses
// it, as from any model.
func CheckLLMOutput(raw json.RawMessage) (provider.ChatResponse, error) {
	var resp provider.ChatResponse
	if len(raw) > MaxLLMOutputBytes {
		return resp, fmt.Errorf("answer of %d bytes, over %d", len(raw), MaxLLMOutputBytes)
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return resp, fmt.Errorf("answer unreadable: %v", err)
	}
	if len(resp.Content) > MaxLLMContentBytes {
		return resp, fmt.Errorf("answer text of %d bytes, over %d", len(resp.Content), MaxLLMContentBytes)
	}
	if len(resp.ToolCalls) > MaxLLMToolCalls {
		return resp, fmt.Errorf("%d tool calls, over %d", len(resp.ToolCalls), MaxLLMToolCalls)
	}
	ids := make(map[string]bool, len(resp.ToolCalls))
	for i, tc := range resp.ToolCalls {
		switch {
		case tc.ID == "":
			return resp, fmt.Errorf("tool call %d has no ID", i)
		case ids[tc.ID]:
			return resp, fmt.Errorf("tool call ID %q twice", Cut(tc.ID, 64))
		case tc.Name == "":
			return resp, fmt.Errorf("tool call %d has no name", i)
		case !jsonObject(tc.Input):
			return resp, fmt.Errorf("the input of tool call %d is no JSON object", i)
		}
		ids[tc.ID] = true
	}
	if !slices.Contains(StopReasons, resp.StopReason) {
		return resp, fmt.Errorf("stop reason %q", Cut(resp.StopReason, 64))
	}
	if len(resp.Model) > MaxLLMModelBytes {
		return resp, fmt.Errorf("model name of %d bytes, over %d", len(resp.Model), MaxLLMModelBytes)
	}
	return resp, nil
}

// jsonObject reports raw a JSON object.
func jsonObject(raw json.RawMessage) bool {
	var v map[string]json.RawMessage
	return json.Unmarshal(raw, &v) == nil && v != nil
}
