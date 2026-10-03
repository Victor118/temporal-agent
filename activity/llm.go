package activity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"

	"go.temporal.io/sdk/temporal"

	"github.com/victor/temporal-agent/conversation"
	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/store"
)

// DefaultMaxContextBytes bounds what one LLM call sends when the worker sets
// no LLM_MAX_CONTEXT_BYTES: system prompt, tool definitions and conversation,
// as JSON. Bytes stand for tokens, at about three bytes a token or more (JSON
// escapes count as bytes, not tokens): 2 MB stays under the 1M-token window
// of the default model, its answer included. A model with a smaller window
// needs a lower bound (200K tokens: about 400000).
const DefaultMaxContextBytes = 2_000_000

// maxResponseTokens is what an answer may take.
const maxResponseTokens = 16384

// ErrContextTooLong is the type of the error a call too large for the model
// fails with. Never retried: the conversation only grows.
const ErrContextTooLong = "ContextTooLong"

// ContextTooLongMessage is what the session's members read when their
// conversation no longer fits: a fork starts again from a summary.
const ContextTooLongMessage = "La conversation est trop longue pour le modèle : forke-la à partir d'un message récent pour repartir d'un résumé."

// ConversationLoader is what the LLM call reads of the store: a turn's
// conversation and its user's memory.
type ConversationLoader interface {
	LoadConversation(ctx context.Context, sessionID string, upTo int64, turnKeys []string) ([]store.MessageWithID, error)
	LoadMemory(ctx context.Context, scope store.MemoryScope, scopeID string) (string, error)
}

// LLMCatalog is what the LLM call reads of the worker's catalog: the tools'
// definitions, the agents' names, and which tool inputs are private.
type LLMCatalog interface {
	ToolDefinitions(names []string) ([]provider.ToolDefinition, []string)
	AgentLabels() map[string]conversation.Label
	PrivateInput(name string) bool
}

// PromptBuilder builds an agent's base prompt for the tools it is offered.
type PromptBuilder interface {
	AgentPrompt(agentID string, tools []string) string
}

// LLMActivities calls the model. It builds each request itself from
// references: the history, the prompt and the tool definitions never go
// through a workflow, whose history records every activity input. Carried
// there on every call of a turn, they grew it by the conversation's size
// times the calls, until Temporal refused the payload.
type LLMActivities struct {
	Provider provider.LLMProvider
	Store    ConversationLoader
	Catalog  LLMCatalog
	Prompts  PromptBuilder
	// MaxContextBytes bounds a request (see DefaultMaxContextBytes); 0 = the
	// default.
	MaxContextBytes int
}

// LLMTurnRequest is one call of a turn. The conversation is given inline
// (Messages) by a run that persists nothing, a sub-agent or a scheduled task,
// whose conversation is its own and short; or by reference (History) by a
// session's turn.
type LLMTurnRequest struct {
	Model   string `json:"model,omitempty"` // empty = the worker's default
	AgentID string `json:"agent_id"`        // who reads: its prompt, its own messages
	// Tools names the tools the turn may dispatch, resolved once at its
	// start; the definitions come from the catalog.
	Tools    []string        `json:"tools,omitempty"`
	Prompt   PromptRef       `json:"prompt"`
	Messages []store.Message `json:"messages,omitempty"`
	History  *TurnHistory    `json:"history,omitempty"`
}

// PromptRef is what the system prompt is built from, on each call: the same
// sections, in the same order, as the agent's base prompt was given when the
// workflow built it. Built here, it is never carried by the workflow, and a
// memory the turn saves is read by its next call.
type PromptRef struct {
	Override string `json:"override,omitempty"`  // replaces the agent's base prompt
	MemoryOf string `json:"memory_of,omitempty"` // user whose memory the prompt holds; empty = none
	UserName string `json:"user_name,omitempty"` // that user's name, as the memory section names them
	PartNote string `json:"part_note,omitempty"` // ends the prompt (several agents addressed)
}

// TurnHistory points at a session turn's conversation: the session up to
// UpTo, the message the turn answers, what the turns answering the earlier
// messages wrote, then this message's turns' (store.TurnReads). A message
// someone stores after UpTo is not read: it gets its own turn.
type TurnHistory struct {
	SessionID string `json:"session_id"`
	UpTo      int64  `json:"up_to"`
	// EarlierTurns answered the same message before this one: their answers
	// are read.
	EarlierTurns []string `json:"earlier_turns,omitempty"`
	TurnKey      string   `json:"turn_key"`
	// Tail is what the turn produced that it could not confirm written, from
	// index TailStart of the turn: a flush failed. Empty otherwise. Read
	// after the turn's stored messages, those among them stored after all
	// read once.
	Tail      []store.Message `json:"tail,omitempty"`
	TailStart int             `json:"tail_start,omitempty"`
}

func (a *LLMActivities) CallLLM(ctx context.Context, req LLMTurnRequest) (provider.ChatResponse, error) {
	request, err := a.buildRequest(ctx, req)
	if err != nil {
		return provider.ChatResponse{}, err
	}
	size, limit := requestSize(request), a.maxContextBytes()
	if size > limit {
		log.Printf("LLM call refused: agent %s, %d bytes over the %d limit (LLM_MAX_CONTEXT_BYTES)", req.AgentID, size, limit)
		return provider.ChatResponse{}, temporal.NewNonRetryableApplicationError(ContextTooLongMessage, ErrContextTooLong, nil)
	}

	resp, err := a.Provider.Chat(ctx, request)
	if err != nil {
		// Under the guard, the model refused it all the same: the same
		// advice, never retried.
		if errors.Is(err, provider.ErrContextTooLong) {
			log.Printf("LLM call refused by the model: agent %s, %d bytes: %v", req.AgentID, size, err)
			return resp, temporal.NewNonRetryableApplicationError(ContextTooLongMessage, ErrContextTooLong, err)
		}
		var permErr *provider.PermanentAPIError
		if errors.As(err, &permErr) {
			return resp, temporal.NewNonRetryableApplicationError(err.Error(), "PermanentAPIError", err)
		}
		// The API said when to come back: the next attempt waits that long
		// instead of the policy's interval. The attempts still count.
		var waitErr *provider.RetryAfterError
		if errors.As(err, &waitErr) {
			return resp, temporal.NewApplicationErrorWithOptions(err.Error(), "RetryAfterError", temporal.ApplicationErrorOptions{
				NextRetryDelay: waitErr.Delay,
				Cause:          err,
			})
		}
	}
	return resp, err
}

// buildRequest builds the request req points at.
func (a *LLMActivities) buildRequest(ctx context.Context, req LLMTurnRequest) (provider.ChatRequest, error) {
	messages, err := a.conversation(ctx, req)
	if err != nil {
		return provider.ChatRequest{}, err
	}
	chat := conversation.Convert(messages, conversation.View{Self: req.AgentID, Agents: a.Catalog.AgentLabels(), Private: a.Catalog})

	tools, missing := a.Catalog.ToolDefinitions(req.Tools)
	offered := make([]string, len(tools))
	for i, t := range tools {
		offered[i] = t.Name
	}
	tools = append(tools, withdrawnTools(req.AgentID, missing, chat)...)

	// Cache breakpoints: the system prompt and the last tool definition are
	// stable across a turn's calls; the second-to-last message ends the
	// prefix the next call shares.
	if len(tools) > 0 {
		tools[len(tools)-1].CacheBreakpoint = true
	}
	if len(chat) >= 2 {
		chat[len(chat)-2].CacheBreakpoint = true
	}
	return provider.ChatRequest{
		Model:       req.Model,
		System:      a.systemPrompt(ctx, req, offered),
		Messages:    chat,
		Tools:       tools,
		MaxTokens:   maxResponseTokens,
		CacheSystem: true,
	}, nil
}

// withdrawnDescription is what the model reads of a tool withdrawn while it
// was in use.
const withdrawnDescription = "Withdrawn: this tool is no longer available. Do not call it."

// withdrawnTools keeps a definition for each tool of missing, gone from the
// catalog since the turn started, that the conversation calls: the API
// rejects tool blocks a request defines no tool for, which an agent whose
// only tool was withdrawn would send. The definition is a stub that says not
// to call it, and the tool is never in the prompt; one the conversation does
// not call is simply not offered.
func withdrawnTools(agentID string, missing []string, chat []provider.ChatMessage) []provider.ToolDefinition {
	if len(missing) == 0 {
		return nil
	}
	called := map[string]bool{}
	for _, m := range chat {
		for _, tc := range m.ToolCalls {
			called[tc.Name] = true
		}
	}
	var stubs []provider.ToolDefinition
	var kept []string
	for _, name := range missing {
		if called[name] {
			stubs = append(stubs, provider.ToolDefinition{Name: name, Description: withdrawnDescription, InputSchema: json.RawMessage(`{"type":"object"}`)})
			kept = append(kept, name)
		}
	}
	log.Printf("LLM call: agent %s, tools %v gone from the catalog, not offered; %v already called, kept as withdrawn", agentID, missing, kept)
	return stubs
}

// conversation returns the messages the call reads, in order.
func (a *LLMActivities) conversation(ctx context.Context, req LLMTurnRequest) ([]store.Message, error) {
	h := req.History
	if h == nil {
		return req.Messages, nil
	}
	if len(req.Messages) > 0 {
		return nil, temporal.NewNonRetryableApplicationError("a call gives its conversation inline or by reference, not both", "BadLLMRequest", nil)
	}
	turns := append(append([]string(nil), h.EarlierTurns...), h.TurnKey)
	// Read and converted again on every call of the turn, the whole history:
	// what it costs is not measured yet. If it shows, a worker cache of the
	// ordered prefix keyed by (session, UpTo, EarlierTurns) would serve the
	// turn's next calls (a retry elsewhere only misses it).
	loaded, err := a.Store.LoadConversation(ctx, h.SessionID, h.UpTo, turns)
	if err != nil {
		return nil, fmt.Errorf("load conversation: %w", err)
	}
	ordered := conversation.Order(loaded)
	stored := make(map[string]bool, len(ordered))
	messages := make([]store.Message, 0, len(ordered)+len(h.Tail))
	for _, m := range ordered {
		stored[m.Key] = true
		messages = append(messages, m.Message)
	}
	// This turn's messages come last: everything else is older than it.
	for i, m := range h.Tail {
		if !stored[store.TurnMessageKey(h.TurnKey, h.TailStart+i)] {
			messages = append(messages, m)
		}
	}
	return messages, nil
}

// systemPrompt builds the prompt: the override or the agent's base prompt
// for the tools offered, the user's memory, the part note. A memory that
// cannot be read costs the personalisation, not the call.
func (a *LLMActivities) systemPrompt(ctx context.Context, req LLMTurnRequest, tools []string) string {
	p := req.Prompt
	prompt := p.Override
	if prompt == "" {
		prompt = a.Prompts.AgentPrompt(req.AgentID, tools)
	}
	if p.MemoryOf != "" {
		memory, err := a.Store.LoadMemory(ctx, store.MemoryScopeUser, p.MemoryOf)
		if err != nil {
			log.Printf("LLM call: memory of user %s not loaded: %v", p.MemoryOf, err)
		} else if memory != "" {
			prompt += userMemorySection(p.UserName, memory)
		}
	}
	return prompt + p.PartNote
}

func (a *LLMActivities) maxContextBytes() int {
	if a.MaxContextBytes > 0 {
		return a.MaxContextBytes
	}
	return DefaultMaxContextBytes
}

// requestSize is the request's size as JSON, what the guard measures. A
// request that cannot be encoded is past any limit: the guard stops it,
// rather than letting it through unmeasured.
func requestSize(r provider.ChatRequest) int {
	b, err := json.Marshal(r)
	if err != nil {
		log.Printf("LLM call: request not measurable: %v", err)
		return math.MaxInt
	}
	return len(b)
}
