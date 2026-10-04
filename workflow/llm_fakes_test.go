package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"

	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/store"
)

// memSession is a session's store in memory, written and read as Postgres
// does: messages numbered in order, a rewrite under a key it holds ignored.
type memSession struct {
	mu       sync.Mutex
	messages []store.MessageWithID
	memory   map[string]store.Memory
	persists []persistCall // every PersistContext, in order
	// failPersists makes the next n PersistContext fail, after writing
	// their messages when writeThenFail: written, but not confirmed.
	failPersists  int
	writeThenFail bool
	loads         int // LoadConversation calls
	// agents are the agents the store holds, by ID, with their names;
	// deleted says the session is gone. For the participant's turn checks.
	agents  map[string]string
	deleted bool
	// checkFails and endFails make the next n session reads (the first
	// step of a turn's check) and turn end writes fail: the store away.
	checkFails, endFails int
}

// persistCall records one PersistContext activity call.
type persistCall struct {
	turnKey    string
	startIndex int
	roles      []string
}

// add stores m under key, as the server stores a person's message, and
// returns its ID.
func (s *memSession) add(key string, m store.Message) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addLocked(key, m)
}

func (s *memSession) addLocked(key string, m store.Message) int64 {
	for _, have := range s.messages {
		if have.Key == key {
			return have.ID
		}
	}
	id := int64(len(s.messages) + 1)
	s.messages = append(s.messages, store.MessageWithID{ID: id, Key: key, Message: m})
	return id
}

// history is every stored message, in ID order.
func (s *memSession) history() []store.MessageWithID {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.messages)
}

func (s *memSession) persisted() []persistCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.persists)
}

func (s *memSession) AppendMessages(_ context.Context, _ string, turnKey string, startIndex int, messages []store.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	call := persistCall{turnKey: turnKey, startIndex: startIndex}
	for _, m := range messages {
		call.roles = append(call.roles, string(m.Role))
	}
	s.persists = append(s.persists, call)
	fail := s.failPersists > 0
	if fail {
		s.failPersists--
	}
	if !fail || s.writeThenFail {
		for i, m := range messages {
			s.addLocked(store.TurnMessageKey(turnKey, startIndex+i), m)
		}
	}
	if fail {
		return errors.New("database unavailable")
	}
	return nil
}

func (s *memSession) LoadConversation(_ context.Context, _ string, scope store.TurnScope) ([]store.MessageWithID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loads++
	ends := store.TurnEndIDs(s.messages)
	var out []store.MessageWithID
	for _, m := range s.messages {
		if store.TurnReads(m.ID, m.Key, scope, ends) {
			out = append(out, m)
		}
	}
	return out, nil
}

func (s *memSession) GetSession(_ context.Context, sessionID string) (*store.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.checkFails > 0 {
		s.checkFails--
		return nil, errors.New("database unavailable")
	}
	if s.deleted {
		return nil, nil
	}
	return &store.Session{SessionID: sessionID}, nil
}

func (s *memSession) GetAgent(_ context.Context, agentID string) (*store.Agent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	name, ok := s.agents[agentID]
	if !ok {
		return nil, nil
	}
	return &store.Agent{ID: agentID, Name: name}, nil
}

func (s *memSession) HasTurnEnd(_ context.Context, _ string, turnKey string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.messages {
		if m.Key == store.TurnEndKey(turnKey) {
			return true, nil
		}
	}
	return false, nil
}

func (s *memSession) AppendTurnEnd(_ context.Context, _ string, turnKey string, msg store.Message) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.endFails > 0 {
		s.endFails--
		return 0, errors.New("database unavailable")
	}
	return s.addLocked(store.TurnEndKey(turnKey), msg), nil
}

func (s *memSession) LoadMemory(_ context.Context, _ store.MemoryScope, userID string) (store.Memory, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.memory[userID], nil
}

// SaveMemory writes as Postgres does: only over the version expected.
func (s *memSession) SaveMemory(_ context.Context, _ store.MemoryScope, userID, content string, expected int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.memory[userID]
	if current.Version != expected {
		return 0, store.ErrMemoryConflict
	}
	s.memory[userID] = store.Memory{Content: content, Version: expected + 1}
	return expected + 1, nil
}

// fakeModel answers the n-th request (from 1) it is sent with answer, and
// keeps them: the requests the LLM activity built.
type fakeModel struct {
	mu       sync.Mutex
	answer   func(n int, req provider.ChatRequest) (provider.ChatResponse, error)
	requests []provider.ChatRequest
}

func (m *fakeModel) Chat(_ context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
	m.mu.Lock()
	m.requests = append(m.requests, req)
	n := len(m.requests)
	m.mu.Unlock()
	return m.answer(n, req)
}

// sent is every request the model received.
func (m *fakeModel) sent() []provider.ChatRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.requests)
}

// promptFunc builds an agent's base prompt.
type promptFunc func(agentID string, tools []string) string

func (f promptFunc) AgentPrompt(agentID string, tools []string) string { return f(agentID, tools) }

// llmFakes is the real LLM and memory activities over fakes.
type llmFakes struct {
	session *memSession
	model   *fakeModel
	catalog *activity.Catalog
	llm     *activity.LLMActivities

	mu     sync.Mutex
	inputs []int // size of each CallLLM input, as Temporal records it
}

// registerLLM registers CallLLM and PersistContext: the real
// activities, over a session in memory, a catalog that knows the tools the
// tests use, a base prompt "prompt", and a model answering with answer.
func registerLLM(env *testsuite.TestWorkflowEnvironment, answer func(n int, req provider.ChatRequest) (provider.ChatResponse, error)) *llmFakes {
	schema := json.RawMessage(`{"type":"object"}`)
	catalog := activity.NewCatalog()
	catalog.SetTools([]store.ToolRecord{{Name: "exec", InputSchema: schema}, {Name: "web_fetch", InputSchema: schema}, {Name: "web_search", InputSchema: schema}})
	f := &llmFakes{
		session: &memSession{memory: map[string]store.Memory{}},
		model:   &fakeModel{answer: answer},
		catalog: catalog,
	}
	f.llm = &activity.LLMActivities{
		Provider: f.model,
		Store:    f.session,
		Catalog:  catalog,
		Prompts:  promptFunc(func(string, []string) string { return "prompt" }),
	}
	env.RegisterActivityWithOptions(func(ctx context.Context, req activity.LLMTurnRequest) (activity.LLMTurnResponse, error) {
		encoded, _ := json.Marshal(req)
		f.mu.Lock()
		f.inputs = append(f.inputs, len(encoded))
		f.mu.Unlock()
		return f.llm.CallLLM(ctx, req)
	}, sdkactivity.RegisterOptions{Name: "CallLLM"})
	memAct := &activity.MemoryActivities{Store: f.session}
	env.RegisterActivityWithOptions(memAct.PersistContext, sdkactivity.RegisterOptions{Name: "PersistContext"})
	return f
}

// inputSizes is the size of each CallLLM input, in order.
func (f *llmFakes) inputSizes() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.inputs)
}

// answers is a model that answers its requests in turn, the last one again
// past the end.
func answers(responses ...provider.ChatResponse) func(int, provider.ChatRequest) (provider.ChatResponse, error) {
	return func(n int, _ provider.ChatRequest) (provider.ChatResponse, error) {
		return responses[min(n, len(responses))-1], nil
	}
}

// done is a final answer.
var done = provider.ChatResponse{Content: "done", StopReason: "end_turn"}

// textOf is a converted message's text.
func textOf(m provider.ChatMessage) string {
	var s string
	json.Unmarshal(m.Content, &s)
	return s
}
