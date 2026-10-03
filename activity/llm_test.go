package activity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"

	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/skill"
	"github.com/victor/temporal-agent/store"
)

// failingProvider answers every request with err.
type failingProvider struct{ err error }

func (p failingProvider) Chat(context.Context, provider.ChatRequest) (provider.ChatResponse, error) {
	return provider.ChatResponse{}, p.err
}

// recordingModel answers every request with done, and keeps them.
type recordingModel struct{ requests []provider.ChatRequest }

func (m *recordingModel) Chat(_ context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
	m.requests = append(m.requests, req)
	return provider.ChatResponse{Content: "done", StopReason: "end_turn"}, nil
}

// memConversation is the store in memory, read as Postgres reads it.
type memConversation struct {
	messages []store.MessageWithID
	memory   map[string]store.Memory
	memErr   error
}

// add stores m under key, numbered after the others.
func (s *memConversation) add(key string, m store.Message) int64 {
	id := int64(len(s.messages) + 1)
	s.messages = append(s.messages, store.MessageWithID{ID: id, Key: key, Message: m})
	return id
}

func (s *memConversation) LoadConversation(_ context.Context, _ string, upTo int64, turnKeys []string) ([]store.MessageWithID, error) {
	var out []store.MessageWithID
	for _, m := range s.messages {
		if store.TurnReads(m.ID, m.Key, upTo, turnKeys) {
			out = append(out, m)
		}
	}
	return out, nil
}

func (s *memConversation) LoadMemory(_ context.Context, _ store.MemoryScope, userID string) (store.Memory, error) {
	return s.memory[userID], s.memErr
}

// promptOf is a PromptBuilder naming the agent and its tools.
type promptOf struct{}

func (promptOf) AgentPrompt(agentID string, tools []string) string {
	return fmt.Sprintf("I am %s, with %v.", agentID, tools)
}

// newLLM is an LLMActivities over st, an empty catalog and model.
func newLLM(model provider.LLMProvider, st ConversationLoader) *LLMActivities {
	return &LLMActivities{Provider: model, Store: st, Catalog: NewCatalog(), Prompts: promptOf{}}
}

// what lists the text of each message the model read, with its role.
func what(req provider.ChatRequest) string {
	var lines []string
	for _, m := range req.Messages {
		var s string
		json.Unmarshal(m.Content, &s)
		switch {
		case m.ToolResult != nil:
			s = "result " + m.ToolResult.Content
		case len(m.ToolCalls) > 0:
			s = "call " + m.ToolCalls[0].Name
		}
		lines = append(lines, m.Role+": "+s)
	}
	return strings.Join(lines, " | ")
}

// What the API says of its failure becomes Temporal's retry: never for a
// refused request, after the wait it asked for, or by the policy.
func TestCallLLM_TranslatesTheRetry(t *testing.T) {
	apiErr := errors.New("anthropic API error (status 429)")
	for _, c := range []struct {
		name         string
		err          error
		nonRetryable bool
		delay        time.Duration
	}{
		{"wait asked", &provider.RetryAfterError{Err: apiErr, Delay: 7 * time.Second}, false, 7 * time.Second},
		{"refused", &provider.PermanentAPIError{Err: apiErr}, true, 0},
	} {
		a := newLLM(failingProvider{c.err}, nil)
		_, err := a.CallLLM(context.Background(), LLMTurnRequest{AgentID: "default"})
		var appErr *temporal.ApplicationError
		if !errors.As(err, &appErr) {
			t.Errorf("%s: %v, want an application error", c.name, err)
			continue
		}
		if appErr.NonRetryable() != c.nonRetryable || appErr.NextRetryDelay() != c.delay || appErr.Message() != apiErr.Error() {
			t.Errorf("%s: non-retryable %v, next retry %s, message %q; want %v, %s, the API's", c.name, appErr.NonRetryable(), appErr.NextRetryDelay(), appErr.Message(), c.nonRetryable, c.delay)
		}
	}

	// The model refusing a prompt too long reads as the guard does: never
	// retried, with what to do.
	tooLong := &provider.PermanentAPIError{Err: fmt.Errorf("%w: %w", provider.ErrContextTooLong, errors.New("prompt is too long"))}
	_, err := newLLM(failingProvider{tooLong}, nil).CallLLM(context.Background(), LLMTurnRequest{AgentID: "default"})
	var tooLongErr *temporal.ApplicationError
	if !errors.As(err, &tooLongErr) || tooLongErr.Type() != ErrContextTooLong || !tooLongErr.NonRetryable() || tooLongErr.Message() != ContextTooLongMessage {
		t.Errorf("prompt too long: %v, want the non-retryable %s", err, ErrContextTooLong)
	}

	// Anything else is left to the retry policy.
	_, err = newLLM(failingProvider{apiErr}, nil).CallLLM(context.Background(), LLMTurnRequest{AgentID: "default"})
	var appErr *temporal.ApplicationError
	if err != apiErr || errors.As(err, &appErr) {
		t.Errorf("plain error: %v", err)
	}
}

// A turn reads the session as it was when its message started, the turns
// that answered it before, and its own messages: a person's message written
// meanwhile is not read, it is the next one to answer.
func TestCallLLM_ReadsTheTurnFromItsSnapshot(t *testing.T) {
	st := &memConversation{}
	st.add(store.HumanMessageKey("old"), store.Message{Role: store.RoleUser, Content: text("earlier")})
	upTo := st.add(store.HumanMessageKey("q"), store.Message{Role: store.RoleUser, Content: text("@jarvis cherche, @smith juge"), Author: "Alice"})
	group := store.TurnGroupKey("run-1", upTo)
	jarvis, smith := store.TurnKey(group, 0), store.TurnKey(group, 1)
	st.add(store.TurnMessageKey(jarvis, 0), store.Message{Role: store.RoleAssistant, Content: text("trouvé"), AgentID: "jarvis", Author: "Jarvis"})
	st.add(store.HumanMessageKey("m"), store.Message{Role: store.RoleUser, Content: text("meanwhile"), Author: "Bob"})
	st.add(store.TurnMessageKey(smith, 0), store.Message{Role: store.RoleAssistant, AgentID: "smith", ToolCalls: []store.ToolCall{{ID: "t1", Name: "web_fetch"}}})
	st.add(store.TurnMessageKey(smith, 1), store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t1", Content: "page"}})

	model := &recordingModel{}
	_, err := newLLM(model, st).CallLLM(context.Background(), LLMTurnRequest{AgentID: "smith", History: &TurnHistory{
		SessionID: "s1", UpTo: upTo, EarlierTurns: []string{jarvis}, TurnKey: smith,
	}})
	if err != nil {
		t.Fatal(err)
	}
	want := "user: earlier\n\n[Alice] @jarvis cherche, @smith juge\n\n[agent Jarvis] trouvé | assistant: call web_fetch | tool: result page"
	if got := what(model.requests[0]); got != want {
		t.Errorf("model read %q\nwant %q", got, want)
	}
}

// A flush that failed leaves the turn's last messages unwritten: the call
// reads them from the request, once, even when one of them was written after
// all.
func TestCallLLM_ReadsTheUnwrittenTailOnce(t *testing.T) {
	st := &memConversation{}
	upTo := st.add(store.HumanMessageKey("q"), store.Message{Role: store.RoleUser, Content: text("go")})
	turn := "run-1.0"
	call := store.Message{Role: store.RoleAssistant, ToolCalls: []store.ToolCall{{ID: "t1", Name: "web_fetch"}}}
	result := store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t1", Content: "page"}}
	answer := store.Message{Role: store.RoleAssistant, Content: text("read")}
	st.add(store.TurnMessageKey(turn, 0), call)
	// The flush of 1 and 2 wrote 1, then failed: the workflow holds both.
	st.add(store.TurnMessageKey(turn, 1), result)

	model := &recordingModel{}
	_, err := newLLM(model, st).CallLLM(context.Background(), LLMTurnRequest{AgentID: "default", History: &TurnHistory{
		SessionID: "s1", UpTo: upTo, TurnKey: turn, Tail: []store.Message{result, answer}, TailStart: 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := what(model.requests[0]), "user: go | assistant: call web_fetch | tool: result page | assistant: read"; got != want {
		t.Errorf("model read %q\nwant %q", got, want)
	}
}

// A sub-agent or a scheduled task gives its conversation inline: the store is
// not read. Inline and by reference at once is refused.
func TestCallLLM_InlineConversation(t *testing.T) {
	model := &recordingModel{}
	a := newLLM(model, nil) // a store read would panic
	req := LLMTurnRequest{AgentID: "analyst", Messages: []store.Message{{Role: store.RoleUser, Content: text("summarize the CAC 40")}}}
	if _, err := a.CallLLM(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if got := what(model.requests[0]); got != "user: summarize the CAC 40" {
		t.Errorf("model read %q", got)
	}

	req.History = &TurnHistory{SessionID: "s1", TurnKey: "run-1.0"}
	var appErr *temporal.ApplicationError
	if _, err := a.CallLLM(context.Background(), req); !errors.As(err, &appErr) || !appErr.NonRetryable() {
		t.Errorf("both inline and by reference: %v, want a refusal", err)
	}
}

// The model is offered the tools the turn dispatches, as the catalog defines
// them; one gone from the catalog since is dropped, from the tools and from
// the prompt.
func TestCallLLM_OffersTheToolsByName(t *testing.T) {
	c := NewCatalog()
	c.SetAgents([]AgentCatalogEntry{
		{ID: "default", Tools: []string{"*"}},
		{ID: "analyst", Name: "Analyst", Description: "Markets."},
	})
	c.SetTools([]store.ToolRecord{{Name: "web_fetch", Description: "Fetch a page.", InputSchema: json.RawMessage(`{"type":"object"}`)}})
	allowed := c.AllowedTools("default")

	model := &recordingModel{}
	a := &LLMActivities{Provider: model, Catalog: c, Prompts: promptOf{}}
	_, err := a.CallLLM(context.Background(), LLMTurnRequest{AgentID: "default", Tools: []string{"agent_analyst", "exec", "web_fetch"},
		Messages: []store.Message{{Role: store.RoleUser, Content: text("go")}}})
	if err != nil {
		t.Fatal(err)
	}
	req := model.requests[0]
	want := allowed.Tools
	want[len(want)-1].CacheBreakpoint = true
	got, _ := json.Marshal(req.Tools)
	wantJSON, _ := json.Marshal(want)
	if string(got) != string(wantJSON) {
		t.Errorf("offered %s\nwant %s", got, wantJSON)
	}
	if req.System != "I am default, with [agent_analyst web_fetch]." {
		t.Errorf("prompt %q, want the tools offered only", req.System)
	}
}

// A tool withdrawn from the catalog while the turn uses it keeps a stub
// definition, which tells the model not to call it: the conversation holds
// its tool blocks, which the API refuses in a request that defines no tool.
// A withdrawn tool the conversation never called is not offered, and neither
// is in the prompt.
func TestCallLLM_KeepsAWithdrawnToolItCalled(t *testing.T) {
	calls := []store.Message{
		{Role: store.RoleUser, Content: text("go")},
		{Role: store.RoleAssistant, ToolCalls: []store.ToolCall{{ID: "t1", Name: "gone", Input: json.RawMessage(`{}`)}}},
		{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t1", Content: "ok"}},
	}
	for _, tc := range []struct {
		name    string
		catalog []store.ToolRecord
	}{
		{"other tools left", []store.ToolRecord{{Name: "web_fetch", InputSchema: json.RawMessage(`{"type":"object"}`)}}},
		{"its only tool", nil},
	} {
		catalog := tc.catalog
		t.Run(tc.name, func(t *testing.T) {
			c := NewCatalog()
			c.SetTools(catalog)
			model := &recordingModel{}
			a := &LLMActivities{Provider: model, Catalog: c, Prompts: promptOf{}}
			if _, err := a.CallLLM(context.Background(), LLMTurnRequest{AgentID: "default", Tools: []string{"gone", "unused", "web_fetch"}, Messages: calls}); err != nil {
				t.Fatal(err)
			}
			req := model.requests[0]
			var offered []string
			for _, d := range req.Tools {
				offered = append(offered, d.Name)
				if d.Name == "gone" && (d.Description != withdrawnDescription || string(d.InputSchema) != `{"type":"object"}`) {
					t.Errorf("withdrawn tool defined as %+v", d)
				}
			}
			want := []string{"gone"}
			prompt := "I am default, with []."
			if catalog != nil {
				want = []string{"web_fetch", "gone"}
				prompt = "I am default, with [web_fetch]."
			}
			if !reflect.DeepEqual(offered, want) {
				t.Errorf("offered %v, want %v", offered, want)
			}
			if req.System != prompt {
				t.Errorf("prompt %q, want %q", req.System, prompt)
			}
		})
	}
}

// A conversation past the bound fails the call for good, with what the
// members must do; the model is not called.
func TestCallLLM_RefusesAConversationTooLong(t *testing.T) {
	model := &recordingModel{}
	a := newLLM(model, nil)
	a.MaxContextBytes = 10_000
	_, err := a.CallLLM(context.Background(), LLMTurnRequest{AgentID: "default",
		Messages: []store.Message{{Role: store.RoleUser, Content: text(strings.Repeat("x", 20_000))}}})

	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) || !appErr.NonRetryable() || appErr.Type() != ErrContextTooLong || appErr.Message() != ContextTooLongMessage {
		t.Errorf("error %v, want a non-retryable %s saying to fork", err, ErrContextTooLong)
	}
	if len(model.requests) != 0 {
		t.Error("the model was called")
	}

	// Under the bound, the call goes through.
	a.MaxContextBytes = 0
	if _, err := a.CallLLM(context.Background(), LLMTurnRequest{AgentID: "default",
		Messages: []store.Message{{Role: store.RoleUser, Content: text(strings.Repeat("x", 20_000))}}}); err != nil {
		t.Errorf("under the default bound: %v", err)
	}
}

// The prompt is the one the workflow used to build: the agent's (or the
// override), its user's memory, then the part note. A memory that cannot be
// read leaves its section out.
func TestCallLLM_BuildsThePromptAsBefore(t *testing.T) {
	c := NewCatalog()
	c.SetAgents([]AgentCatalogEntry{{ID: "smith", Name: "Agent Smith", Mention: "smith", Skills: []string{"review"}}})
	skills := NewSkillActivities([]skill.Skill{{Name: "review", Content: "REVIEW SKILL"}}, c)
	base, _ := skills.LoadSkillsForAgent(context.Background(), LoadSkillsForAgentInput{AgentID: "smith"})
	st := &memConversation{memory: map[string]store.Memory{"u-alice": {Content: "likes tea", Version: 3}}}

	for _, tc := range []struct {
		name   string
		prompt PromptRef
		memErr error
		want   string
	}{
		{"agent", PromptRef{MemoryOf: "u-alice", UserName: "Alice", PartNote: "\n## PART\n"},
			nil, base.SystemPrompt + userMemorySection("Alice", "likes tea") + "\n## PART\n"},
		{"override", PromptRef{Override: "OVERRIDE", MemoryOf: "u-alice"}, nil, "OVERRIDE" + userMemorySection("", "likes tea")},
		{"memory unread", PromptRef{MemoryOf: "u-alice"}, errors.New("db down"), base.SystemPrompt},
		{"no memory", PromptRef{}, nil, base.SystemPrompt},
	} {
		st.memErr = tc.memErr
		model := &recordingModel{}
		a := &LLMActivities{Provider: model, Store: st, Catalog: c, Prompts: skills.Prompts}
		if _, err := a.CallLLM(context.Background(), LLMTurnRequest{AgentID: "smith", Prompt: tc.prompt,
			Messages: []store.Message{{Role: store.RoleUser, Content: text("go")}}}); err != nil {
			t.Fatal(err)
		}
		if got := model.requests[0].System; got != tc.want {
			t.Errorf("%s: prompt %q\nwant %q", tc.name, got, tc.want)
		}
	}
	if !strings.Contains(base.SystemPrompt, "Your name is Agent Smith") || !strings.Contains(base.SystemPrompt, "REVIEW SKILL") {
		t.Errorf("base prompt %q lacks the identity or the skill", base.SystemPrompt)
	}
}

// The answer says which version of the user's memory the prompt held, the
// one the model read: 0 for a memory never saved, none when the prompt held
// no memory, asked for (unread) or not.
func TestCallLLM_ReturnsTheMemoryVersionInThePrompt(t *testing.T) {
	st := &memConversation{memory: map[string]store.Memory{"u-alice": {Content: "likes tea", Version: 3}}}
	msgs := []store.Message{{Role: store.RoleUser, Content: text("go")}}
	for _, tc := range []struct {
		name   string
		of     string
		memErr error
		want   *int64
		unread bool
	}{
		{"saved", "u-alice", nil, ptr(int64(3)), false},
		{"never saved", "u-bob", nil, ptr(int64(0)), false},
		{"unread", "u-alice", errors.New("db down"), nil, true},
		{"not asked", "", nil, nil, false},
	} {
		st.memErr = tc.memErr
		model := &recordingModel{}
		resp, err := newLLM(model, st).CallLLM(context.Background(), LLMTurnRequest{AgentID: "smith", Prompt: PromptRef{MemoryOf: tc.of}, Messages: msgs})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(resp.MemoryVersion, tc.want) || resp.MemoryUnread != tc.unread || resp.Content != "done" {
			t.Errorf("%s: answer %+v, memory version %v (unread %v), want %v (unread %v)", tc.name, resp.ChatResponse, deref(resp.MemoryVersion), resp.MemoryUnread, deref(tc.want), tc.unread)
		}
		if inPrompt := strings.Contains(model.requests[0].System, "likes tea"); inPrompt != (tc.name == "saved") {
			t.Errorf("%s: memory in the prompt = %v", tc.name, inPrompt)
		}
	}
}

func ptr[T any](v T) *T { return &v }

// deref shows a version or its absence.
func deref(v *int64) string {
	if v == nil {
		return "none"
	}
	return fmt.Sprint(*v)
}

// Another agent's call to a private tool reaches the model as the members
// see it, whichever agent reads: the catalog says which tools are private.
func TestCallLLM_HidesPrivateInputs(t *testing.T) {
	c := NewCatalog()
	c.SetAgents([]AgentCatalogEntry{{ID: "jarvis", Name: "Jarvis", Mention: "jarvis"}, {ID: "smith"}})
	c.SetTools([]store.ToolRecord{{Name: "save_user_memory", PrivateInput: true}})
	model := &recordingModel{}
	a := &LLMActivities{Provider: model, Catalog: c, Prompts: promptOf{}}
	_, err := a.CallLLM(context.Background(), LLMTurnRequest{AgentID: "smith", Messages: []store.Message{
		{Role: store.RoleUser, Content: text("remember")},
		{Role: store.RoleAssistant, AgentID: "jarvis", ToolCalls: []store.ToolCall{{ID: "t1", Name: "save_user_memory", Input: json.RawMessage(`{"content":"Alice's secret"}`)}}},
		{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t1", Content: "saved"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	got := what(model.requests[0])
	if strings.Contains(got, "secret") || !strings.Contains(got, `[agent Jarvis (@jarvis) called save_user_memory {"content":"(private)"}]`) {
		t.Errorf("model read %q", got)
	}
}

// A request that cannot be encoded is past the guard, never under it.
func TestRequestSize_UnencodableIsPastAnyLimit(t *testing.T) {
	bad := provider.ChatRequest{Messages: []provider.ChatMessage{{Role: "user", Content: json.RawMessage(`{not json`)}}}
	if got := requestSize(bad); got != math.MaxInt {
		t.Errorf("requestSize = %d, want math.MaxInt", got)
	}
}
