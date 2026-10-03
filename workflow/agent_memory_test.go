package workflow

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/tool"
)

// registerMemoryTool offers save_user_memory, the real tool over f's store,
// and returns the inputs of its calls.
func registerMemoryTool(env *testsuite.TestWorkflowEnvironment, f *llmFakes) func() []activity.ExecuteToolInput {
	schema := json.RawMessage(`{"type":"object"}`)
	f.catalog.SetTools([]store.ToolRecord{{Name: "save_user_memory", InputSchema: schema, PrivateInput: true, NeedsCallContext: true}})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.ListToolsInput) (activity.ListToolsOutput, error) {
		return activity.ListToolsOutput{
			Tools: []provider.ToolDefinition{{Name: "save_user_memory", InputSchema: schema}},
			Resolutions: map[string]activity.ToolResolution{
				"save_user_memory": {Kind: "activity", TaskQueue: "tools", PrivateInput: true, NeedsCallContext: true},
			},
		}, nil
	}, sdkactivity.RegisterOptions{Name: "ListTools"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.LoadSkillsForAgentInput) (activity.LoadSkillsForAgentOutput, error) {
		return activity.LoadSkillsForAgentOutput{SystemPrompt: "prompt"}, nil
	}, sdkactivity.RegisterOptions{Name: "LoadSkillsForAgent"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.NotifyInput) error {
		return nil
	}, sdkactivity.RegisterOptions{Name: "NotifyStep"})

	registry := tool.NewRegistry()
	tool.RegisterMemoryTools(registry, f.session)
	tools := &activity.ToolActivities{Registry: registry}
	var mu sync.Mutex
	var inputs []activity.ExecuteToolInput
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.ExecuteToolInput) (activity.ExecuteToolOutput, error) {
		mu.Lock()
		inputs = append(inputs, in)
		mu.Unlock()
		return tools.ExecuteTool(ctx, in)
	}, sdkactivity.RegisterOptions{Name: "ExecuteTool"})
	return func() []activity.ExecuteToolInput {
		mu.Lock()
		defer mu.Unlock()
		return append([]activity.ExecuteToolInput(nil), inputs...)
	}
}

// saveMemory is a model call to save_user_memory.
func saveMemory(id, content string) provider.ChatResponse {
	input, _ := json.Marshal(map[string]string{"content": content})
	return provider.ChatResponse{ToolCalls: []provider.ToolCallInfo{{ID: id, Name: "save_user_memory", Input: input}}}
}

// toolResult is the result of call id among what the model read.
func toolResult(req provider.ChatRequest, id string) *provider.ToolResultInfo {
	for _, m := range req.Messages {
		if m.ToolResult != nil && m.ToolResult.ToolCallID == id {
			return m.ToolResult
		}
	}
	return nil
}

// save_user_memory replaces the version of the memory the model read at the
// LLM call that made it. Another session saving in between makes it a
// conflict, an error result that does not repeat the memory; the next call
// reads that memory with its version in its prompt, and the merged save
// succeeds. A session turn and a scheduled task alike.
func TestAgentWorkflow_SaveMemoryFromTheVersionTheModelRead(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input AgentWorkflowInput
	}{
		{"session turn", AgentWorkflowInput{SessionID: "s1", TurnKey: "run-1"}},
		{"scheduled task", AgentWorkflowInput{SessionID: "schedule-1", LoadUserMemory: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			var f *llmFakes
			f = registerLLM(env, func(n int, _ provider.ChatRequest) (provider.ChatResponse, error) {
				switch n {
				case 1:
					// Another session saves while this call answers.
					if _, err := f.session.SaveMemory(context.Background(), store.MemoryScopeUser, "u-alice", "likes tea; lives in Lyon", 3); err != nil {
						t.Error(err)
					}
					return saveMemory("m1", "likes tea and coffee"), nil
				case 2:
					return saveMemory("m2", "likes tea and coffee; lives in Lyon"), nil
				}
				return done, nil
			})
			f.session.memory["u-alice"] = store.Memory{Content: "likes tea", Version: 3}
			calls := registerMemoryTool(env, f)

			in := tc.input
			in.UserID, in.UserName, in.AgentID, in.UserMessage = "u-alice", "Alice", "default", "I like coffee too"
			env.ExecuteWorkflow(AgentWorkflow, in)
			if err := env.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}

			got := calls()
			if len(got) != 2 || got[0].Call == nil || got[1].Call == nil {
				t.Fatalf("tool calls %+v, want two, each with its call context", got)
			}
			if v := *got[0].Call.MemoryVersion; v != 3 {
				t.Errorf("first save from version %d, want 3: the one the first call read", v)
			}
			if v := *got[1].Call.MemoryVersion; v != 4 {
				t.Errorf("second save from version %d, want 4: the one the second call read", v)
			}

			sent := f.model.sent()
			if len(sent) != 3 {
				t.Fatalf("%d LLM calls, want 3", len(sent))
			}
			if r := toolResult(sent[1], "m1"); r == nil || !r.IsError || !strings.Contains(r.Content, "changed since you read it") || strings.Contains(r.Content, "Lyon") {
				t.Errorf("conflict result %+v, want a conflict that does not repeat the memory", r)
			}
			if !strings.Contains(sent[1].System, "lives in Lyon") {
				t.Errorf("the second call did not read the new memory: %q", sent[1].System)
			}
			if r := toolResult(sent[2], "m2"); r == nil || r.IsError || r.Content != "Memory saved." {
				t.Errorf("retry result %+v", r)
			}
			if m := f.session.memory["u-alice"]; m != (store.Memory{Content: "likes tea and coffee; lives in Lyon", Version: 5}) {
				t.Errorf("memory %+v", m)
			}
		})
	}
}

// A sub-agent's prompt holds no memory: its save would replace what it never
// read, and is refused.
func TestAgentWorkflow_SubAgentCannotSaveMemoryBlind(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	f := registerLLM(env, answers(saveMemory("m1", "overwritten"), done))
	f.session.memory["u-alice"] = store.Memory{Content: "likes tea", Version: 3}
	calls := registerMemoryTool(env, f)

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{
		SessionID: "s1-tool-agent_helper-1", UserID: "u-alice", AgentID: "helper", UserMessage: "remember coffee",
		AgentChain: []string{"default"},
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if got := calls(); len(got) != 1 || got[0].Call == nil || got[0].Call.MemoryVersion != nil {
		t.Errorf("tool calls %+v, want one with no memory version", got)
	}
	if r := toolResult(f.model.sent()[1], "m1"); r == nil || !r.IsError || !strings.Contains(r.Content, "a sub-agent is given none") {
		t.Errorf("result %+v", r)
	}
	if m := f.session.memory["u-alice"]; m != (store.Memory{Content: "likes tea", Version: 3}) {
		t.Errorf("memory %+v", m)
	}
}

// Two saves in one answer read the same version: the first to reach the store
// replaces it, the other is a conflict, and nothing of either is lost
// silently.
func TestAgentWorkflow_TwoSavesInOneAnswer(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	both := provider.ChatResponse{ToolCalls: []provider.ToolCallInfo{
		saveMemory("m1", "likes coffee").ToolCalls[0],
		saveMemory("m2", "likes cake").ToolCalls[0],
	}}
	f := registerLLM(env, answers(both, done))
	f.session.memory["u-alice"] = store.Memory{Content: "likes tea", Version: 3}
	calls := registerMemoryTool(env, f)

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{
		SessionID: "s1", TurnKey: "run-1", UserID: "u-alice", UserName: "Alice", AgentID: "default", UserMessage: "coffee and cake",
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	got := calls()
	if len(got) != 2 || got[0].Call == nil || got[1].Call == nil || *got[0].Call.MemoryVersion != 3 || *got[1].Call.MemoryVersion != 3 {
		t.Fatalf("tool calls %+v, want two from version 3", got)
	}
	sent := f.model.sent()
	r1, r2 := toolResult(sent[1], "m1"), toolResult(sent[1], "m2")
	if r1 == nil || r2 == nil {
		t.Fatalf("results %+v, %+v", r1, r2)
	}
	won, lost := r1, r2
	if r1.IsError {
		won, lost = r2, r1
	}
	if won.IsError || won.Content != "Memory saved." || !lost.IsError || !strings.Contains(lost.Content, "changed since you read it") {
		t.Errorf("results %+v, %+v: want one saved, one conflict", r1, r2)
	}
	if m := f.session.memory["u-alice"]; m.Version != 4 || (m.Content != "likes coffee" && m.Content != "likes cake") {
		t.Errorf("memory %+v, want one save at version 4", m)
	}
}

// The agent's turn stores the user it answers on its messages: answering Bob
// later in the same session, it reads its save for Alice as the members do,
// the call and its result kept, their contents hidden.
func TestAgentWorkflow_AnotherMembersSaveIsHidden(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	alice := registerLLM(env, answers(saveMemory("m1", "Alice drinks tea"), done))
	registerMemoryTool(env, alice)
	upTo := alice.session.add(store.HumanMessageKey("a"), store.Message{Role: store.RoleUser, Content: `"I drink tea"`, UserID: "u-alice", Author: "Alice"})
	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{
		SessionID: "s1", UserID: "u-alice", UserName: "Alice", AgentID: "default",
		UserMessage: "I drink tea", UserMessageStored: true, TurnKey: "run-1@1.0", HistoryUpTo: upTo,
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	stored := alice.session.history()
	for _, m := range stored {
		if m.Role == store.RoleAssistant && m.UserID != "u-alice" {
			t.Errorf("assistant message %+v, want the user it answered", m.Message)
		}
	}

	env = suite.NewTestWorkflowEnvironment()
	bob := registerLLM(env, answers(done))
	registerMemoryTool(env, bob)
	bob.session.messages = stored
	upTo = bob.session.add(store.HumanMessageKey("b"), store.Message{Role: store.RoleUser, Content: `"what about Alice?"`, UserID: "u-bob", Author: "Bob"})
	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{
		SessionID: "s1", UserID: "u-bob", UserName: "Bob", AgentID: "default",
		UserMessage: "what about Alice?", UserMessageStored: true, TurnKey: "run-2@2.0", HistoryUpTo: upTo,
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	seen := bob.model.sent()[0]
	if b, _ := json.Marshal(seen.Messages); strings.Contains(string(b), "drinks tea") || strings.Contains(string(b), "Memory saved") {
		t.Errorf("Bob's turn read Alice's save: %s", b)
	}
	var call *provider.ToolCallInfo
	for _, m := range seen.Messages {
		for i := range m.ToolCalls {
			call = &m.ToolCalls[i]
		}
	}
	if call == nil || call.ID != "m1" || string(call.Input) != `{"content":"(private)"}` {
		t.Errorf("call %+v, want m1 hidden", call)
	}
	if r := toolResult(seen, "m1"); r == nil || r.Content != "(private)" {
		t.Errorf("result %+v, want m1's, hidden", r)
	}
}
