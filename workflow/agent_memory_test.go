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
// conflict, an error result holding the memory now; the next call reads that
// memory with its version, and the merged save succeeds. A session turn and a
// scheduled task alike.
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
			if r := toolResult(sent[1], "m1"); r == nil || !r.IsError || !strings.Contains(r.Content, "changed elsewhere") || !strings.Contains(r.Content, "lives in Lyon") {
				t.Errorf("conflict result %+v", r)
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
	if r := toolResult(f.model.sent()[1], "m1"); r == nil || !r.IsError || !strings.Contains(r.Content, "not in your prompt") {
		t.Errorf("result %+v", r)
	}
	if m := f.session.memory["u-alice"]; m != (store.Memory{Content: "likes tea", Version: 3}) {
		t.Errorf("memory %+v", m)
	}
}
