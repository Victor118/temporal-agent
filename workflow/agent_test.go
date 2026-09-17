package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	enumspb "go.temporal.io/api/enums/v1"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/store"
)

// TestAgentWorkflow_ToolDispatch checks that an allowed tool runs on its own
// task queue and that a tool outside the agent's allowlist is refused without
// being executed.
func TestAgentWorkflow_ToolDispatch(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.ListToolsInput) (activity.ListToolsOutput, error) {
		if in.AgentID != "reviewer" {
			t.Errorf("ListTools agent = %q, want reviewer", in.AgentID)
		}
		return activity.ListToolsOutput{
			Tools: []provider.ToolDefinition{{Name: "web_fetch", InputSchema: json.RawMessage(`{"type":"object"}`)}},
			Resolutions: map[string]activity.ToolResolution{
				"web_fetch": {Kind: "activity", TaskQueue: "tools-web"},
			},
		}, nil
	}, sdkactivity.RegisterOptions{Name: "ListTools"})

	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.LoadSkillsForAgentInput) (activity.LoadSkillsForAgentOutput, error) {
		return activity.LoadSkillsForAgentOutput{SystemPrompt: "prompt"}, nil
	}, sdkactivity.RegisterOptions{Name: "LoadSkillsForAgent"})

	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.NotifyInput) error {
		return nil
	}, sdkactivity.RegisterOptions{Name: "NotifyStep"})

	var executed []string
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.ExecuteToolInput) (activity.ExecuteToolOutput, error) {
		executed = append(executed, in.Name)
		if q := sdkactivity.GetInfo(ctx).TaskQueue; q != "tools-web" {
			t.Errorf("%s ran on task queue %q, want tools-web", in.Name, q)
		}
		return activity.ExecuteToolOutput{Content: "page content"}, nil
	}, sdkactivity.RegisterOptions{Name: "ExecuteTool"})

	var secondRequest provider.ChatRequest
	calls := 0
	env.RegisterActivityWithOptions(func(ctx context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
		calls++
		if calls == 1 {
			return provider.ChatResponse{ToolCalls: []provider.ToolCallInfo{
				{ID: "1", Name: "web_fetch", Input: json.RawMessage(`{}`)},
				{ID: "2", Name: "exec", Input: json.RawMessage(`{}`)},
			}}, nil
		}
		secondRequest = req
		return provider.ChatResponse{Content: "done", StopReason: "end_turn"}, nil
	}, sdkactivity.RegisterOptions{Name: "CallLLM"})

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{
		SessionID:   "s1",
		AgentID:     "reviewer",
		UserMessage: "hello",
	})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if fmt.Sprint(executed) != "[web_fetch]" {
		t.Errorf("executed tools = %v, want [web_fetch]", executed)
	}

	results := map[string]*provider.ToolResultInfo{}
	for _, m := range secondRequest.Messages {
		if m.ToolResult != nil {
			results[m.ToolResult.ToolCallID] = m.ToolResult
		}
	}
	if r := results["1"]; r == nil || r.IsError || r.Content != "page content" {
		t.Errorf("web_fetch result = %+v", r)
	}
	if r := results["2"]; r == nil || !r.IsError || r.Content != `Tool "exec" is not available to this agent.` {
		t.Errorf("exec result = %+v", r)
	}
}

func TestIsScheduleToStartTimeout(t *testing.T) {
	scheduleToStart := temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START, nil)
	startToClose := temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_START_TO_CLOSE, nil)

	if !isScheduleToStartTimeout(fmt.Errorf("activity error: %w", scheduleToStart)) {
		t.Error("wrapped schedule-to-start timeout not detected")
	}
	if isScheduleToStartTimeout(startToClose) {
		t.Error("start-to-close timeout must not be reported as unavailable")
	}
	if isScheduleToStartTimeout(fmt.Errorf("boom")) {
		t.Error("plain error must not be reported as unavailable")
	}
}

func TestAgentWorkflow_RequiresAgentID(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{SessionID: "s1", UserMessage: "hello"})

	err := env.GetWorkflowError()
	if err == nil || !strings.Contains(err.Error(), "agent_id is required") {
		t.Fatalf("got error %v, want agent_id is required", err)
	}
}

func TestBuildChildInput_SpawnSession(t *testing.T) {
	known := map[string]bool{"default": true, "market-analyst": true}
	parent := AgentWorkflowInput{Model: "m"}

	t.Run("target agent runs on the current queue", func(t *testing.T) {
		res := activity.ToolResolution{WorkflowName: "AgentWorkflow", TaskQueue: "tools-core"}
		name, in, err := buildChildInput("spawn_session", json.RawMessage(`{"task":"t","agent_id":"market-analyst"}`),
			parent, "child", &res, []string{"default"}, "default", known, "agent")
		if err != nil {
			t.Fatal(err)
		}
		child := in.(AgentWorkflowInput)
		if name != "AgentWorkflow" || child.AgentID != "market-analyst" || child.Model != "m" || res.TaskQueue != "agent" {
			t.Errorf("name=%q child=%+v queue=%q", name, child, res.TaskQueue)
		}
	})

	t.Run("no agent_id spawns the current agent", func(t *testing.T) {
		res := activity.ToolResolution{WorkflowName: "AgentWorkflow"}
		_, in, err := buildChildInput("spawn_session", json.RawMessage(`{"task":"t"}`),
			parent, "child", &res, nil, "default", known, "agent")
		if err != nil {
			t.Fatal(err)
		}
		if got := in.(AgentWorkflowInput).AgentID; got != "default" {
			t.Errorf("child agent = %q, want default", got)
		}
	})

	t.Run("unknown agent is refused", func(t *testing.T) {
		res := activity.ToolResolution{WorkflowName: "AgentWorkflow"}
		_, _, err := buildChildInput("spawn_session", json.RawMessage(`{"task":"t","agent_id":"ghost"}`),
			parent, "child", &res, nil, "default", known, "agent")
		if err == nil || !strings.Contains(err.Error(), `unknown agent_id "ghost"`) {
			t.Errorf("got error %v", err)
		}
	})
}

func TestSessionToolsQueue(t *testing.T) {
	resolutions := map[string]activity.ToolResolution{
		"read_file":     {Kind: "activity", TaskQueue: "tools-fs"},
		"write_file":    {Kind: "activity", TaskQueue: "tools-fs"},
		"web_fetch":     {Kind: "activity", TaskQueue: "tools-core"},
		"spawn_session": {Kind: "workflow", TaskQueue: "tools-core"},
	}

	q, err := sessionToolsQueue([]string{"read_file", "write_file"}, resolutions)
	if err != nil || q != "tools-fs" {
		t.Errorf("got %q, %v; want tools-fs", q, err)
	}

	cases := map[string][]string{
		"not available":             {"read_file", "exec"},
		"not an activity tool":      {"spawn_session"},
		"must share one task queue": {"read_file", "web_fetch"},
	}
	for want, names := range cases {
		if _, err := sessionToolsQueue(names, resolutions); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%v: got error %v, want %q", names, err, want)
		}
	}
}

func TestWorkflowToolContent(t *testing.T) {
	cases := map[string]string{
		`"yes"`: "yes",
		`{"response":"CAC 40 summary","messages":[{"role":"user"}],"goal_achieved":true}`: "CAC 40 summary",
		`{"other":1}`: `{"other":1}`,
	}
	for raw, want := range cases {
		if got := workflowToolContent(json.RawMessage(raw)); got != want {
			t.Errorf("workflowToolContent(%s) = %q, want %q", raw, got, want)
		}
	}
}

func TestChildWorkflowID(t *testing.T) {
	if got := childWorkflowID("s1", "spawn_session", "toolu_01A", 3, 1); got != "s1-tool-spawn_session-toolu_01A" {
		t.Errorf("got %q", got)
	}
	if got := childWorkflowID("s1", "ask_user", "", 3, 1); got != "s1-tool-ask_user-3-1" {
		t.Errorf("got %q", got)
	}
}

// persistCall records one PersistContext activity call.
type persistCall struct {
	turnKey    string
	startIndex int
	roles      []string
}

// recordPersists registers a PersistContext stub collecting what the agent
// flushed, in order.
func recordPersists(env *testsuite.TestWorkflowEnvironment, out *[]persistCall) {
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.PersistContextInput) error {
		call := persistCall{turnKey: in.TurnKey, startIndex: in.StartIndex}
		for _, m := range in.Messages {
			call.roles = append(call.roles, string(m.Role))
		}
		*out = append(*out, call)
		return nil
	}, sdkactivity.RegisterOptions{Name: "PersistContext"})
}

// registerAgentStubs wires the activities every AgentWorkflow run needs.
func registerAgentStubs(env *testsuite.TestWorkflowEnvironment) {
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.LoadContextInput) (activity.LoadContextOutput, error) {
		return activity.LoadContextOutput{}, nil
	}, sdkactivity.RegisterOptions{Name: "LoadContext"})

	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.ListToolsInput) (activity.ListToolsOutput, error) {
		return activity.ListToolsOutput{
			Tools: []provider.ToolDefinition{{Name: "web_fetch", InputSchema: json.RawMessage(`{"type":"object"}`)}},
			Resolutions: map[string]activity.ToolResolution{
				"web_fetch": {Kind: "activity", TaskQueue: "tools-web"},
			},
		}, nil
	}, sdkactivity.RegisterOptions{Name: "ListTools"})

	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.LoadSkillsForAgentInput) (activity.LoadSkillsForAgentOutput, error) {
		return activity.LoadSkillsForAgentOutput{SystemPrompt: "prompt"}, nil
	}, sdkactivity.RegisterOptions{Name: "LoadSkillsForAgent"})

	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.NotifyInput) error {
		return nil
	}, sdkactivity.RegisterOptions{Name: "NotifyStep"})
}

// TestAgentWorkflow_PersistsTurnIncrementally checks that a turn is written as
// it goes, in slices that are each replayable on their own: the user message,
// then every assistant message carrying tool calls together with their results,
// then the final answer.
func TestAgentWorkflow_PersistsTurnIncrementally(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	registerAgentStubs(env)

	var persists []persistCall
	recordPersists(env, &persists)

	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.ExecuteToolInput) (activity.ExecuteToolOutput, error) {
		return activity.ExecuteToolOutput{Content: "page content"}, nil
	}, sdkactivity.RegisterOptions{Name: "ExecuteTool"})

	calls := 0
	env.RegisterActivityWithOptions(func(ctx context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
		calls++
		if calls == 1 {
			return provider.ChatResponse{ToolCalls: []provider.ToolCallInfo{
				{ID: "1", Name: "web_fetch", Input: json.RawMessage(`{}`)},
			}}, nil
		}
		return provider.ChatResponse{Content: "done", StopReason: "end_turn"}, nil
	}, sdkactivity.RegisterOptions{Name: "CallLLM"})

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{
		SessionID:   "s1",
		AgentID:     "reviewer",
		UserMessage: "hello",
		TurnKey:     "run-abc-3",
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}

	want := []persistCall{
		{turnKey: "run-abc-3", startIndex: 0, roles: []string{"user"}},
		{turnKey: "run-abc-3", startIndex: 1, roles: []string{"assistant", "tool"}},
		{turnKey: "run-abc-3", startIndex: 3, roles: []string{"assistant"}},
	}
	if fmt.Sprint(persists) != fmt.Sprint(want) {
		t.Errorf("persisted %v, want %v", persists, want)
	}

	var out AgentWorkflowOutput
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.NewMessages) != 4 {
		t.Errorf("NewMessages = %d messages, want 4 (the turn only, not the history)", len(out.NewMessages))
	}
}

// TestAgentWorkflow_NoPersistWithoutTurn checks that a sub-agent or a scheduled
// run writes nothing: they own no session history.
func TestAgentWorkflow_NoPersistWithoutTurn(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	registerAgentStubs(env)

	var persists []persistCall
	recordPersists(env, &persists)

	env.RegisterActivityWithOptions(func(ctx context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
		return provider.ChatResponse{Content: "done", StopReason: "end_turn"}, nil
	}, sdkactivity.RegisterOptions{Name: "CallLLM"})

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{
		SessionID: "s1", AgentID: "reviewer", UserMessage: "hello",
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if len(persists) != 0 {
		t.Errorf("persisted %v, want nothing without a turn key", persists)
	}
}

// TestAgentWorkflow_LLMFailureKeepsTranscript checks that an LLM giving up ends
// the turn without failing the workflow: a failed workflow returns no result,
// which would throw away everything the turn produced.
func TestAgentWorkflow_LLMFailureKeepsTranscript(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	registerAgentStubs(env)

	var persists []persistCall
	recordPersists(env, &persists)

	env.RegisterActivityWithOptions(func(ctx context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
		return provider.ChatResponse{}, temporal.NewNonRetryableApplicationError(
			"overloaded", "PermanentAPIError", nil)
	}, sdkactivity.RegisterOptions{Name: "CallLLM"})

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{
		SessionID:   "s1",
		AgentID:     "reviewer",
		UserMessage: "hello",
		TurnKey:     "run-abc-7",
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v, want a completed workflow reporting the failure in its output", err)
	}

	var out AgentWorkflowOutput
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Error, "call LLM") {
		t.Errorf("Error = %q, want it to report the LLM failure", out.Error)
	}
	if len(out.NewMessages) != 1 || out.NewMessages[0].Role != "user" {
		t.Errorf("NewMessages = %+v, want the user message to survive", out.NewMessages)
	}
	want := []persistCall{{turnKey: "run-abc-7", startIndex: 0, roles: []string{"user"}}}
	if fmt.Sprint(persists) != fmt.Sprint(want) {
		t.Errorf("persisted %v, want %v", persists, want)
	}
}

// TestAgentWorkflow_SubAgentLoadsNoHistory checks that a run without a turn key
// never reads the session transcript: a sub-agent's context is isolated.
func TestAgentWorkflow_SubAgentLoadsNoHistory(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	registerAgentStubs(env)

	loads := 0
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.LoadContextInput) (activity.LoadContextOutput, error) {
		loads++
		return activity.LoadContextOutput{}, nil
	}, sdkactivity.RegisterOptions{Name: "LoadContext"})

	env.RegisterActivityWithOptions(func(ctx context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
		if len(req.Messages) != 1 {
			t.Errorf("sub-agent saw %d messages, want only its own task", len(req.Messages))
		}
		return provider.ChatResponse{Content: "done", StopReason: "end_turn"}, nil
	}, sdkactivity.RegisterOptions{Name: "CallLLM"})

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{
		SessionID: "child-1", AgentID: "reviewer", UserMessage: "sub task",
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if loads != 0 {
		t.Errorf("LoadContext called %d times, want 0 for a sub-agent", loads)
	}
}

// TestAgentWorkflow_LoadsItsOwnHistory checks that a session turn reads the
// transcript itself rather than receiving it in its input, and that what it
// loaded reaches the model.
func TestAgentWorkflow_LoadsItsOwnHistory(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	registerAgentStubs(env)
	recordPersists(env, new([]persistCall))

	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.LoadContextInput) (activity.LoadContextOutput, error) {
		if in.SessionID != "s1" || in.UserID != "victor" {
			t.Errorf("LoadContext(%+v), want session s1 for victor", in)
		}
		return activity.LoadContextOutput{
			Messages: []store.Message{
				{Role: store.RoleUser, Content: `"earlier question"`},
				{Role: store.RoleAssistant, Content: `"earlier answer"`},
			},
			UserMemory: "likes concise answers",
		}, nil
	}, sdkactivity.RegisterOptions{Name: "LoadContext"})

	var seen provider.ChatRequest
	env.RegisterActivityWithOptions(func(ctx context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
		seen = req
		return provider.ChatResponse{Content: "done", StopReason: "end_turn"}, nil
	}, sdkactivity.RegisterOptions{Name: "CallLLM"})

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{
		SessionID: "s1", UserID: "victor", AgentID: "reviewer",
		UserMessage: "new question", TurnKey: "run-abc-4",
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if len(seen.Messages) != 3 {
		t.Fatalf("model saw %d messages, want the 2 loaded plus the new one", len(seen.Messages))
	}
	if !strings.Contains(seen.System, "likes concise answers") {
		t.Errorf("system prompt lost the user memory: %q", seen.System)
	}

	var out AgentWorkflowOutput
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.NewMessages) != 2 {
		t.Errorf("NewMessages = %d, want only the turn's 2 messages, not the history", len(out.NewMessages))
	}
}

func TestTruncateToolResult(t *testing.T) {
	small := strings.Repeat("a", maxToolResultBytes)
	if got := truncateToolResult(small); got != small {
		t.Error("a result at the limit must pass through untouched")
	}

	big := strings.Repeat("H", maxToolResultBytes) + "MIDDLE" + strings.Repeat("T", maxToolResultBytes)
	got := truncateToolResult(big)
	switch {
	case len(got) > maxToolResultBytes+200:
		t.Errorf("truncated to %d bytes, want about %d", len(got), maxToolResultBytes)
	case !strings.HasPrefix(got, "HHH"):
		t.Error("head of the output was dropped")
	case !strings.HasSuffix(got, "TTT"):
		t.Error("tail of the output was dropped, where errors usually are")
	case strings.Contains(got, "MIDDLE"):
		t.Error("the middle should have been the part omitted")
	case !strings.Contains(got, "bytes omitted"):
		t.Error("truncation must be visible to the model")
	}

	// A cut landing inside a multi-byte rune must not produce invalid UTF-8.
	accents := strings.Repeat("é", maxToolResultBytes)
	if !utf8.ValidString(truncateToolResult(accents)) {
		t.Error("truncation broke a rune")
	}
}
