package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/mock"
	enumspb "go.temporal.io/api/enums/v1"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/tool"
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

// Each tool call is bounded by its tool's timeout, from the catalog: exec
// waits for its longest command, a tool that declares none gets the default.
func TestAgentWorkflow_ToolTimeoutFromTheCatalog(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	schema := json.RawMessage(`{"type":"object"}`)
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.ListToolsInput) (activity.ListToolsOutput, error) {
		return activity.ListToolsOutput{
			Tools: []provider.ToolDefinition{{Name: "exec", InputSchema: schema}, {Name: "web_search", InputSchema: schema}},
			Resolutions: map[string]activity.ToolResolution{
				"exec":       {Kind: "activity", TaskQueue: "tools", Timeout: 330 * time.Second},
				"web_search": {Kind: "activity", TaskQueue: "tools"},
			},
		}, nil
	}, sdkactivity.RegisterOptions{Name: "ListTools"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.LoadSkillsForAgentInput) (activity.LoadSkillsForAgentOutput, error) {
		return activity.LoadSkillsForAgentOutput{SystemPrompt: "prompt"}, nil
	}, sdkactivity.RegisterOptions{Name: "LoadSkillsForAgent"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.NotifyInput) error {
		return nil
	}, sdkactivity.RegisterOptions{Name: "NotifyStep"})

	var mu sync.Mutex // the calls run in parallel
	timeouts := map[string]time.Duration{}
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.ExecuteToolInput) (activity.ExecuteToolOutput, error) {
		info := sdkactivity.GetInfo(ctx)
		mu.Lock()
		defer mu.Unlock()
		timeouts[in.Name] = info.Deadline.Sub(info.StartedTime)
		return activity.ExecuteToolOutput{Content: "ok"}, nil
	}, sdkactivity.RegisterOptions{Name: "ExecuteTool"})

	calls := 0
	env.RegisterActivityWithOptions(func(ctx context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
		calls++
		if calls == 1 {
			return provider.ChatResponse{ToolCalls: []provider.ToolCallInfo{
				{ID: "1", Name: "exec", Input: json.RawMessage(`{"command":"make test","timeout_seconds":300}`)},
				{ID: "2", Name: "web_search", Input: json.RawMessage(`{}`)},
			}}, nil
		}
		return provider.ChatResponse{Content: "done", StopReason: "end_turn"}, nil
	}, sdkactivity.RegisterOptions{Name: "CallLLM"})

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{SessionID: "s1", AgentID: "dev", UserMessage: "test it"})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if timeouts["exec"] != 330*time.Second || timeouts["web_search"] != tool.DefaultTimeout {
		t.Errorf("timeouts = %v, want exec 5m30s and web_search the default %s", timeouts, tool.DefaultTimeout)
	}
}

// A run stopped before its loop returns what it produced, the message it was
// given: ended by an error, it would complete as cancelled, with no result
// for the session to persist.
func TestAgentWorkflow_CancelWhileLoadingReturnsTheTranscript(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.LoadContextInput) (activity.LoadContextOutput, error) {
		return activity.LoadContextOutput{}, nil
	}, sdkactivity.RegisterOptions{Name: "LoadContext"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.PersistContextInput) error {
		return nil
	}, sdkactivity.RegisterOptions{Name: "PersistContext"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.LoadSkillsForAgentInput) (activity.LoadSkillsForAgentOutput, error) {
		<-ctx.Done()
		return activity.LoadSkillsForAgentOutput{}, ctx.Err()
	}, sdkactivity.RegisterOptions{Name: "LoadSkillsForAgent"})
	env.RegisterDelayedCallback(env.CancelWorkflow, time.Second)

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{SessionID: "s1", AgentID: "dev", UserMessage: "hello", TurnKey: "run-1"})

	var out AgentWorkflowOutput
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatalf("cancelled run: %v, want its output", err)
	}
	if len(out.NewMessages) != 1 || out.NewMessages[0].Role != store.RoleUser {
		t.Errorf("new messages %+v, want the user's message", out.NewMessages)
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

func TestBuildChildInput_AgentTool(t *testing.T) {
	parent := AgentWorkflowInput{Model: "m"}
	analyst := func() activity.ToolResolution {
		return activity.ToolResolution{Kind: "workflow", AgentID: "market-analyst"}
	}

	t.Run("the target comes from the resolution and runs on the current queue", func(t *testing.T) {
		res := analyst()
		_, in, err := buildChildInput(json.RawMessage(`{"task":"t"}`),
			parent, "child", &res, []string{"default"}, "default", "agent", "")
		if err != nil {
			t.Fatal(err)
		}
		child := in.(AgentWorkflowInput)
		if child.AgentID != "market-analyst" || child.UserMessage != "t" || child.Model != "m" || child.SessionID != "child" || res.TaskQueue != "agent" {
			t.Errorf("child=%+v queue=%q", child, res.TaskQueue)
		}
	})

	t.Run("a sub-agent reaches the user on the parent's channel", func(t *testing.T) {
		res := analyst()
		tg := AgentWorkflowInput{Model: "m", UserID: "u-alice", Channel: "telegram", ChannelID: "42", SignReply: true}
		_, in, _ := buildChildInput(json.RawMessage(`{"task":"t"}`),
			tg, "child", &res, nil, "default", "agent", "")
		if child := in.(AgentWorkflowInput); child.Channel != "telegram" || child.ChannelID != "42" || child.UserID != "u-alice" || !child.SignReply {
			t.Errorf("child = %+v", child)
		}
	})

	t.Run("the call may pick its own model", func(t *testing.T) {
		res := analyst()
		_, in, _ := buildChildInput(json.RawMessage(`{"task":"t","model":"other"}`),
			parent, "child", &res, nil, "default", "agent", "")
		if child := in.(AgentWorkflowInput); child.Model != "other" {
			t.Errorf("model = %q, want other", child.Model)
		}
	})

	t.Run("an agent_id in the input is ignored", func(t *testing.T) {
		res := analyst()
		_, in, _ := buildChildInput(json.RawMessage(`{"task":"t","agent_id":"root"}`),
			parent, "child", &res, nil, "default", "agent", "")
		if child := in.(AgentWorkflowInput); child.AgentID != "market-analyst" {
			t.Errorf("agent = %q, want market-analyst", child.AgentID)
		}
	})

	t.Run("an empty task is refused", func(t *testing.T) {
		res := analyst()
		_, _, err := buildChildInput(json.RawMessage(`{"task":"  "}`),
			parent, "child", &res, nil, "default", "agent", "")
		if err == nil || !strings.Contains(err.Error(), "task is required") {
			t.Errorf("got error %v", err)
		}
	})

	t.Run("delegating to itself is refused", func(t *testing.T) {
		res := analyst()
		_, _, err := buildChildInput(json.RawMessage(`{"task":"t"}`),
			parent, "child", &res, nil, "market-analyst", "agent", "")
		if err == nil || !strings.Contains(err.Error(), "cannot delegate to itself") {
			t.Errorf("got error %v", err)
		}
	})
}

// TestAgentWorkflow_DelegatesThroughAgentTool runs a parent that calls an
// agent_<id> tool and checks the child it starts.
func TestAgentWorkflow_DelegatesThroughAgentTool(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.ListToolsInput) (activity.ListToolsOutput, error) {
		return activity.ListToolsOutput{
			Tools: []provider.ToolDefinition{{Name: "agent_analyst", InputSchema: json.RawMessage(activity.AgentToolSchema)}},
			Resolutions: map[string]activity.ToolResolution{
				"agent_analyst": {Kind: "workflow", AgentID: "analyst"},
			},
		}, nil
	}, sdkactivity.RegisterOptions{Name: "ListTools"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.LoadSkillsForAgentInput) (activity.LoadSkillsForAgentOutput, error) {
		return activity.LoadSkillsForAgentOutput{SystemPrompt: "prompt"}, nil
	}, sdkactivity.RegisterOptions{Name: "LoadSkillsForAgent"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.NotifyInput) error {
		return nil
	}, sdkactivity.RegisterOptions{Name: "NotifyStep"})

	var secondRequest provider.ChatRequest
	calls := 0
	env.RegisterActivityWithOptions(func(ctx context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
		calls++
		if calls == 1 {
			return provider.ChatResponse{ToolCalls: []provider.ToolCallInfo{
				{ID: "1", Name: "agent_analyst", Input: json.RawMessage(`{"task":"summarize the CAC 40"}`)},
			}}, nil
		}
		secondRequest = req
		return provider.ChatResponse{Content: "done", StopReason: "end_turn"}, nil
	}, sdkactivity.RegisterOptions{Name: "CallLLM"})

	// Parent and child are both AgentWorkflow, and the mock catches both: the
	// parent runs the real thing, the child is replaced.
	var child AgentWorkflowInput
	env.OnWorkflow(AgentWorkflow, mock.Anything, mock.Anything).Return(
		func(ctx workflow.Context, in AgentWorkflowInput) (AgentWorkflowOutput, error) {
			if in.AgentID == "default" {
				return AgentWorkflow(ctx, in)
			}
			child = in
			return AgentWorkflowOutput{Response: "CAC 40 summary"}, nil
		})

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{SessionID: "s1", AgentID: "default", UserMessage: "hello", Model: "m"})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if child.AgentID != "analyst" || child.UserMessage != "summarize the CAC 40" || child.Model != "m" ||
		fmt.Sprint(child.AgentChain) != "[default]" {
		t.Errorf("child input = %+v", child)
	}
	found := false
	for _, m := range secondRequest.Messages {
		if m.ToolResult != nil {
			found = true
			if m.ToolResult.IsError || m.ToolResult.Content != "CAC 40 summary" {
				t.Errorf("tool result = %+v", m.ToolResult)
			}
		}
	}
	if !found {
		t.Error("the sub-agent's answer never reached the parent")
	}
}

func TestSubAgentContent(t *testing.T) {
	cases := map[string]string{
		`{"response":"CAC 40 summary","messages":[{"role":"user"}],"goal_achieved":true}`: "CAC 40 summary",
		`{"other":1}`: `{"other":1}`,
	}
	for raw, want := range cases {
		if got := subAgentContent(json.RawMessage(raw)); got != want {
			t.Errorf("subAgentContent(%s) = %q, want %q", raw, got, want)
		}
	}
}

// A workflow tool not flagged as needing the call context gets the model's
// input untouched; a flagged one with an input that is no object is refused.
func TestBuildChildInput_CallContextOnlyWhenPublished(t *testing.T) {
	parent := AgentWorkflowInput{Channel: "telegram", ChannelID: "42"}
	plain := activity.ToolResolution{Kind: "workflow", WorkflowName: "AnalyzeRepoWorkflow"}
	_, in, err := buildChildInput(json.RawMessage(`{"repo":"r"}`), parent, "child", &plain, []string{"default"}, "default", "agent", "")
	if err != nil || string(in.(json.RawMessage)) != `{"repo":"r"}` {
		t.Errorf("plain tool input %s, %v", in, err)
	}
	flagged := activity.ToolResolution{Kind: "workflow", WorkflowName: "AskUserWorkflow", NeedsCallContext: true}
	if _, _, err := buildChildInput(json.RawMessage(`"just text"`), parent, "child", &flagged, nil, "default", "agent", ""); err == nil {
		t.Error("a non-object input was enriched")
	}
}

func TestChildWorkflowID(t *testing.T) {
	if got := childWorkflowID("s1", "agent_analyst", "toolu_01A", 3, 1); got != "s1-tool-agent_analyst-toolu_01A" {
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
	// The members read it: the API's message, not Temporal's envelope.
	if out.Error != "call LLM: overloaded" {
		t.Errorf("Error = %q, want the LLM failure's own message", out.Error)
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

// TestAgentWorkflow_ScheduledRunLoadsOnlyTheUserMemory checks that a run with
// no turn key that asks for its user's memory gets it, and nothing of a
// session history.
func TestAgentWorkflow_ScheduledRunLoadsOnlyTheUserMemory(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	registerAgentStubs(env)

	loads := 0
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.LoadContextInput) (activity.LoadContextOutput, error) {
		loads++
		return activity.LoadContextOutput{}, nil
	}, sdkactivity.RegisterOptions{Name: "LoadContext"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.LoadMemoryInput) (string, error) {
		if in.Scope != store.MemoryScopeUser || in.ScopeID != "victor" {
			t.Errorf("LoadMemory(%+v), want victor's user memory", in)
		}
		return "likes concise answers", nil
	}, sdkactivity.RegisterOptions{Name: "LoadMemory"})

	var seen provider.ChatRequest
	env.RegisterActivityWithOptions(func(ctx context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
		seen = req
		return provider.ChatResponse{Content: "done", StopReason: "end_turn"}, nil
	}, sdkactivity.RegisterOptions{Name: "CallLLM"})

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{
		SessionID: "schedule-1", UserID: "victor", AgentID: "default",
		UserMessage: "check my reminders", LoadUserMemory: true,
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if loads != 0 {
		t.Errorf("LoadContext called %d times, want 0 without a turn key", loads)
	}
	if !strings.Contains(seen.System, "likes concise answers") {
		t.Errorf("system prompt lacks the user memory: %q", seen.System)
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

// In a shared session the model must know who speaks: each user message
// reaches it prefixed with its author, while the stored message keeps the text
// and the author apart.
func TestConvertMessages_NamesTheAuthor(t *testing.T) {
	msgs := convertMessages([]store.Message{
		{Role: store.RoleUser, Content: `"hello"`, UserID: "u-alice", Author: "Alice"},
		{Role: store.RoleAssistant, Content: `"hi Alice"`},
		{Role: store.RoleUser, Content: `"scheduled prompt"`}, // no author: a scheduled run
	}, historyView{self: "default"})
	for i, want := range []string{`"[Alice] hello"`, `"hi Alice"`, `"scheduled prompt"`} {
		if got := string(msgs[i].Content); got != want {
			t.Errorf("message %d = %s, want %s", i, got, want)
		}
	}
}

// Several agents answer in a session: the model reads another agent's turn
// as text under its name and mention, in a user message, and its own as they
// are. An agent gone from the catalog keeps the name it signed with, or its
// ID; a message signed by no agent is read as the reader's own.
func TestConvertMessages_NamesTheOtherAgents(t *testing.T) {
	view := historyView{self: "smith", agents: map[string]activity.AgentLabel{
		"jarvis": {Name: "Jarvis", Mention: "jarvis"},
		"smith":  {Name: "Agent Smith", Mention: "smith"},
	}}
	msgs := convertMessages([]store.Message{
		{Role: store.RoleUser, Content: `"@jarvis résume, @smith juge"`, Author: "Alice"},
		{Role: store.RoleAssistant, Content: `"voici le résumé"`, AgentID: "jarvis", Author: "Jarvis"},
		{Role: store.RoleAssistant, Content: `"gone"`, AgentID: "old", Author: "Old One"},
		{Role: store.RoleAssistant, Content: `"nameless"`, AgentID: "older"},
		{Role: store.RoleAssistant, Content: `"mine"`, AgentID: "smith", Author: "Agent Smith"},
		{Role: store.RoleAssistant, Content: `"unsigned"`},
	}, view)

	want := []struct{ role, content string }{
		{"user", "[Alice] @jarvis résume, @smith juge\n\n[agent Jarvis (@jarvis)] voici le résumé\n\n[agent Old One] gone\n\n[agent older] nameless"},
		{"assistant", "mine"},
		{"assistant", "unsigned"},
	}
	if len(msgs) != len(want) {
		t.Fatalf("%d messages, want %d: %+v", len(msgs), len(want), msgs)
	}
	for i, w := range want {
		if msgs[i].Role != w.role || textOf(msgs[i]) != w.content {
			t.Errorf("message %d = %s %q, want %s %q", i, msgs[i].Role, textOf(msgs[i]), w.role, w.content)
		}
	}
}

// An agent without tools answers after one that used some: its request holds
// no tool block, or the API would reject it, and ends on a user message, or
// the model would continue the other agent's answer. A member's message
// written between the other agent's call and its result comes after the
// result; a private input stays hidden.
func TestConvertMessages_OtherAgentsToolsAsText(t *testing.T) {
	view := historyView{
		self:    "smith",
		agents:  map[string]activity.AgentLabel{"jarvis": {Name: "Jarvis", Mention: "jarvis"}},
		private: tool.PrivateSet{"save_user_memory": true},
	}
	msgs := convertMessages([]store.Message{
		{Role: store.RoleUser, Content: `"@jarvis cherche, @smith juge"`, Author: "Alice"},
		{Role: store.RoleAssistant, Content: `"je cherche"`, AgentID: "jarvis", Author: "Jarvis", ToolCalls: []store.ToolCall{
			{ID: "t1", Name: "web_search", Input: json.RawMessage(`{"q":"temporal"}`)},
			{ID: "t2", Name: "save_user_memory", Input: json.RawMessage(`{"content":"Alice's secret"}`)},
		}},
		{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t1", Content: "found " + strings.Repeat("x", 3000)}},
		{Role: store.RoleUser, Content: `"meanwhile"`, Author: "Bob"},
		{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t2", Content: "denied", IsError: true}},
		{Role: store.RoleAssistant, Content: `"voici"`, AgentID: "jarvis", Author: "Jarvis"},
	}, view)

	if len(msgs) != 1 || msgs[0].Role != "user" {
		t.Fatalf("messages %+v, want one user message", msgs)
	}
	for _, m := range msgs {
		if len(m.ToolCalls) > 0 || m.ToolResult != nil {
			t.Errorf("a tool block reached an agent that did not make it: %+v", m)
		}
	}
	text := textOf(msgs[0])
	for _, want := range []string{
		"[Alice] @jarvis cherche, @smith juge\n\n[agent Jarvis (@jarvis)] je cherche\n",
		`[agent Jarvis (@jarvis) called web_search {"q":"temporal"}]`,
		`[agent Jarvis (@jarvis) called save_user_memory {"content":"(private)"}]`,
		"[result of web_search, called by agent Jarvis (@jarvis)] found xxx",
		"[error from save_user_memory, called by agent Jarvis (@jarvis)] denied\n\n[Bob] meanwhile\n\n[agent Jarvis (@jarvis)] voici",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("model reads %q\nwant %q in it", text, want)
		}
	}
	if strings.Contains(text, "secret") {
		t.Error("a private tool input reached another agent")
	}
	if len(text) > 2500 {
		t.Errorf("another agent's tool result was not clipped: %d bytes", len(text))
	}
}

// The reader's own tool calls keep their blocks, each followed by its result,
// with a member's message written in between after them; another agent's
// turn later on is text.
func TestConvertMessages_KeepsItsOwnToolPairing(t *testing.T) {
	view := historyView{self: "smith", agents: map[string]activity.AgentLabel{"jarvis": {Name: "Jarvis", Mention: "jarvis"}}}
	msgs := convertMessages([]store.Message{
		{Role: store.RoleUser, Content: `"@smith lis le dépôt"`, Author: "Alice"},
		{Role: store.RoleAssistant, AgentID: "smith", ToolCalls: []store.ToolCall{{ID: "s1", Name: "read_file"}}},
		{Role: store.RoleUser, Content: `"meanwhile"`, Author: "Bob"},
		{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "s1", Content: "main.go"}},
		{Role: store.RoleAssistant, Content: `"lu"`, AgentID: "smith"},
		{Role: store.RoleUser, Content: `"@jarvis cherche, @smith juge"`, Author: "Alice"},
		{Role: store.RoleAssistant, AgentID: "jarvis", ToolCalls: []store.ToolCall{{ID: "j1", Name: "web_search"}}},
		{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "j1", Content: "found"}},
		{Role: store.RoleAssistant, Content: `"voici"`, AgentID: "jarvis"},
	}, view)

	want := []struct{ role, content, call, result string }{
		{role: "user", content: "[Alice] @smith lis le dépôt"},
		{role: "assistant", call: "s1"},
		{role: "tool", result: "s1"},
		{role: "user", content: "[Bob] meanwhile"},
		{role: "assistant", content: "lu"},
		{role: "user", content: "[Alice] @jarvis cherche, @smith juge\n\n[agent Jarvis (@jarvis) called web_search {}]\n\n[result of web_search, called by agent Jarvis (@jarvis)] found\n\n[agent Jarvis (@jarvis)] voici"},
	}
	if len(msgs) != len(want) {
		t.Fatalf("%d messages, want %d: %+v", len(msgs), len(want), msgs)
	}
	for i, w := range want {
		m := msgs[i]
		if m.Role != w.role || (w.content != "" && textOf(m) != w.content) {
			t.Errorf("message %d = %s %q, want %s %q", i, m.Role, textOf(m), w.role, w.content)
		}
		if w.call != "" && (len(m.ToolCalls) != 1 || m.ToolCalls[0].ID != w.call) {
			t.Errorf("message %d: calls %+v, want %s", i, m.ToolCalls, w.call)
		}
		if w.result != "" && (m.ToolResult == nil || m.ToolResult.ToolCallID != w.result) {
			t.Errorf("message %d: result %+v, want the result of %s", i, m.ToolResult, w.result)
		}
	}
}

// textOf is a converted message's text.
func textOf(m provider.ChatMessage) string {
	var s string
	json.Unmarshal(m.Content, &s)
	return s
}

// Why a turn failed is for the members: the model never sees it, or it would
// answer the error instead of the user. The two user messages around it are
// read as one.
func TestConvertMessages_SkipsTurnErrors(t *testing.T) {
	msgs := convertMessages([]store.Message{
		{Role: store.RoleUser, Content: `"analyse the repo"`},
		{Role: store.RoleAssistant, Kind: store.KindTurnError, Content: `"call LLM: credit balance is too low"`},
		{Role: store.RoleUser, Content: `"try again"`},
	}, historyView{self: "default"})
	if len(msgs) != 1 || textOf(msgs[0]) != "analyse the repo\n\ntry again" {
		t.Errorf("messages = %+v, want the two user messages alone", msgs)
	}
}

// A member wrote while the agent was between a tool call and its result: the
// model must still see the result right after the call, or the API rejects
// the conversation. The message comes after the results, and is kept.
func TestDeferInterleaved(t *testing.T) {
	call := store.Message{Role: store.RoleAssistant, ToolCalls: []store.ToolCall{{ID: "t1"}, {ID: "t2"}}}
	r1 := store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t1"}}
	r2 := store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t2"}}
	human := store.Message{Role: store.RoleUser, Content: `"meanwhile"`, Author: "Bob"}
	start := store.Message{Role: store.RoleUser, Content: `"go"`}
	done := store.Message{Role: store.RoleAssistant, Content: `"done"`}

	got := deferInterleaved([]store.Message{start, call, r1, human, r2, done})
	want := []store.Message{start, call, r1, r2, human, done}
	if len(got) != len(want) {
		t.Fatalf("%d messages, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Content != want[i].Content || (got[i].ToolResult == nil) != (want[i].ToolResult == nil) ||
			(got[i].ToolResult != nil && got[i].ToolResult.ToolCallID != want[i].ToolResult.ToolCallID) {
			t.Errorf("message %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// A message the server stored already is loaded with the history, not added
// by the turn a second time.
func TestAgentWorkflow_StoredMessageIsNotAddedAgain(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	registerAgentStubs(env)
	var persisted []persistCall
	recordPersists(env, &persisted)

	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.LoadContextInput) (activity.LoadContextOutput, error) {
		return activity.LoadContextOutput{Messages: []store.Message{
			{Role: store.RoleUser, Content: `"we talked"`, Author: "Alice"},
			{Role: store.RoleUser, Content: `"@agent sum it up"`, Author: "Bob"},
		}}, nil
	}, sdkactivity.RegisterOptions{Name: "LoadContext"})
	var seen provider.ChatRequest
	env.RegisterActivityWithOptions(func(ctx context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
		seen = req
		return provider.ChatResponse{Content: "summary", StopReason: "end_turn"}, nil
	}, sdkactivity.RegisterOptions{Name: "CallLLM"})

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{
		SessionID: "s1", UserID: "u-bob", UserName: "Bob", AgentID: "reviewer",
		UserMessage: "@agent sum it up", UserMessageStored: true, TurnKey: "run-1",
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(seen.Messages) != 1 || textOf(seen.Messages[0]) != "[Alice] we talked\n\n[Bob] @agent sum it up" {
		t.Errorf("model saw %+v, want the 2 stored messages, in one", seen.Messages)
	}
	for _, p := range persisted {
		for _, role := range p.roles {
			if role == string(store.RoleUser) {
				t.Errorf("the turn stored a user message again: %+v", p)
			}
		}
	}
}

// A sub-agent of a Telegram session asks its questions on Telegram, but its
// answer goes to its parent only: on Telegram it would read as the reply.
func TestAgentWorkflow_SubAgentRepliesToItsParentOnly(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.ListToolsInput) (activity.ListToolsOutput, error) {
		return activity.ListToolsOutput{}, nil
	}, sdkactivity.RegisterOptions{Name: "ListTools"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.LoadSkillsForAgentInput) (activity.LoadSkillsForAgentOutput, error) {
		return activity.LoadSkillsForAgentOutput{}, nil
	}, sdkactivity.RegisterOptions{Name: "LoadSkillsForAgent"})
	env.RegisterActivityWithOptions(func(ctx context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
		return provider.ChatResponse{Content: "the analysis", StopReason: "end_turn"}, nil
	}, sdkactivity.RegisterOptions{Name: "CallLLM"})
	var channels []string
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.NotifyInput) error {
		channels = append(channels, in.Channel)
		return nil
	}, sdkactivity.RegisterOptions{Name: "NotifyStep"})

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{
		SessionID: "s1-tool-agent_analyst-1", AgentID: "analyst", UserMessage: "go",
		Channel: "telegram", ChannelID: "42", // inherited, no TurnKey: a sub-agent
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	for _, c := range channels {
		if c == "telegram" {
			t.Errorf("the sub-agent's answer went to Telegram: %v", channels)
		}
	}
}

// ask_user gets the channel of the agent that asks, a sub-agent's included.
func TestBuildChildInput_AskUserGetsTheChannel(t *testing.T) {
	sub := AgentWorkflowInput{SessionID: "s1-tool-agent_analyst-1", Channel: "telegram", ChannelID: "42"}
	res := activity.ToolResolution{Kind: "workflow", WorkflowName: "AskUserWorkflow", NeedsCallContext: true}
	_, in, err := buildChildInput(json.RawMessage(`{"question":"ok?","agent":"forged"}`), sub, "child", &res, []string{"default", "analyst"}, "analyst", "agent", "Analyst")
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Channel    string   `json:"channel"`
		ChannelID  string   `json:"channel_id"`
		AgentChain []string `json:"agent_chain"`
		Agent      string   `json:"agent"`
	}
	json.Unmarshal(in.(json.RawMessage), &got)
	if got.Channel != "telegram" || got.ChannelID != "42" || len(got.AgentChain) != 2 {
		t.Errorf("ask_user input %s", in)
	}
	// The question is signed as the answer would be, never as the model says.
	if got.Agent != "Analyst" {
		t.Errorf("ask_user signed %q, want Analyst", got.Agent)
	}
}

// A channel refusing the answer for good must not hold the turn: the answer
// is in the transcript, and the turn ends.
func TestAgentWorkflow_UndeliveredAnswerEndsTheTurn(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.ListToolsInput) (activity.ListToolsOutput, error) {
		return activity.ListToolsOutput{}, nil
	}, sdkactivity.RegisterOptions{Name: "ListTools"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.LoadSkillsForAgentInput) (activity.LoadSkillsForAgentOutput, error) {
		return activity.LoadSkillsForAgentOutput{}, nil
	}, sdkactivity.RegisterOptions{Name: "LoadSkillsForAgent"})
	env.RegisterActivityWithOptions(func(ctx context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
		return provider.ChatResponse{Content: "the answer", StopReason: "end_turn"}, nil
	}, sdkactivity.RegisterOptions{Name: "CallLLM"})
	attempts := 0
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.NotifyInput) error {
		attempts++
		return errors.New("telegram send: status 400")
	}, sdkactivity.RegisterOptions{Name: "NotifyStep"})

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{SessionID: "s1", AgentID: "default", UserMessage: "go", Channel: "telegram", ChannelID: "42", TurnKey: ""})
	if !env.IsWorkflowCompleted() {
		t.Fatal("the turn did not end")
	}
	var out AgentWorkflowOutput
	if err := env.GetWorkflowResult(&out); err != nil || out.Response != "the answer" {
		t.Errorf("result %+v, %v", out, err)
	}
	if attempts != 3 {
		t.Errorf("%d attempts, want 3", attempts)
	}
}

func TestDelegationRefusal(t *testing.T) {
	for _, c := range []struct {
		chain   []string
		agentID string
		refused string // part of the refusal; "" = allowed
	}{
		{[]string{"root"}, "analyst", ""},
		{[]string{"root", "analyst"}, "root", "already in the call chain (root → analyst)"},
		{[]string{"root", "a", "b"}, "a", "already in the call chain"},
		{[]string{"root", "a", "b"}, "c", ""}, // the third level
		{[]string{"root", "a", "b", "c"}, "d", "limited to 3 levels"},
	} {
		err := delegationRefusal(c.chain, c.agentID)
		switch {
		case c.refused == "" && err != nil:
			t.Errorf("%v → %s refused: %v", c.chain, c.agentID, err)
		case c.refused != "" && (err == nil || !strings.Contains(err.Error(), c.refused)):
			t.Errorf("%v → %s = %v, want a refusal saying %q", c.chain, c.agentID, err, c.refused)
		}
	}
}

// A sub-agent calling back an agent above it gets the refusal as the tool's
// error, and no child starts: it would only bounce the task back.
func TestAgentWorkflow_RefusesADelegationLoop(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var listed []string
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.ListToolsInput) (activity.ListToolsOutput, error) {
		listed = append(listed, in.AgentID)
		return activity.ListToolsOutput{
			Tools:       []provider.ToolDefinition{{Name: "agent_root", InputSchema: json.RawMessage(`{"type":"object"}`)}},
			Resolutions: map[string]activity.ToolResolution{"agent_root": {Kind: "workflow", WorkflowName: "AgentWorkflow", AgentID: "root"}},
		}, nil
	}, sdkactivity.RegisterOptions{Name: "ListTools"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.LoadSkillsForAgentInput) (activity.LoadSkillsForAgentOutput, error) {
		return activity.LoadSkillsForAgentOutput{}, nil
	}, sdkactivity.RegisterOptions{Name: "LoadSkillsForAgent"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.NotifyInput) error {
		return nil
	}, sdkactivity.RegisterOptions{Name: "NotifyStep"})

	var second provider.ChatRequest
	calls := 0
	env.RegisterActivityWithOptions(func(ctx context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
		calls++
		if calls == 1 {
			return provider.ChatResponse{ToolCalls: []provider.ToolCallInfo{
				{ID: "c1", Name: "agent_root", Input: json.RawMessage(`{"task":"ask root"}`)},
			}}, nil
		}
		second = req
		return provider.ChatResponse{Content: "done", StopReason: "end_turn"}, nil
	}, sdkactivity.RegisterOptions{Name: "CallLLM"})

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{
		SessionID: "s1-tool-agent_analyst-c0", AgentID: "analyst", AgentChain: []string{"root"}, UserMessage: "go",
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if fmt.Sprint(listed) != "[analyst]" {
		t.Errorf("tools listed for %v: a child agent ran", listed)
	}
	var result *provider.ToolResultInfo
	for _, m := range second.Messages {
		if m.ToolResult != nil {
			result = m.ToolResult
		}
	}
	if result == nil || !result.IsError || !strings.Contains(result.Content, `agent "root" is already in the call chain`) {
		t.Errorf("tool result = %+v, want the refusal as an error", result)
	}
}

// A turn signs what it writes with its agent, tells its model the part the
// message gives it, and signs the answer sent to the channel when asked to:
// several agents answer in the session.
func TestAgentWorkflow_SignsItsMessages(t *testing.T) {
	for _, sign := range []bool{false, true} {
		t.Run(fmt.Sprint("sign=", sign), func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.RegisterActivityWithOptions(func(ctx context.Context, in activity.LoadContextInput) (activity.LoadContextOutput, error) {
				return activity.LoadContextOutput{}, nil
			}, sdkactivity.RegisterOptions{Name: "LoadContext"})
			env.RegisterActivityWithOptions(func(ctx context.Context, in activity.ListToolsInput) (activity.ListToolsOutput, error) {
				return activity.ListToolsOutput{}, nil
			}, sdkactivity.RegisterOptions{Name: "ListTools"})
			env.RegisterActivityWithOptions(func(ctx context.Context, in activity.LoadSkillsForAgentInput) (activity.LoadSkillsForAgentOutput, error) {
				return activity.LoadSkillsForAgentOutput{SystemPrompt: "prompt", Name: "Agent Smith"}, nil
			}, sdkactivity.RegisterOptions{Name: "LoadSkillsForAgent"})
			env.RegisterActivityWithOptions(func(ctx context.Context, in activity.PersistContextInput) error {
				return nil
			}, sdkactivity.RegisterOptions{Name: "PersistContext"})
			var system string
			env.RegisterActivityWithOptions(func(ctx context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
				system = req.System
				return provider.ChatResponse{Content: "it fits", StopReason: "end_turn"}, nil
			}, sdkactivity.RegisterOptions{Name: "CallLLM"})
			var answers []string
			env.RegisterActivityWithOptions(func(ctx context.Context, in activity.NotifyInput) error {
				answers = append(answers, string(in.Event.Data))
				return nil
			}, sdkactivity.RegisterOptions{Name: "NotifyStep"})

			env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{
				SessionID: "s1", AgentID: "smith", UserMessage: "go", UserMessageStored: true, TurnKey: "run-1",
				Channel: "telegram", ChannelID: "42", PartNote: "\n## PART\n", SignReply: sign,
			})
			if err := env.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}
			var out AgentWorkflowOutput
			if err := env.GetWorkflowResult(&out); err != nil {
				t.Fatal(err)
			}
			if len(out.NewMessages) != 1 || out.NewMessages[0].AgentID != "smith" || out.NewMessages[0].Author != "Agent Smith" {
				t.Errorf("wrote %+v, want the answer signed by smith (Agent Smith)", out.NewMessages)
			}
			if !strings.HasSuffix(system, "\n## PART\n") {
				t.Errorf("system prompt %q does not end with the part note", system)
			}
			want := `{"content":"it fits","type":"message"}`
			if sign {
				want = `{"agent":"Agent Smith","content":"it fits","type":"message"}`
			}
			if len(answers) != 1 || answers[0] != want {
				t.Errorf("answer sent %v, want %s", answers, want)
			}
		})
	}
}
