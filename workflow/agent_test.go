package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	enumspb "go.temporal.io/api/enums/v1"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/provider"
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
