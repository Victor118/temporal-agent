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

	f := registerLLM(env, answers(provider.ChatResponse{ToolCalls: []provider.ToolCallInfo{
		{ID: "1", Name: "web_fetch", Input: json.RawMessage(`{}`)},
		{ID: "2", Name: "exec", Input: json.RawMessage(`{}`)},
	}}, done))

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
	for _, m := range f.model.sent()[1].Messages {
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
			Tools: []provider.ToolDefinition{{Name: "exec", InputSchema: schema}, {Name: "web_search", InputSchema: schema}, {Name: "web_fetch", InputSchema: schema}},
			Resolutions: map[string]activity.ToolResolution{
				"exec":       {Kind: "activity", TaskQueue: "tools", Timeout: 330 * time.Second},
				"web_search": {Kind: "activity", TaskQueue: "tools"},
				// A row edited by hand: not a duration, so the default.
				"web_fetch": {Kind: "activity", TaskQueue: "tools", Timeout: -time.Second},
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

	registerLLM(env, answers(provider.ChatResponse{ToolCalls: []provider.ToolCallInfo{
		{ID: "1", Name: "exec", Input: json.RawMessage(`{"command":"make test","timeout_seconds":300}`)},
		{ID: "2", Name: "web_search", Input: json.RawMessage(`{}`)},
		{ID: "3", Name: "web_fetch", Input: json.RawMessage(`{}`)},
	}}, done))

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{SessionID: "s1", AgentID: "dev", UserMessage: "test it"})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if timeouts["exec"] != 330*time.Second || timeouts["web_search"] != tool.DefaultTimeout || timeouts["web_fetch"] != tool.DefaultTimeout {
		t.Errorf("timeouts = %v, want exec 5m30s, web_search and web_fetch the default %s", timeouts, tool.DefaultTimeout)
	}
}

// A run stopped before its loop returns what it produced, the task it was
// given: ended by an error, it would complete as cancelled, with no result
// for its caller.
func TestAgentWorkflow_CancelWhileLoadingReturnsTheTranscript(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.PersistContextInput) error {
		return nil
	}, sdkactivity.RegisterOptions{Name: "PersistContext"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.LoadSkillsForAgentInput) (activity.LoadSkillsForAgentOutput, error) {
		<-ctx.Done()
		return activity.LoadSkillsForAgentOutput{}, ctx.Err()
	}, sdkactivity.RegisterOptions{Name: "LoadSkillsForAgent"})
	env.RegisterDelayedCallback(env.CancelWorkflow, time.Second)

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{SessionID: "s1:p:jarvis:m3:tool:agent_dev:1", AgentID: "dev", UserMessage: "hello"})

	var out AgentWorkflowOutput
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatalf("cancelled run: %v, want its output", err)
	}
	if len(out.NewMessages) != 1 || out.NewMessages[0].Role != store.RoleUser {
		t.Errorf("new messages %+v, want the task", out.NewMessages)
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
			parent, "child", &res, tool.CallContext{AgentChain: []string{"default"}}, "default", "agent")
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
			tg, "child", &res, tool.CallContext{}, "default", "agent")
		if child := in.(AgentWorkflowInput); child.Channel != "telegram" || child.ChannelID != "42" || child.UserID != "u-alice" || !child.SignReply {
			t.Errorf("child = %+v", child)
		}
	})

	t.Run("the call may pick its own model", func(t *testing.T) {
		res := analyst()
		_, in, _ := buildChildInput(json.RawMessage(`{"task":"t","model":"other"}`),
			parent, "child", &res, tool.CallContext{}, "default", "agent")
		if child := in.(AgentWorkflowInput); child.Model != "other" {
			t.Errorf("model = %q, want other", child.Model)
		}
	})

	t.Run("an agent_id in the input is ignored", func(t *testing.T) {
		res := analyst()
		_, in, _ := buildChildInput(json.RawMessage(`{"task":"t","agent_id":"root"}`),
			parent, "child", &res, tool.CallContext{}, "default", "agent")
		if child := in.(AgentWorkflowInput); child.AgentID != "market-analyst" {
			t.Errorf("agent = %q, want market-analyst", child.AgentID)
		}
	})

	t.Run("an empty task is refused", func(t *testing.T) {
		res := analyst()
		_, _, err := buildChildInput(json.RawMessage(`{"task":"  "}`),
			parent, "child", &res, tool.CallContext{}, "default", "agent")
		if err == nil || !strings.Contains(err.Error(), "task is required") {
			t.Errorf("got error %v", err)
		}
	})

	t.Run("delegating to itself is refused", func(t *testing.T) {
		res := analyst()
		_, _, err := buildChildInput(json.RawMessage(`{"task":"t"}`),
			parent, "child", &res, tool.CallContext{}, "market-analyst", "agent")
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

	f := registerLLM(env, answers(provider.ChatResponse{ToolCalls: []provider.ToolCallInfo{
		{ID: "1", Name: "agent_analyst", Input: json.RawMessage(`{"task":"summarize the CAC 40"}`)},
	}}, done))

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
	for _, m := range f.model.sent()[1].Messages {
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

// The parent reads a sub-agent's answer; with none, why it stopped, as an
// error, never its whole output, nor the fork advice meant for the members.
func TestSubAgentContent(t *testing.T) {
	tooLong, _ := json.Marshal(AgentWorkflowOutput{Error: activity.ContextTooLongMessage, ErrorType: activity.ErrContextTooLong, NewMessages: []store.Message{{Role: store.RoleUser}}})
	// The type decides, not the members' text, which may change.
	reworded, _ := json.Marshal(AgentWorkflowOutput{Error: "Trop long (1 234 567 octets)", ErrorType: activity.ErrContextTooLong})
	exhausted, _ := json.Marshal(exhaustedOutput([]store.Message{{Role: store.RoleUser}}))
	cases := []struct {
		raw     string
		want    string
		isError bool
	}{
		{`{"response":"CAC 40 summary","messages":[{"role":"user"}],"goal_achieved":true}`, "CAC 40 summary", false},
		{string(tooLong), subAgentTooLong, true},
		{string(reworded), subAgentTooLong, true},
		{string(exhausted), fmt.Sprintf("The agent stopped without an answer: stopped after %d iterations without a final answer", maxReActIterations), true},
		{`{"response":"","error":"call LLM: overloaded","new_messages":[{"role":"user"}]}`, "The agent stopped without an answer: call LLM: overloaded", true},
		{`{"other":1}`, `{"other":1}`, false},
	}
	for _, c := range cases {
		if got, isError := subAgentContent(json.RawMessage(c.raw)); got != c.want || isError != c.isError {
			t.Errorf("subAgentContent(%s) = %q, %v; want %q, %v", c.raw, got, isError, c.want, c.isError)
		}
	}
}

// A workflow tool not flagged as needing the call context gets the model's
// input untouched; a flagged one with an input that is no object is refused.
func TestBuildChildInput_CallContextOnlyWhenPublished(t *testing.T) {
	parent := AgentWorkflowInput{Channel: "telegram", ChannelID: "42"}
	plain := activity.ToolResolution{Kind: "workflow", WorkflowName: "AnalyzeRepoWorkflow"}
	_, in, err := buildChildInput(json.RawMessage(`{"repo":"r"}`), parent, "child", &plain, tool.CallContext{AgentChain: []string{"default"}}, "default", "agent")
	if err != nil || string(in.(json.RawMessage)) != `{"repo":"r"}` {
		t.Errorf("plain tool input %s, %v", in, err)
	}
	flagged := activity.ToolResolution{Kind: "workflow", WorkflowName: "AskUserWorkflow", NeedsCallContext: true}
	if _, _, err := buildChildInput(json.RawMessage(`"just text"`), parent, "child", &flagged, tool.CallContext{}, "default", "agent"); err == nil {
		t.Error("a non-object input was enriched")
	}
}

func TestChildWorkflowID(t *testing.T) {
	if got := childWorkflowID("s1:p:jarvis:m3", "agent_analyst", "toolu_01A", 3, 1); got != "s1:p:jarvis:m3:tool:agent_analyst:toolu_01A" {
		t.Errorf("got %q", got)
	}
	if got := childWorkflowID("s1:p:jarvis:m3", "ask_user", "", 3, 1); got != "s1:p:jarvis:m3:tool:ask_user:3-1" {
		t.Errorf("got %q", got)
	}
}

// registerAgentStubs wires the activities every AgentWorkflow run needs
// besides the LLM's (registerLLM).
func registerAgentStubs(env *testsuite.TestWorkflowEnvironment) {
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

// fetchPage is a model that fetches a page, then answers.
func fetchPage() func(int, provider.ChatRequest) (provider.ChatResponse, error) {
	return answers(provider.ChatResponse{ToolCalls: []provider.ToolCallInfo{{ID: "1", Name: "web_fetch", Input: json.RawMessage(`{}`)}}}, done)
}

// executePage stubs ExecuteTool with a page.
func executePage(env *testsuite.TestWorkflowEnvironment) {
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.ExecuteToolInput) (activity.ExecuteToolOutput, error) {
		return activity.ExecuteToolOutput{Content: "page content"}, nil
	}, sdkactivity.RegisterOptions{Name: "ExecuteTool"})
}

// TestAgentWorkflow_PersistsTurnIncrementally checks that a turn is written as
// it goes, in slices that are each replayable on their own: every assistant
// message carrying tool calls together with their results, then the final
// answer. The message it answers is the server's to store.
func TestAgentWorkflow_PersistsTurnIncrementally(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	registerAgentStubs(env)

	f := registerLLM(env, fetchPage())
	executePage(env)
	turn := store.TurnKey(f.session.add(store.HumanMessageKey("q"), store.Message{Role: store.RoleUser, Content: `"hello"`}), "reviewer")

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{SessionID: "s1", AgentID: "reviewer", TurnKey: turn})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}

	want := []persistCall{
		{turnKey: turn, startIndex: 0, roles: []string{"assistant", "tool"}},
		{turnKey: turn, startIndex: 2, roles: []string{"assistant"}},
	}
	if persists := f.session.persisted(); fmt.Sprint(persists) != fmt.Sprint(want) {
		t.Errorf("persisted %v, want %v", persists, want)
	}

	var out AgentWorkflowOutput
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.NewMessages) != 3 {
		t.Errorf("NewMessages = %d messages, want 3 (the turn only, not the history)", len(out.NewMessages))
	}
}

// TestAgentWorkflow_NoPersistWithoutTurn checks that a sub-agent or a scheduled
// run writes nothing: they own no session history.
func TestAgentWorkflow_NoPersistWithoutTurn(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	registerAgentStubs(env)

	f := registerLLM(env, answers(done))

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{
		SessionID: "s1", AgentID: "reviewer", UserMessage: "hello",
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if persists := f.session.persisted(); len(persists) != 0 {
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

	f := registerLLM(env, func(int, provider.ChatRequest) (provider.ChatResponse, error) {
		return provider.ChatResponse{}, &provider.PermanentAPIError{Err: errors.New("overloaded")}
	})

	f.session.add(store.HumanMessageKey("q"), store.Message{Role: store.RoleUser, Content: `"hello"`})
	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{SessionID: "s1", AgentID: "reviewer", UserMessage: "hello"})

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
		t.Errorf("NewMessages = %+v, want the task to survive", out.NewMessages)
	}
}

// TestAgentWorkflow_SubAgentLoadsNoHistory checks that a run without a turn key
// never reads the session transcript: a sub-agent's context is isolated, its
// conversation given inline.
func TestAgentWorkflow_SubAgentLoadsNoHistory(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	registerAgentStubs(env)
	f := registerLLM(env, answers(done))
	f.session.add(store.HumanMessageKey("a"), store.Message{Role: store.RoleUser, Content: `"the session's"`})

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{
		SessionID: "child-1", AgentID: "reviewer", UserMessage: "sub task",
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if sent := f.model.sent(); len(sent) != 1 || len(sent[0].Messages) != 1 || textOf(sent[0].Messages[0]) != "sub task" {
		t.Errorf("sub-agent saw %+v, want only its own task", sent)
	}
	if f.session.loads != 0 {
		t.Errorf("the conversation was loaded %d times, want never for a sub-agent", f.session.loads)
	}
}

// TestAgentWorkflow_ScheduledRunLoadsOnlyTheUserMemory checks that a run with
// no turn key that asks for its user's memory gets it, and nothing of a
// session history.
func TestAgentWorkflow_ScheduledRunLoadsOnlyTheUserMemory(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	registerAgentStubs(env)
	f := registerLLM(env, answers(done))
	f.session.memory["victor"] = store.Memory{Content: "likes concise answers", Version: 1}

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{
		SessionID: "schedule-1", UserID: "victor", AgentID: "default",
		UserMessage: "check my reminders", LoadUserMemory: true,
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if f.session.loads != 0 {
		t.Errorf("the conversation was loaded %d times, want never without a turn key", f.session.loads)
	}
	seen := f.model.sent()[0]
	if !strings.Contains(seen.System, "likes concise answers") {
		t.Errorf("system prompt lacks the user memory: %q", seen.System)
	}
	if len(seen.Messages) != 1 || textOf(seen.Messages[0]) != "check my reminders" {
		t.Errorf("model saw %+v, want the task's prompt alone", seen.Messages)
	}
}

// TestAgentWorkflow_LoadsItsOwnHistory checks that a session turn's model
// reads the transcript, which the workflow never holds, and its user's
// memory.
func TestAgentWorkflow_LoadsItsOwnHistory(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	registerAgentStubs(env)
	f := registerLLM(env, answers(done))
	f.session.memory["victor"] = store.Memory{Content: "likes concise answers", Version: 1}
	f.session.add(store.HumanMessageKey("a"), store.Message{Role: store.RoleUser, Content: `"earlier question"`})
	f.session.add(store.TurnMessageKey(store.TurnKey(1, "reviewer"), 0), store.Message{Role: store.RoleAssistant, Content: `"earlier answer"`})
	upTo := f.session.add(store.HumanMessageKey("b"), store.Message{Role: store.RoleUser, Content: `"new question"`})

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{
		SessionID: "s1", UserID: "victor", AgentID: "reviewer", TurnKey: store.TurnKey(upTo, "reviewer"),
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	seen := f.model.sent()[0]
	if len(seen.Messages) != 3 || textOf(seen.Messages[2]) != "new question" {
		t.Fatalf("model saw %+v, want the 3 stored", seen.Messages)
	}
	if !strings.Contains(seen.System, "likes concise answers") {
		t.Errorf("system prompt lost the user memory: %q", seen.System)
	}

	var out AgentWorkflowOutput
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.NewMessages) != 1 {
		t.Errorf("NewMessages = %d, want only the turn's answer, not the history", len(out.NewMessages))
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

// The message a turn answers is loaded with the history: the server stored
// it, the turn never adds it.
func TestAgentWorkflow_StoredMessageIsNotAddedAgain(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	registerAgentStubs(env)
	f := registerLLM(env, answers(provider.ChatResponse{Content: "summary", StopReason: "end_turn"}))
	f.session.add(store.HumanMessageKey("a"), store.Message{Role: store.RoleUser, Content: `"we talked"`, Author: "Alice"})
	upTo := f.session.add(store.HumanMessageKey("b"), store.Message{Role: store.RoleUser, Content: `"@agent sum it up"`, Author: "Bob"})

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{
		SessionID: "s1", UserID: "u-bob", UserName: "Bob", AgentID: "reviewer",
		TurnKey: store.TurnKey(upTo, "reviewer"),
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	seen := f.model.sent()[0]
	if len(seen.Messages) != 1 || textOf(seen.Messages[0]) != "[Alice] we talked\n\n[Bob] @agent sum it up" {
		t.Errorf("model saw %+v, want the 2 stored messages, in one", seen.Messages)
	}
	for _, p := range f.session.persisted() {
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
	registerLLM(env, answers(provider.ChatResponse{Content: "the analysis", StopReason: "end_turn"}))
	var channels []string
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.NotifyInput) error {
		channels = append(channels, in.Channel)
		return nil
	}, sdkactivity.RegisterOptions{Name: "NotifyStep"})

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{
		SessionID: "s1:p:jarvis:m3:tool:agent_analyst:1", AgentID: "analyst", UserMessage: "go",
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
	sub := AgentWorkflowInput{SessionID: "s1:p:jarvis:m3:tool:agent_analyst:1", Channel: "telegram", ChannelID: "42"}
	res := activity.ToolResolution{Kind: "workflow", WorkflowName: "AskUserWorkflow", NeedsCallContext: true}
	_, in, err := buildChildInput(json.RawMessage(`{"question":"ok?","agent":"forged"}`), sub, "child", &res, callContext(sub, []string{"default", "analyst"}, "Analyst", "agent"), "analyst", "agent")
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
	registerLLM(env, answers(provider.ChatResponse{Content: "the answer", StopReason: "end_turn"}))
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

	f := registerLLM(env, answers(provider.ChatResponse{ToolCalls: []provider.ToolCallInfo{
		{ID: "c1", Name: "agent_root", Input: json.RawMessage(`{"task":"ask root"}`)},
	}}, done))

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{
		SessionID: "s1:p:jarvis:m3:tool:agent_analyst:c0", AgentID: "analyst", AgentChain: []string{"root"}, UserMessage: "go",
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if fmt.Sprint(listed) != "[analyst]" {
		t.Errorf("tools listed for %v: a child agent ran", listed)
	}
	var result *provider.ToolResultInfo
	for _, m := range f.model.sent()[1].Messages {
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
			env.RegisterActivityWithOptions(func(ctx context.Context, in activity.ListToolsInput) (activity.ListToolsOutput, error) {
				return activity.ListToolsOutput{}, nil
			}, sdkactivity.RegisterOptions{Name: "ListTools"})
			env.RegisterActivityWithOptions(func(ctx context.Context, in activity.LoadSkillsForAgentInput) (activity.LoadSkillsForAgentOutput, error) {
				return activity.LoadSkillsForAgentOutput{SystemPrompt: "prompt", Name: "Agent Smith"}, nil
			}, sdkactivity.RegisterOptions{Name: "LoadSkillsForAgent"})
			f := registerLLM(env, answers(provider.ChatResponse{Content: "it fits", StopReason: "end_turn"}))
			var answers []string
			env.RegisterActivityWithOptions(func(ctx context.Context, in activity.NotifyInput) error {
				answers = append(answers, string(in.Event.Data))
				return nil
			}, sdkactivity.RegisterOptions{Name: "NotifyStep"})

			env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{
				SessionID: "s1", AgentID: "smith", TurnKey: store.TurnKey(f.session.add(store.HumanMessageKey("q"), store.Message{Role: store.RoleUser, Content: `"go"`}), "smith"),
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
			if system := f.model.sent()[0].System; !strings.HasSuffix(system, "\n## PART\n") {
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

// A turn's LLM calls carry references, never the conversation: an input does
// not grow with the session's history, nor with the turn's iterations. Before,
// each input was the whole request the model reads (what the fake model
// receives here), recorded in the workflow's history on every call.
func TestAgentWorkflow_LLMInputsStaySmall(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	registerAgentStubs(env)
	executePage(env)
	fetch := provider.ChatResponse{ToolCalls: []provider.ToolCallInfo{{ID: "1", Name: "web_fetch", Input: json.RawMessage(`{}`)}}}
	f := registerLLM(env, func(n int, _ provider.ChatRequest) (provider.ChatResponse, error) {
		if n <= 5 {
			fetch.ToolCalls[0].ID = fmt.Sprint(n)
			return fetch, nil
		}
		return done, nil
	})
	var upTo int64
	for i := range 150 { // about 1.5 MB of history
		role := store.RoleUser
		if i%2 == 1 {
			role = store.RoleAssistant
		}
		content, _ := json.Marshal(fmt.Sprintf("message %d: %s", i, strings.Repeat("lorem ipsum ", 850)))
		upTo = f.session.add(store.HumanMessageKey(fmt.Sprint(i)), store.Message{Role: role, Content: string(content)})
	}

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{
		SessionID: "s1", UserID: "victor", AgentID: "reviewer",
		TurnKey: store.TurnKey(upTo, "reviewer"),
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}

	sizes, sent := f.inputSizes(), f.model.sent()
	if len(sizes) != 6 {
		t.Fatalf("%d LLM calls, want 6", len(sizes))
	}
	for i, size := range sizes {
		if size > 1024 {
			t.Errorf("call %d: input of %d bytes, want under 1 KiB whatever the history", i+1, size)
		}
	}
	read, _ := json.Marshal(sent[len(sent)-1])
	if len(read) < 1_500_000 {
		t.Errorf("the model read %d bytes, want the whole history", len(read))
	}
	t.Logf("CallLLM input: %v bytes; the request the model read (the input before): %d bytes", sizes, len(read))
}

// A flush that failed leaves the turn's messages unwritten: the next call
// still reads them, from its input, and once when the flush wrote them after
// all. The end of the turn writes them, once.
func TestAgentWorkflow_UnwrittenMessagesReachTheModel(t *testing.T) {
	for _, writeThenFail := range []bool{false, true} {
		t.Run(fmt.Sprint("written=", writeThenFail), func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			registerAgentStubs(env)
			executePage(env)
			f := registerLLM(env, fetchPage())
			upTo := f.session.add(store.HumanMessageKey("q"), store.Message{Role: store.RoleUser, Content: `"read it"`})
			// The flush after the tool call fails, every attempt.
			f.session.failPersists, f.session.writeThenFail = 3, writeThenFail

			env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{
				SessionID: "s1", AgentID: "reviewer", TurnKey: store.TurnKey(upTo, "reviewer"),
			})
			if err := env.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}

			second := f.model.sent()[1].Messages
			if len(second) != 3 || len(second[1].ToolCalls) != 1 || second[2].ToolResult == nil || second[2].ToolResult.Content != "page content" {
				t.Errorf("second call read %+v, want the question, the call and its result, once each", second)
			}
			if h := f.session.history(); len(h) != 4 {
				t.Errorf("stored %d messages, want the question and the turn's 3, once each", len(h))
			}
		})
	}
}

// The model is offered the tools the workflow dispatches, defined by the
// catalog; one gone from the catalog is not offered.
func TestAgentWorkflow_OffersTheDispatchableTools(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	schema := json.RawMessage(`{"type":"object"}`)
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.ListToolsInput) (activity.ListToolsOutput, error) {
		return activity.ListToolsOutput{
			Tools: []provider.ToolDefinition{{Name: "gone", InputSchema: schema}, {Name: "web_fetch", InputSchema: schema}, {Name: "web_search", InputSchema: schema}},
			Resolutions: map[string]activity.ToolResolution{
				"gone": {Kind: "activity"}, "web_fetch": {Kind: "activity"}, "web_search": {Kind: "activity"},
			},
		}, nil
	}, sdkactivity.RegisterOptions{Name: "ListTools"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.LoadSkillsForAgentInput) (activity.LoadSkillsForAgentOutput, error) {
		return activity.LoadSkillsForAgentOutput{}, nil
	}, sdkactivity.RegisterOptions{Name: "LoadSkillsForAgent"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.NotifyInput) error { return nil }, sdkactivity.RegisterOptions{Name: "NotifyStep"})
	f := registerLLM(env, answers(done))
	var prompted []string
	f.llm.Prompts = promptFunc(func(_ string, tools []string) string { prompted = tools; return "prompt" })

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{SessionID: "s1", AgentID: "dev", UserMessage: "go"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var offered []string
	for _, tool := range f.model.sent()[0].Tools {
		offered = append(offered, tool.Name)
	}
	if fmt.Sprint(offered) != "[web_fetch web_search]" || fmt.Sprint(prompted) != "[web_fetch web_search]" {
		t.Errorf("offered %v, prompt for %v; want the dispatchable tools the catalog defines", offered, prompted)
	}
}

// A conversation too long for the model ends the turn at once, not retried,
// with what the members must do.
func TestAgentWorkflow_AConversationTooLongEndsTheTurn(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	registerAgentStubs(env)
	f := registerLLM(env, answers(done))
	f.llm.MaxContextBytes = 2000
	upTo := f.session.add(store.HumanMessageKey("q"), store.Message{Role: store.RoleUser, Content: `"` + strings.Repeat("x", 3000) + `"`})

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{
		SessionID: "s1", AgentID: "reviewer", TurnKey: store.TurnKey(upTo, "reviewer"),
	})
	var out AgentWorkflowOutput
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatal(err)
	}
	if out.Error != activity.ContextTooLongMessage {
		t.Errorf("error %q, want %q", out.Error, activity.ContextTooLongMessage)
	}
	if out.ErrorType != activity.ErrContextTooLong {
		t.Errorf("error type %q, want %q", out.ErrorType, activity.ErrContextTooLong)
	}
	if n := len(f.inputSizes()); n != 1 {
		t.Errorf("CallLLM ran %d times, want once: never retried", n)
	}
	if len(f.model.sent()) != 0 {
		t.Error("the model was called")
	}
}

// A sub-agent runs for real: its conversation, its task alone, goes inline,
// and its answer reaches the parent's next call.
func TestAgentWorkflow_SubAgentRunsInline(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.ListToolsInput) (activity.ListToolsOutput, error) {
		if in.AgentID != "default" {
			return activity.ListToolsOutput{}, nil
		}
		return activity.ListToolsOutput{
			Tools:       []provider.ToolDefinition{{Name: "agent_analyst"}},
			Resolutions: map[string]activity.ToolResolution{"agent_analyst": {Kind: "workflow", AgentID: "analyst"}},
		}, nil
	}, sdkactivity.RegisterOptions{Name: "ListTools"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.LoadSkillsForAgentInput) (activity.LoadSkillsForAgentOutput, error) {
		return activity.LoadSkillsForAgentOutput{}, nil
	}, sdkactivity.RegisterOptions{Name: "LoadSkillsForAgent"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.NotifyInput) error { return nil }, sdkactivity.RegisterOptions{Name: "NotifyStep"})
	var child provider.ChatRequest
	f := registerLLM(env, func(n int, req provider.ChatRequest) (provider.ChatResponse, error) {
		switch {
		case textOf(req.Messages[0]) == "summarize the CAC 40":
			child = req
			return provider.ChatResponse{Content: "CAC 40 summary", StopReason: "end_turn"}, nil
		case n == 1:
			return provider.ChatResponse{ToolCalls: []provider.ToolCallInfo{{ID: "1", Name: "agent_analyst", Input: json.RawMessage(`{"task":"summarize the CAC 40"}`)}}}, nil
		}
		return done, nil
	})
	f.catalog.SetAgents([]activity.AgentCatalogEntry{{ID: "default", Tools: []string{"agent_*"}}, {ID: "analyst", Name: "Analyst"}})
	upTo := f.session.add(store.HumanMessageKey("q"), store.Message{Role: store.RoleUser, Content: `"CAC 40?"`})

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{SessionID: "s1", AgentID: "default", TurnKey: store.TurnKey(upTo, "default")})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(child.Messages) != 1 || len(child.Tools) != 0 {
		t.Errorf("the sub-agent read %+v with tools %+v, want its task alone", child.Messages, child.Tools)
	}
	sent := f.model.sent()
	last := sent[len(sent)-1].Messages
	if r := last[len(last)-1].ToolResult; r == nil || r.Content != "CAC 40 summary" {
		t.Errorf("the parent's last call read %+v, want the sub-agent's answer last", last)
	}
	if f.session.loads != 2 {
		t.Errorf("the conversation was loaded %d times, want twice: the parent's calls only", f.session.loads)
	}
}
