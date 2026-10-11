package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	sdkworkflow "go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/tool"
)

// accessEnv runs AgentWorkflow over a ListTools that closes the agent
// "commercial" to Bob, and its tool crm_update to everyone; it records who
// each ListTools was for.
func accessEnv(t *testing.T, answer func(int, provider.ChatRequest) (provider.ChatResponse, error)) (*testsuite.TestWorkflowEnvironment, *llmFakes, func() []activity.ListToolsInput) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(AgentWorkflow)
	var mu sync.Mutex
	var asked []activity.ListToolsInput
	schema := json.RawMessage(`{"type":"object"}`)
	env.RegisterActivityWithOptions(func(_ context.Context, in activity.ListToolsInput) (activity.ListToolsOutput, error) {
		mu.Lock()
		asked = append(asked, in)
		mu.Unlock()
		switch {
		case in.AgentID == "commercial" && in.UserID == "u-bob":
			return activity.ListToolsOutput{Refused: "agent commercial not open to u-bob"}, nil
		case in.AgentID == "commercial":
			return activity.ListToolsOutput{
				Tools:       []provider.ToolDefinition{{Name: "web_fetch", InputSchema: schema}},
				Resolutions: map[string]activity.ToolResolution{"web_fetch": {Kind: "activity", TaskQueue: "tools"}},
				Closed:      []string{"crm_update"},
			}, nil
		}
		// Without the new fields, as a worker from before them answers:
		// open, nothing closed.
		return activity.ListToolsOutput{
			Tools:       []provider.ToolDefinition{{Name: "agent_commercial", InputSchema: schema}},
			Resolutions: map[string]activity.ToolResolution{"agent_commercial": {Kind: "workflow", AgentID: "commercial", Background: true}},
		}, nil
	}, sdkactivity.RegisterOptions{Name: "ListTools"})
	env.RegisterActivityWithOptions(func(_ context.Context, in activity.LoadSkillsForAgentInput) (activity.LoadSkillsForAgentOutput, error) {
		return activity.LoadSkillsForAgentOutput{Name: strings.ToUpper(in.AgentID[:1]) + in.AgentID[1:]}, nil
	}, sdkactivity.RegisterOptions{Name: "LoadSkillsForAgent"})
	env.RegisterActivityWithOptions(func(context.Context, activity.NotifyInput) error { return nil }, sdkactivity.RegisterOptions{Name: "NotifyStep"})
	f := registerLLM(env, answer)
	return env, f, func() []activity.ListToolsInput {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(asked)
	}
}

// An agent closed to the turn's user stops the turn before its model, with
// AgentNotOpen and what the members read.
func TestAgentWorkflow_ClosedAgentStopsBeforeTheModel(t *testing.T) {
	env, f, asked := accessEnv(t, answers(done))
	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{SessionID: "s1", AgentID: "commercial", UserID: "u-bob", UserName: "Bob", UserMessage: "hi"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var out AgentWorkflowOutput
	env.GetWorkflowResult(&out)
	if out.ErrorType != activity.AgentNotOpen || out.Error != AgentNotOpenMessage("Commercial", "Bob") || out.Response != "" {
		t.Errorf("output %+v", out)
	}
	if n := len(f.model.sent()); n != 0 {
		t.Errorf("the model was called %d times", n)
	}
	if got := asked(); len(got) != 1 || got[0].UserID != "u-bob" {
		t.Errorf("ListTools asked %+v", got)
	}
}

// A sub-agent's tools are those open to its parent's user: it asks for
// them, and its prompt names the ones closed. Closed to that user (since
// the parent's turn started), the parent reads why it got no answer.
func TestAgentWorkflow_SubAgentFilteredForTheSameUser(t *testing.T) {
	delegate := provider.ChatResponse{ToolCalls: []provider.ToolCallInfo{{ID: "c1", Name: "agent_commercial", Input: json.RawMessage(`{"task":"find Acme"}`)}}}
	for _, user := range []string{"u-alice", "u-bob"} {
		t.Run(user, func(t *testing.T) {
			env, f, asked := accessEnv(t, func(n int, req provider.ChatRequest) (provider.ChatResponse, error) {
				if n == 1 {
					return delegate, nil
				}
				return done, nil
			})
			env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{SessionID: "s1", AgentID: "jarvis", UserID: user, UserName: "Someone", UserMessage: "go"})
			if err := env.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}
			got := asked()
			if len(got) != 2 || got[1].AgentID != "commercial" || got[1].UserID != user {
				t.Fatalf("ListTools asked %+v", got)
			}
			sent := f.model.sent()
			var result *provider.ToolResultInfo
			for _, m := range sent[len(sent)-1].Messages {
				if m.ToolResult != nil {
					result = m.ToolResult
				}
			}
			if user == "u-bob" {
				if len(sent) != 2 || result == nil || !result.IsError || result.Content != subAgentNotOpen {
					t.Errorf("%d calls, the parent read %+v", len(sent), result)
				}
				return
			}
			// The sub-agent's call: its prompt names the tool closed.
			if len(sent) != 3 || !strings.Contains(sent[1].System, "Some of your tools are not open to this user: crm_update.") {
				t.Errorf("%d calls, the sub-agent's prompt %q", len(sent), sent[1].System)
			}
			if result == nil || result.IsError || result.Content != "done" {
				t.Errorf("the parent read %+v", result)
			}
		})
	}
}

// An agent refused is the agent's refusal, not the message's: its turn ends
// with the error, told on the channel, and the message is relayed to the
// next agent, which does not read the refused turn.
func TestParticipant_RelaysPastAClosedAgent(t *testing.T) {
	h := newHarness(t, bySystem(map[string]func(int) provider.ChatResponse{
		"smith": func(int) provider.ChatResponse { return say("jugé") },
	}), nil)
	h.closed = map[string]bool{"jarvis": true}
	msg := h.human("@jarvis résume, @smith juge", "Alice")
	msg.Next, msg.SignReply = []AddressedAgent{{ID: "smith", Name: "Agent Smith", Mention: "smith"}}, true
	if err := h.run("jarvis", msg); err != nil {
		t.Fatal(err)
	}
	if n := len(h.f.model.sent()); n != 0 {
		t.Errorf("the model was called %d times", n)
	}
	want := AgentNotOpenMessage("Jarvis", "Alice")
	if got := h.ends()["m1.jarvis"]; got != want {
		t.Errorf("turn end %q, want %q", got, want)
	}
	if !slices.Contains(h.events(), "message Error processing message: "+want) {
		t.Errorf("events %v", h.events())
	}
	if len(h.relays) != 1 {
		t.Fatalf("%d relays, want one", len(h.relays))
	}
	next, _ := relayed(t, h.relays[0])
	if h.relays[0].WorkflowID != "s1:p:smith" || len(next.EarlierTurns) != 0 || len(next.Next) != 0 {
		t.Errorf("relayed %+v to %s", next, h.relays[0].WorkflowID)
	}
}

// A recurring task its agent refuses is paused, its owner told once; a
// one-shot fails as any run does.
func TestScheduledAgentWorkflow_PausedWhenRefused(t *testing.T) {
	for name, cron := range map[string]string{"recurring": "0 9 * * *", "one-shot": ""} {
		t.Run(name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.RegisterWorkflowWithOptions(func(sdkworkflow.Context, AgentWorkflowInput) (AgentWorkflowOutput, error) {
				return AgentWorkflowOutput{Error: AgentNotOpenMessage("Commercial", ""), ErrorType: activity.AgentNotOpen}, nil
			}, sdkworkflow.RegisterOptions{Name: "AgentWorkflow"})
			var delivered []string
			env.RegisterActivityWithOptions(func(_ context.Context, in activity.DeliverInput) error {
				delivered = append(delivered, in.Content)
				return nil
			}, sdkactivity.RegisterOptions{Name: "DeliverResult"})
			var paused []activity.PauseScheduleInput
			env.RegisterActivityWithOptions(func(_ context.Context, in activity.PauseScheduleInput) error {
				paused = append(paused, in)
				return nil
			}, sdkactivity.RegisterOptions{Name: "PauseSchedule"})
			var deleted []activity.DeleteScheduleInput
			env.RegisterActivityWithOptions(func(_ context.Context, in activity.DeleteScheduleInput) error {
				deleted = append(deleted, in)
				return nil
			}, sdkactivity.RegisterOptions{Name: "DeleteSchedule"})

			env.ExecuteWorkflow(ScheduledAgentWorkflow, tool.ScheduledAgentInput{AgentID: "commercial", Prompt: "report", UserID: "bob", ScheduleID: "sched-1", Cron: cron})
			if err := env.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}
			if len(delivered) != 1 {
				t.Fatalf("delivered %v", delivered)
			}
			if cron == "" {
				if len(paused) != 0 || len(deleted) != 1 || deleted[0].Status != store.TaskFailed {
					t.Errorf("one-shot: paused %v, deleted %v", paused, deleted)
				}
				return
			}
			if len(paused) != 1 || paused[0].ScheduleID != "sched-1" || len(deleted) != 0 {
				t.Errorf("paused %v, deleted %v", paused, deleted)
			}
			if !strings.HasPrefix(delivered[0], "Tâche planifiée mise en pause : ") {
				t.Errorf("delivered %q", delivered[0])
			}
		})
	}
}

// A worker whose catalog is not ready fails ListTools: the turn retries it
// for about two minutes, then fails without calling the model.
func TestAgentWorkflow_WaitsForAReadyCatalog(t *testing.T) {
	for name, failures := range map[string]int{"ready in time": 3, "never ready": 1000} {
		t.Run(name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			calls := 0
			env.RegisterActivityWithOptions(func(context.Context, activity.ListToolsInput) (activity.ListToolsOutput, error) {
				calls++
				if calls <= failures {
					return activity.ListToolsOutput{}, errors.New("this worker's catalog has not read its agents and access rules yet")
				}
				return activity.ListToolsOutput{}, nil
			}, sdkactivity.RegisterOptions{Name: "ListTools"})
			env.RegisterActivityWithOptions(func(context.Context, activity.LoadSkillsForAgentInput) (activity.LoadSkillsForAgentOutput, error) {
				return activity.LoadSkillsForAgentOutput{}, nil
			}, sdkactivity.RegisterOptions{Name: "LoadSkillsForAgent"})
			env.RegisterActivityWithOptions(func(context.Context, activity.NotifyInput) error { return nil }, sdkactivity.RegisterOptions{Name: "NotifyStep"})
			f := registerLLM(env, answers(done))
			env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{SessionID: "s1", AgentID: "jarvis", UserID: "u-bob", UserMessage: "hi"})
			err := env.GetWorkflowError()
			if failures < 1000 {
				if err != nil || len(f.model.sent()) != 1 || calls != failures+1 {
					t.Errorf("err %v, %d model calls, %d ListTools", err, len(f.model.sent()), calls)
				}
				return
			}
			if err == nil || len(f.model.sent()) != 0 || calls != 12 {
				t.Errorf("err %v, %d model calls, %d ListTools", err, len(f.model.sent()), calls)
			}
		})
	}
}
