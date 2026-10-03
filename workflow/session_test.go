package workflow

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	sdkworkflow "go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/store"
)

// TestSessionWorkflow_IgnoresEmptyMessage checks the backstop: a blank message
// from any channel must not start a turn. Persisting an empty user message
// breaks every later turn, since the LLM API rejects one.
func TestSessionWorkflow_IgnoresEmptyMessage(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	turns := 0
	env.RegisterWorkflowWithOptions(func(ctx sdkworkflow.Context, in AgentWorkflowInput) (AgentWorkflowOutput, error) {
		turns++
		return AgentWorkflowOutput{Response: "done"}, nil
	}, sdkworkflow.RegisterOptions{Name: "AgentWorkflow"})

	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.NotifyInput) error {
		return nil
	}, sdkactivity.RegisterOptions{Name: "NotifyStep"})

	// The real message proves the signal path works, so the two blanks being
	// dropped is the guard and not a test that never signalled anything.
	for i, msg := range []string{"", "   \n\t ", "a real question"} {
		msg := msg
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(SignalUserMessage, UserMessage{Text: msg, UserID: "victor"})
		}, time.Duration(i+1)*time.Second)
	}

	env.ExecuteWorkflow(SessionWorkflow, SessionWorkflowInput{
		SessionID: "s1", AgentID: "default",
	})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if turns != 1 {
		t.Errorf("ran %d agent turns, want 1: the blanks dropped, the real message through", turns)
	}
}

// TestSessionWorkflow_EachTurnAnswersItsAuthor checks a shared session: every
// turn runs for the user who wrote the message, so it loads that user's
// memory and its tools act for that user.
func TestSessionWorkflow_EachTurnAnswersItsAuthor(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var turns []AgentWorkflowInput
	env.RegisterWorkflowWithOptions(func(ctx sdkworkflow.Context, in AgentWorkflowInput) (AgentWorkflowOutput, error) {
		turns = append(turns, in)
		return AgentWorkflowOutput{Response: "done"}, nil
	}, sdkworkflow.RegisterOptions{Name: "AgentWorkflow"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.NotifyInput) error {
		return nil
	}, sdkactivity.RegisterOptions{Name: "NotifyStep"})

	for i, msg := range []UserMessage{
		{Text: "hello", UserID: "u-alice", UserName: "Alice"},
		{Text: "hi", UserID: "u-bob", UserName: "Bob"},
	} {
		msg := msg
		env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalUserMessage, msg) }, time.Duration(i+1)*time.Second)
	}
	env.ExecuteWorkflow(SessionWorkflow, SessionWorkflowInput{SessionID: "s1", AgentID: "default"})

	if len(turns) != 2 {
		t.Fatalf("ran %d turns, want 2", len(turns))
	}
	for i, want := range []struct{ id, name, text string }{{"u-alice", "Alice", "hello"}, {"u-bob", "Bob", "hi"}} {
		if got := turns[i]; got.UserID != want.id || got.UserName != want.name || got.UserMessage != want.text {
			t.Errorf("turn %d: user %q (%q), message %q; want %q (%q), %q",
				i, got.UserID, got.UserName, got.UserMessage, want.id, want.name, want.text)
		}
	}
}

// A turn that fails is written to the conversation with why it failed, after
// what it produced: the members see it after a reload and on every channel,
// not only in a notification a page that reloads its thread from the store
// never shows.
func TestSessionWorkflow_RecordsWhyATurnFailed(t *testing.T) {
	call := store.Message{Role: store.RoleAssistant, ToolCalls: []store.ToolCall{{ID: "t1", Name: "analyze_repo"}}}
	result := store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t1", Content: "report"}}
	persisted, notified := runFailedTurn(AgentWorkflowOutput{NewMessages: []store.Message{call, result}, Error: "call LLM: " + strings.Repeat("é", 2000)})

	if len(persisted) != 1 {
		t.Fatalf("persisted %d times, want once", len(persisted))
	}
	msgs := persisted[0].Messages
	if len(msgs) != 3 || msgs[0].ToolCalls == nil || msgs[1].ToolResult == nil || msgs[2].Kind != store.KindTurnError {
		t.Fatalf("persisted %+v, want the call, its result, then the error", msgs)
	}
	var reason string
	if err := json.Unmarshal([]byte(msgs[2].Content), &reason); err != nil || !strings.HasPrefix(reason, "call LLM: é") {
		t.Errorf("error content %q (%v), want the reason as a JSON string", msgs[2].Content, err)
	}
	if len(reason) > maxTurnErrorBytes+len("…") || !utf8.ValidString(reason) {
		t.Errorf("reason of %d bytes, valid UTF-8 %v: want it cut on a rune, under the bound", len(reason), utf8.ValidString(reason))
	}
	if len(notified) == 0 {
		t.Error("the failure was not notified: the page would not reload its thread")
	}
}

// The first LLM call of a turn fails: the turn produced nothing (the human
// message is stored already), and the error is all there is to write.
func TestSessionWorkflow_RecordsAFailureThatProducedNothing(t *testing.T) {
	persisted, _ := runFailedTurn(AgentWorkflowOutput{Error: "call LLM: credit balance is too low"})

	if len(persisted) != 1 {
		t.Fatalf("persisted %d times, want once", len(persisted))
	}
	if msgs := persisted[0].Messages; len(msgs) != 1 || msgs[0].Kind != store.KindTurnError || msgs[0].Content != `"call LLM: credit balance is too low"` {
		t.Errorf("persisted %+v, want the error alone", msgs)
	}
}

// runFailedTurn runs one session turn whose agent returns out, and reports
// what was persisted and notified.
func runFailedTurn(out AgentWorkflowOutput) (persisted []activity.PersistContextInput, notified []string) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflowWithOptions(func(ctx sdkworkflow.Context, in AgentWorkflowInput) (AgentWorkflowOutput, error) {
		return out, nil
	}, sdkworkflow.RegisterOptions{Name: "AgentWorkflow"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.PersistContextInput) error {
		persisted = append(persisted, in)
		return nil
	}, sdkactivity.RegisterOptions{Name: "PersistContext"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.NotifyInput) error {
		notified = append(notified, string(in.Event.Data))
		return nil
	}, sdkactivity.RegisterOptions{Name: "NotifyStep"})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalUserMessage, UserMessage{Text: "analyse", UserID: "victor", Stored: true})
	}, time.Second)
	env.ExecuteWorkflow(SessionWorkflow, SessionWorkflowInput{SessionID: "s1", AgentID: "default"})
	return persisted, notified
}

// A stop sent while no turn runs must not interrupt the next one before it
// starts; a stop during a turn still interrupts it.
func TestSessionWorkflow_CancelOnlyStopsTheRunningTurn(t *testing.T) {
	for _, c := range []struct {
		name            string
		cancelAt        time.Duration
		wantInterrupted bool
	}{
		{"stop while idle", 1 * time.Second, false},
		{"stop during the turn", 5 * time.Second, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()

			env.RegisterWorkflowWithOptions(func(ctx sdkworkflow.Context, in AgentWorkflowInput) (AgentWorkflowOutput, error) {
				if err := sdkworkflow.Sleep(ctx, 10*time.Second); err != nil {
					return AgentWorkflowOutput{Response: "Agent cancelled."}, nil
				}
				return AgentWorkflowOutput{Response: "done"}, nil
			}, sdkworkflow.RegisterOptions{Name: "AgentWorkflow"})
			var notified []string
			env.RegisterActivityWithOptions(func(ctx context.Context, in activity.NotifyInput) error {
				notified = append(notified, string(in.Event.Data))
				return nil
			}, sdkactivity.RegisterOptions{Name: "NotifyStep"})

			env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalCancelAgent, nil) }, c.cancelAt)
			env.RegisterDelayedCallback(func() {
				env.SignalWorkflow(SignalUserMessage, UserMessage{Text: "go", UserID: "u-alice"})
			}, 2*time.Second)
			env.ExecuteWorkflow(SessionWorkflow, SessionWorkflowInput{SessionID: "s1", AgentID: "default"})

			interrupted := false
			for _, n := range notified {
				if strings.Contains(n, "interrupted") {
					interrupted = true
				}
			}
			if interrupted != c.wantInterrupted {
				t.Errorf("interrupted = %v, want %v (notifications %v)", interrupted, c.wantInterrupted, notified)
			}
		})
	}
}

// addressed is a message to @jarvis then @smith, in a session whose agent is
// "default".
var addressed = UserMessage{Text: "@jarvis résume, @smith juge", UserID: "u-alice", UserName: "Alice", Stored: true,
	Agents: []AddressedAgent{{ID: "jarvis", Name: "Jarvis", Mention: "jarvis"}, {ID: "smith", Name: "Agent Smith", Mention: "smith"}}}

// childRun is what a stubbed AgentWorkflow saw of its run.
type childRun struct {
	workflowID string
	in         AgentWorkflowInput
}

// runSession runs a session whose AgentWorkflow is agent, sends it msgs one
// second apart, and reports the turns in order, what was persisted and what
// was notified.
func runSession(t *testing.T, agent func(sdkworkflow.Context, AgentWorkflowInput) (AgentWorkflowOutput, error), cancelAt time.Duration, msgs ...UserMessage) (runs []childRun, persisted []activity.PersistContextInput, notified []string) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(func(ctx sdkworkflow.Context, in AgentWorkflowInput) (AgentWorkflowOutput, error) {
		runs = append(runs, childRun{workflowID: sdkworkflow.GetInfo(ctx).WorkflowExecution.ID, in: in})
		return agent(ctx, in)
	}, sdkworkflow.RegisterOptions{Name: "AgentWorkflow"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.PersistContextInput) error {
		persisted = append(persisted, in)
		return nil
	}, sdkactivity.RegisterOptions{Name: "PersistContext"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.NotifyInput) error {
		notified = append(notified, string(in.Event.Data))
		return nil
	}, sdkactivity.RegisterOptions{Name: "NotifyStep"})
	for i, m := range msgs {
		m := m
		env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalUserMessage, m) }, time.Duration(i+1)*time.Second)
	}
	if cancelAt > 0 {
		env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalCancelAgent, nil) }, cancelAt)
	}
	env.ExecuteWorkflow(SessionWorkflow, SessionWorkflowInput{SessionID: "s1", AgentID: "default", SystemPrompt: "OVERRIDE"})
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	return runs, persisted, notified
}

func answer(ctx sdkworkflow.Context, in AgentWorkflowInput) (AgentWorkflowOutput, error) {
	return AgentWorkflowOutput{Response: "done"}, nil
}

// A message to two agents runs them one after the other, each as a turn of
// its own: its own child ID and turn key, told its part, its answer signed.
// A plain message, then, runs the session's agent, as it always did.
func TestSessionWorkflow_RunsTheAddressedAgentsInOrder(t *testing.T) {
	plain := UserMessage{Text: "merci", UserID: "u-alice", Stored: true}
	runs, _, _ := runSession(t, answer, 0, addressed, plain)

	if len(runs) != 3 {
		t.Fatalf("ran %d turns, want 3", len(runs))
	}
	for i, want := range []struct {
		agent, id, prompt string
		sign              bool
		part              []string
	}{
		{"jarvis", "s1-turn-1", "", true, []string{"@jarvis, @smith", "You are @jarvis", "after you"}},
		{"smith", "s1-turn-2", "", true, []string{"You are @smith", "before you"}},
		{"default", "s1-turn-3", "OVERRIDE", false, nil},
	} {
		got := runs[i]
		if got.in.AgentID != want.agent || got.workflowID != want.id || got.in.SystemPrompt != want.prompt || got.in.SignReply != want.sign {
			t.Errorf("turn %d: agent %q, child %q, prompt %q, signed %v; want %q, %q, %q, %v",
				i, got.in.AgentID, got.workflowID, got.in.SystemPrompt, got.in.SignReply, want.agent, want.id, want.prompt, want.sign)
		}
		if !got.in.UserMessageStored || got.in.UserID != "u-alice" {
			t.Errorf("turn %d: stored %v, user %q: want the stored message, for its author", i, got.in.UserMessageStored, got.in.UserID)
		}
		for _, p := range want.part {
			if !strings.Contains(got.in.PartNote, p) {
				t.Errorf("turn %d: part note %q lacks %q", i, got.in.PartNote, p)
			}
		}
		if want.part == nil && got.in.PartNote != "" {
			t.Errorf("turn %d: part note %q, want none", i, got.in.PartNote)
		}
	}
	if runs[0].in.TurnKey == runs[1].in.TurnKey || runs[1].in.TurnKey == runs[2].in.TurnKey {
		t.Errorf("turn keys %q, %q, %q: want one per turn", runs[0].in.TurnKey, runs[1].in.TurnKey, runs[2].in.TurnKey)
	}
	if strings.Contains(runs[0].in.PartNote, "before you") || strings.Contains(runs[1].in.PartNote, "after you") {
		t.Errorf("part notes %q / %q: the first has no one before it, the last no one after", runs[0].in.PartNote, runs[1].in.PartNote)
	}
}

// One agent addressed, not the session's: no part to tell, but its answer is
// signed, or on the channel it would read as the session's agent's.
func TestSessionWorkflow_SignsAnotherAgentAlone(t *testing.T) {
	msg := addressed
	msg.Agents = msg.Agents[1:]
	runs, _, _ := runSession(t, answer, 0, msg)
	if len(runs) != 1 || runs[0].in.AgentID != "smith" || !runs[0].in.SignReply || runs[0].in.PartNote != "" || runs[0].in.SystemPrompt != "" {
		t.Errorf("runs %+v, want smith alone, signed, with its own prompt", runs)
	}
}

// An agent that fails ends the message: the agents after it do not run, and
// the failure is recorded under the agent that failed.
func TestSessionWorkflow_AFailureStopsTheRest(t *testing.T) {
	fail := func(ctx sdkworkflow.Context, in AgentWorkflowInput) (AgentWorkflowOutput, error) {
		return AgentWorkflowOutput{Error: "call LLM: boom"}, nil
	}
	runs, persisted, notified := runSession(t, fail, 0, addressed)

	if len(runs) != 1 || runs[0].in.AgentID != "jarvis" {
		t.Fatalf("runs %+v, want jarvis alone", runs)
	}
	if len(persisted) != 1 || len(persisted[0].Messages) != 1 {
		t.Fatalf("persisted %+v, want the error alone", persisted)
	}
	if m := persisted[0].Messages[0]; m.Kind != store.KindTurnError || m.AgentID != "jarvis" {
		t.Errorf("persisted %+v, want jarvis's turn error", m)
	}
	if len(notified) == 0 {
		t.Error("the failure was not notified")
	}
}

// Stopping the agent stops the message: the agents after it do not run.
func TestSessionWorkflow_ACancelStopsTheRest(t *testing.T) {
	slow := func(ctx sdkworkflow.Context, in AgentWorkflowInput) (AgentWorkflowOutput, error) {
		if err := sdkworkflow.Sleep(ctx, 10*time.Second); err != nil {
			return AgentWorkflowOutput{Response: "Agent cancelled."}, nil
		}
		return AgentWorkflowOutput{Response: "done"}, nil
	}
	runs, _, notified := runSession(t, slow, 5*time.Second, addressed)

	if len(runs) != 1 || runs[0].in.AgentID != "jarvis" {
		t.Errorf("runs %+v, want jarvis alone", runs)
	}
	if !strings.Contains(strings.Join(notified, " "), "interrupted") {
		t.Errorf("notified %v, want the interruption", notified)
	}
}

// The real AgentWorkflow, over a store in memory: the second agent loads the
// conversation after the first answered, and reads that answer under the
// first agent's name.
func TestSessionWorkflow_TheNextAgentSeesTheAnswerBefore(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(AgentWorkflow)

	history := []store.Message{{Role: store.RoleUser, Content: `"@jarvis résume, @smith juge"`, UserID: "u-alice", Author: "Alice"}}
	keys := map[string]bool{}
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.LoadContextInput) (activity.LoadContextOutput, error) {
		return activity.LoadContextOutput{Messages: append([]store.Message(nil), history...)}, nil
	}, sdkactivity.RegisterOptions{Name: "LoadContext"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.PersistContextInput) error {
		for i, m := range in.Messages {
			if k := store.TurnMessageKey(in.TurnKey, in.StartIndex+i); !keys[k] {
				keys[k] = true
				history = append(history, m)
			}
		}
		return nil
	}, sdkactivity.RegisterOptions{Name: "PersistContext"})
	names := map[string]string{"jarvis": "Jarvis", "smith": "Agent Smith"}
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.LoadSkillsForAgentInput) (activity.LoadSkillsForAgentOutput, error) {
		return activity.LoadSkillsForAgentOutput{SystemPrompt: "I am " + in.AgentID, Name: names[in.AgentID]}, nil
	}, sdkactivity.RegisterOptions{Name: "LoadSkillsForAgent"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.ListToolsInput) (activity.ListToolsOutput, error) {
		return activity.ListToolsOutput{}, nil
	}, sdkactivity.RegisterOptions{Name: "ListTools"})
	var requests []provider.ChatRequest
	env.RegisterActivityWithOptions(func(ctx context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
		requests = append(requests, req)
		if strings.HasPrefix(req.System, "I am jarvis") {
			return provider.ChatResponse{Content: "Temporal orchestre des workflows.", StopReason: "end_turn"}, nil
		}
		return provider.ChatResponse{Content: "Utile, oui.", StopReason: "end_turn"}, nil
	}, sdkactivity.RegisterOptions{Name: "CallLLM"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.NotifyInput) error {
		return nil
	}, sdkactivity.RegisterOptions{Name: "NotifyStep"})

	env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalUserMessage, addressed) }, time.Second)
	env.ExecuteWorkflow(SessionWorkflow, SessionWorkflowInput{SessionID: "s1", AgentID: "default"})

	if len(requests) != 2 {
		t.Fatalf("%d LLM calls, want 2", len(requests))
	}
	if !strings.HasPrefix(requests[0].System, "I am jarvis") || !strings.HasPrefix(requests[1].System, "I am smith") {
		t.Fatalf("calls in order %q, %q: want jarvis then smith", requests[0].System, requests[1].System)
	}
	if n := len(requests[0].Messages); n != 1 {
		t.Errorf("jarvis read %d messages, want the question alone", n)
	}
	seen := requests[1].Messages
	if len(seen) != 2 || string(seen[1].Content) != `"[Jarvis] Temporal orchestre des workflows."` {
		t.Errorf("smith read %+v, want the question then Jarvis's answer under his name", seen)
	}
	if len(history) != 3 || history[1].AgentID != "jarvis" || history[2].AgentID != "smith" || history[2].Author != "Agent Smith" {
		t.Errorf("history %+v, want the question, jarvis's answer, smith's", history)
	}
}
