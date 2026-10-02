package workflow

import (
	"context"
	"strings"
	"testing"
	"time"

	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	sdkworkflow "go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
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
