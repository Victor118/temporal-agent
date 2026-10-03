package workflow

import (
	"context"
	"errors"
	"testing"

	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	sdkworkflow "go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/tool"
)

// TestScheduledAgentWorkflow_Cleanup checks that the schedule is deleted after
// a one-shot run, but kept for a recurring (cron) task.
func TestScheduledAgentWorkflow_Cleanup(t *testing.T) {
	cases := map[string]struct {
		cron       string
		wantDelete bool
	}{
		"one-shot":  {cron: "", wantDelete: true},
		"recurring": {cron: "*/2 * * * *", wantDelete: false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()

			env.RegisterWorkflowWithOptions(func(ctx sdkworkflow.Context, in AgentWorkflowInput) (AgentWorkflowOutput, error) {
				return AgentWorkflowOutput{Response: "done"}, nil
			}, sdkworkflow.RegisterOptions{Name: "AgentWorkflow"})

			var delivered string
			env.RegisterActivityWithOptions(func(ctx context.Context, in activity.DeliverInput) error {
				delivered = in.Content
				return nil
			}, sdkactivity.RegisterOptions{Name: "DeliverResult"})

			deleted, status := false, ""
			env.RegisterActivityWithOptions(func(ctx context.Context, in activity.DeleteScheduleInput) error {
				deleted, status = true, in.Status
				return nil
			}, sdkactivity.RegisterOptions{Name: "DeleteSchedule"})

			env.ExecuteWorkflow(ScheduledAgentWorkflow, tool.ScheduledAgentInput{
				AgentID:    "default",
				Prompt:     "tell a joke",
				UserID:     "victor",
				ScheduleID: "schedule-test",
				Cron:       tc.cron,
			})

			if !env.IsWorkflowCompleted() {
				t.Fatal("workflow did not complete")
			}
			if err := env.GetWorkflowError(); err != nil {
				t.Fatalf("workflow error: %v", err)
			}
			if delivered != "done" {
				t.Errorf("delivered = %q, want done", delivered)
			}
			if deleted != tc.wantDelete {
				t.Errorf("schedule deleted = %v, want %v", deleted, tc.wantDelete)
			}
			if deleted && status != store.TaskCompleted {
				t.Errorf("task log closed as %q, want completed", status)
			}
		})
	}
}

// A one-shot schedule has fired its only action: a delivery that failed still
// deletes it, but closes its task log as failed, and the run reports the
// failure. So does a run of the agent that failed, its error delivered.
func TestScheduledAgentWorkflow_FailedOneShotIsClosedAsFailed(t *testing.T) {
	cases := map[string]struct {
		agent       AgentWorkflowOutput
		deliverErr  error
		wantErrored bool
	}{
		"delivery failed": {agent: AgentWorkflowOutput{Response: "done"}, deliverErr: errors.New("server refused the notification"), wantErrored: true},
		"agent failed":    {agent: AgentWorkflowOutput{Error: "LLM unavailable"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()

			env.RegisterWorkflowWithOptions(func(ctx sdkworkflow.Context, in AgentWorkflowInput) (AgentWorkflowOutput, error) {
				return tc.agent, nil
			}, sdkworkflow.RegisterOptions{Name: "AgentWorkflow"})
			env.RegisterActivityWithOptions(func(ctx context.Context, in activity.DeliverInput) error {
				return tc.deliverErr
			}, sdkactivity.RegisterOptions{Name: "DeliverResult"})
			var deleted activity.DeleteScheduleInput
			env.RegisterActivityWithOptions(func(ctx context.Context, in activity.DeleteScheduleInput) error {
				deleted = in
				return nil
			}, sdkactivity.RegisterOptions{Name: "DeleteSchedule"})

			env.ExecuteWorkflow(ScheduledAgentWorkflow, tool.ScheduledAgentInput{
				AgentID: "default", Prompt: "remind me", UserID: "victor", ScheduleID: "schedule-test",
			})

			if !env.IsWorkflowCompleted() {
				t.Fatal("workflow did not complete")
			}
			if errored := env.GetWorkflowError() != nil; errored != tc.wantErrored {
				t.Errorf("workflow error = %v, want an error: %v", env.GetWorkflowError(), tc.wantErrored)
			}
			if deleted.ScheduleID != "schedule-test" || deleted.Status != store.TaskFailed {
				t.Errorf("deleted %+v, want schedule-test closed as failed", deleted)
			}
		})
	}
}

// A scheduled task runs for the user who scheduled it: without the user, the
// scheduling tools answer "user not identified" and the memory stays out.
func TestScheduledAgentWorkflow_RunsForItsUser(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var got AgentWorkflowInput
	env.RegisterWorkflowWithOptions(func(ctx sdkworkflow.Context, in AgentWorkflowInput) (AgentWorkflowOutput, error) {
		got = in
		return AgentWorkflowOutput{Response: "done"}, nil
	}, sdkworkflow.RegisterOptions{Name: "AgentWorkflow"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.DeliverInput) error {
		return nil
	}, sdkactivity.RegisterOptions{Name: "DeliverResult"})

	env.ExecuteWorkflow(ScheduledAgentWorkflow, tool.ScheduledAgentInput{
		AgentID: "default", Prompt: "check my reminders", UserID: "victor",
		ScheduleID: "schedule-test", Cron: "0 9 * * 1",
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if got.UserID != "victor" || !got.LoadUserMemory {
		t.Errorf("agent input UserID = %q, LoadUserMemory = %v; want victor, true", got.UserID, got.LoadUserMemory)
	}
	if got.TurnKey != "" {
		t.Errorf("TurnKey = %q: a scheduled run owns no session history", got.TurnKey)
	}
}
