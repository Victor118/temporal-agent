package workflow

import (
	"context"
	"errors"
	"testing"

	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	sdkworkflow "go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
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

			deleted := false
			env.RegisterActivityWithOptions(func(ctx context.Context, in activity.DeleteScheduleInput) error {
				deleted = true
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
		})
	}
}

// A one-shot schedule has fired its only action: a delivery that failed still
// deletes it and closes its task log, and the run reports the failure.
func TestScheduledAgentWorkflow_CleansUpAfterAFailedDelivery(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflowWithOptions(func(ctx sdkworkflow.Context, in AgentWorkflowInput) (AgentWorkflowOutput, error) {
		return AgentWorkflowOutput{Response: "done"}, nil
	}, sdkworkflow.RegisterOptions{Name: "AgentWorkflow"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.DeliverInput) error {
		return errors.New("server refused the notification")
	}, sdkactivity.RegisterOptions{Name: "DeliverResult"})
	deleted := ""
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.DeleteScheduleInput) error {
		deleted = in.ScheduleID
		return nil
	}, sdkactivity.RegisterOptions{Name: "DeleteSchedule"})

	env.ExecuteWorkflow(ScheduledAgentWorkflow, tool.ScheduledAgentInput{
		AgentID: "default", Prompt: "remind me", UserID: "victor", ScheduleID: "schedule-test",
	})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if env.GetWorkflowError() == nil {
		t.Error("a failed delivery was reported as a success")
	}
	if deleted != "schedule-test" {
		t.Errorf("deleted schedule %q, want schedule-test", deleted)
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
