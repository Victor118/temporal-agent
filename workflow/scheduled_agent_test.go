package workflow

import (
	"context"
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
