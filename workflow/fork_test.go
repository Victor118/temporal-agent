package workflow

import (
	"context"
	"strings"
	"testing"

	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/victor/temporal-agent/activity"
)

// forkEnv runs ForkSessionWorkflow against stub activities; summarize is what
// the summary activity does, post what posting it does (nil: it is posted).
func forkEnv(t *testing.T, summarize func(activity.SummarizeConversationInput) (activity.SummarizeConversationOutput, error),
	post func(activity.PostForkSummaryInput) (int64, error)) (
	*testsuite.TestWorkflowEnvironment, *[]activity.PostForkSummaryInput, *[]string) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	persisted := &[]activity.PostForkSummaryInput{}
	events := &[]string{}

	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.SummarizeConversationInput) (activity.SummarizeConversationOutput, error) {
		return summarize(in)
	}, sdkactivity.RegisterOptions{Name: "SummarizeConversation"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.PostForkSummaryInput) (int64, error) {
		*persisted = append(*persisted, in)
		if post != nil {
			return post(in)
		}
		return 1, nil
	}, sdkactivity.RegisterOptions{Name: "PostForkSummary"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.NotifyInput) error {
		if in.SessionID != "fork1" {
			t.Errorf("notified session %q, want the fork", in.SessionID)
		}
		*events = append(*events, in.Event.Type)
		return nil
	}, sdkactivity.RegisterOptions{Name: "NotifyStep"})
	return env, persisted, events
}

func TestForkSessionWorkflow_StoresTheSummary(t *testing.T) {
	env, persisted, events := forkEnv(t, func(in activity.SummarizeConversationInput) (activity.SummarizeConversationOutput, error) {
		if in.SessionID != "parent1" || in.UpToMessageID != 42 || in.Model != "small" || in.Purpose != "CSV export" {
			t.Errorf("summarized %+v", in)
		}
		return activity.SummarizeConversationOutput{Summary: "What happened.", Truncated: true}, nil
	}, nil)
	env.ExecuteWorkflow(ForkSessionWorkflow, ForkSessionInput{ForkSessionID: "fork1", ParentSessionID: "parent1", UpToMessageID: 42, Purpose: "CSV export", Model: "small"})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(*persisted) != 1 {
		t.Fatalf("persisted %d times", len(*persisted))
	}
	p := (*persisted)[0]
	if p.ForkSessionID != "fork1" || !strings.HasSuffix(p.Summary, "What happened.") || !strings.Contains(p.Summary, "beginning is not covered") {
		t.Errorf("posted %+v", p)
	}
	if strings.Join(*events, ",") != EventForkReady {
		t.Errorf("events %v", *events)
	}
}

func TestForkSessionWorkflow_ReportsFailure(t *testing.T) {
	env, persisted, events := forkEnv(t, func(activity.SummarizeConversationInput) (activity.SummarizeConversationOutput, error) {
		return activity.SummarizeConversationOutput{}, temporal.NewNonRetryableApplicationError("gone", "MessageNotFound", nil)
	}, nil)
	env.ExecuteWorkflow(ForkSessionWorkflow, ForkSessionInput{ForkSessionID: "fork1", ParentSessionID: "parent1", UpToMessageID: 42})

	if env.GetWorkflowError() == nil {
		t.Error("a failed summary must fail the workflow")
	}
	if len(*persisted) != 0 || strings.Join(*events, ",") != EventForkFailed {
		t.Errorf("persisted %v, events %v", *persisted, *events)
	}
}

// A fork deleted while its summary was written: the post is not tried again,
// whatever the activity says of it (the workflow's policy suffices).
func TestForkSessionWorkflow_ForkGoneIsFinal(t *testing.T) {
	env, posted, events := forkEnv(t, func(activity.SummarizeConversationInput) (activity.SummarizeConversationOutput, error) {
		return activity.SummarizeConversationOutput{Summary: "brief"}, nil
	}, func(activity.PostForkSummaryInput) (int64, error) {
		return 0, temporal.NewApplicationError("the fork was deleted", activity.ErrTypeForkGone)
	})
	env.ExecuteWorkflow(ForkSessionWorkflow, ForkSessionInput{ForkSessionID: "fork1", ParentSessionID: "parent1", UpToMessageID: 42})

	if env.GetWorkflowError() == nil {
		t.Error("a summary for a deleted fork must fail the workflow")
	}
	if len(*posted) != 1 || strings.Join(*events, ",") != EventForkFailed {
		t.Errorf("%d attempts, events %v", len(*posted), *events)
	}
}
