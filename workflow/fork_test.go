package workflow

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/store"
)

// forkEnv runs ForkSessionWorkflow against stub activities; summarize is what
// the summary activity does.
func forkEnv(t *testing.T, summarize func(activity.SummarizeConversationInput) (activity.SummarizeConversationOutput, error)) (
	*testsuite.TestWorkflowEnvironment, *[]activity.PersistContextInput, *[]string) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	persisted := &[]activity.PersistContextInput{}
	events := &[]string{}

	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.SummarizeConversationInput) (activity.SummarizeConversationOutput, error) {
		return summarize(in)
	}, sdkactivity.RegisterOptions{Name: "SummarizeConversation"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.PersistContextInput) error {
		*persisted = append(*persisted, in)
		return nil
	}, sdkactivity.RegisterOptions{Name: "PersistContext"})
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
		if in.SessionID != "parent1" || in.UpToMessageID != 42 || in.Model != "small" {
			t.Errorf("summarized %+v", in)
		}
		return activity.SummarizeConversationOutput{Summary: "What happened.", Truncated: true}, nil
	})
	env.ExecuteWorkflow(ForkSessionWorkflow, ForkSessionInput{ForkSessionID: "fork1", ParentSessionID: "parent1", UpToMessageID: 42, Model: "small"})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(*persisted) != 1 {
		t.Fatalf("persisted %d times", len(*persisted))
	}
	p := (*persisted)[0]
	var content string
	json.Unmarshal([]byte(p.Messages[0].Content), &content)
	if p.SessionID != "fork1" || p.Messages[0].Kind != store.KindForkSummary || p.Messages[0].Role != store.RoleUser ||
		!strings.HasSuffix(content, "What happened.") || !strings.Contains(content, "beginning is not covered") {
		t.Errorf("persisted %+v (%q)", p, content)
	}
	if strings.Join(*events, ",") != EventForkReady {
		t.Errorf("events %v", *events)
	}
}

func TestForkSessionWorkflow_ReportsFailure(t *testing.T) {
	env, persisted, events := forkEnv(t, func(activity.SummarizeConversationInput) (activity.SummarizeConversationOutput, error) {
		return activity.SummarizeConversationOutput{}, temporal.NewNonRetryableApplicationError("gone", "MessageNotFound", nil)
	})
	env.ExecuteWorkflow(ForkSessionWorkflow, ForkSessionInput{ForkSessionID: "fork1", ParentSessionID: "parent1", UpToMessageID: 42})

	if env.GetWorkflowError() == nil {
		t.Error("a failed summary must fail the workflow")
	}
	if len(*persisted) != 0 || strings.Join(*events, ",") != EventForkFailed {
		t.Errorf("persisted %v, events %v", *persisted, *events)
	}
}

// The summary reaches the model as context carried over, not as something the
// user said.
func TestConvertMessages_FramesForkSummary(t *testing.T) {
	msgs := convertMessages([]store.Message{{Role: store.RoleUser, Kind: store.KindForkSummary, Content: `"what happened"`}}, historyView{self: "default"})
	var got string
	json.Unmarshal(msgs[0].Content, &got)
	if !strings.HasPrefix(got, "[Context carried over from an earlier conversation") || !strings.HasSuffix(got, "what happened") {
		t.Errorf("model sees %q", got)
	}
}
