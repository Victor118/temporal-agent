package workflow

import (
	"context"
	"errors"
	"strings"
	"testing"

	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/store"
)

// reportRun is what ReportToParentWorkflow did against stub activities.
type reportRun struct {
	summarized []activity.SummarizeForkReportInput
	posts      int // attempts at posting
	parent     parentReports
	events     []string // "<session>:<event>"
}

// parentReports are the reports in the parent, by key: one per fork and
// range, whatever the attempts, as in the store.
type parentReports map[string]activity.PostForkReportInput

func (p parentReports) store(in activity.PostForkReportInput) {
	p[store.ForkReportKey(in.ForkSessionID, in.From, in.UpTo)] = in
}

// reportEnv runs ReportToParentWorkflow; summarize is what the summary
// activity does, post what the post activity does to the parent.
func reportEnv(t *testing.T, summarize func(activity.SummarizeForkReportInput) (activity.SummarizeConversationOutput, error),
	post func(attempt int, in activity.PostForkReportInput, parent parentReports) error) (*testsuite.TestWorkflowEnvironment, *reportRun) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	run := &reportRun{parent: parentReports{}}

	env.RegisterActivityWithOptions(func(_ context.Context, in activity.SummarizeForkReportInput) (activity.SummarizeConversationOutput, error) {
		run.summarized = append(run.summarized, in)
		return summarize(in)
	}, sdkactivity.RegisterOptions{Name: "SummarizeForkReport"})
	env.RegisterActivityWithOptions(func(_ context.Context, in activity.PostForkReportInput) (int64, error) {
		run.posts++
		if err := post(run.posts, in, run.parent); err != nil {
			return 0, err
		}
		return 100 + in.UpTo, nil
	}, sdkactivity.RegisterOptions{Name: "PostForkReport"})
	env.RegisterActivityWithOptions(func(_ context.Context, in activity.NotifyInput) error {
		run.events = append(run.events, in.SessionID+":"+in.Event.Type)
		return nil
	}, sdkactivity.RegisterOptions{Name: "NotifyStep"})
	return env, run
}

func summary(text string) func(activity.SummarizeForkReportInput) (activity.SummarizeConversationOutput, error) {
	return func(activity.SummarizeForkReportInput) (activity.SummarizeConversationOutput, error) {
		return activity.SummarizeConversationOutput{Summary: text}, nil
	}
}

func posted(_ int, in activity.PostForkReportInput, parent parentReports) error {
	parent.store(in)
	return nil
}

var reportIn = ReportToParentInput{ForkSessionID: "fork1", ParentSessionID: "parent1", ForkTitle: "Export CSV", Purpose: "CSV export",
	UpTo: 40, ReporterID: "u-victor", ReporterName: "Victor", Model: "small"}

// The first report covers the fork from its start; it is posted as the
// member's, then both sessions hear of it.
func TestReportToParentWorkflow_First(t *testing.T) {
	env, run := reportEnv(t, summary("## Fait"), posted)
	env.ExecuteWorkflow(ReportToParentWorkflow, reportIn)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	want := activity.SummarizeForkReportInput{SessionID: "fork1", AfterMessageID: 0, UpToMessageID: 40, Purpose: "CSV export", Model: "small"}
	if len(run.summarized) != 1 || run.summarized[0] != want {
		t.Errorf("summarized %+v", run.summarized)
	}
	p, ok := run.parent[store.ForkReportKey("fork1", 0, 40)]
	if !ok || len(run.parent) != 1 || p.ParentSessionID != "parent1" || p.ReporterID != "u-victor" || p.ReporterName != "Victor" ||
		p.ForkTitle != "Export CSV" || p.Report != "## Fait" {
		t.Errorf("parent's reports %+v", run.parent)
	}
	if got := strings.Join(run.events, ","); got != "parent1:"+EventForkReport+",fork1:"+EventForkReported {
		t.Errorf("events %s", got)
	}
}

// A second report starts where the first stopped.
func TestReportToParentWorkflow_SecondCoversOnlyWhatIsNew(t *testing.T) {
	env, run := reportEnv(t, summary("since"), posted)
	in := reportIn
	in.From, in.UpTo = 40, 55
	env.ExecuteWorkflow(ReportToParentWorkflow, in)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if run.summarized[0].AfterMessageID != 40 || run.summarized[0].UpToMessageID != 55 {
		t.Errorf("summarized %+v", run.summarized[0])
	}
	if _, ok := run.parent[store.ForkReportKey("fork1", 40, 55)]; !ok || len(run.parent) != 1 {
		t.Errorf("parent's reports %+v", run.parent)
	}
}

// A post whose answer was lost is retried: the report is in the parent once,
// and announced once.
func TestReportToParentWorkflow_RetriedPostPostsOnce(t *testing.T) {
	env, run := reportEnv(t, summary("## Fait"), func(attempt int, in activity.PostForkReportInput, parent parentReports) error {
		parent.store(in)
		if attempt == 1 {
			return errors.New("connection reset") // written, then the answer was lost
		}
		return nil
	})
	env.ExecuteWorkflow(ReportToParentWorkflow, reportIn)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if run.posts != 2 || len(run.parent) != 1 {
		t.Errorf("%d attempts, parent's reports %+v", run.posts, run.parent)
	}
	if got := strings.Join(run.events, ","); strings.Count(got, EventForkReport+",") != 1 {
		t.Errorf("events %s", got)
	}
}

// A summary that fails is shown in the fork, and nothing reaches the parent.
func TestReportToParentWorkflow_SummaryFails(t *testing.T) {
	env, run := reportEnv(t, func(activity.SummarizeForkReportInput) (activity.SummarizeConversationOutput, error) {
		return activity.SummarizeConversationOutput{}, temporal.NewNonRetryableApplicationError("bad request", "PermanentAPIError", nil)
	}, posted)
	env.ExecuteWorkflow(ReportToParentWorkflow, reportIn)
	if env.GetWorkflowError() == nil {
		t.Error("a failed report must fail the workflow")
	}
	if run.posts != 0 || strings.Join(run.events, ",") != "fork1:"+EventForkReportFailed {
		t.Errorf("posts %d, events %v", run.posts, run.events)
	}
}

// The fork or the parent was deleted while the report was written, or the
// reporter left one of them: the post is refused once and for all, and the
// fork says so.
func TestReportToParentWorkflow_RefusedMeanwhile(t *testing.T) {
	for refusal, errType := range map[error]string{
		store.ErrForkGone:              activity.ErrTypeForkGone,
		store.ErrReportParentGone:      activity.ErrTypeReportParentGone,
		store.ErrReportNotForkMember:   activity.ErrTypeReportNotForkMember,
		store.ErrReportNotParentMember: activity.ErrTypeReportNotParentMember,
	} {
		t.Run(errType, func(t *testing.T) {
			env, run := reportEnv(t, summary("## Fait"), func(int, activity.PostForkReportInput, parentReports) error {
				// Retryable as returned: the workflow's policy alone must
				// stop at the first attempt.
				return temporal.NewApplicationError(refusal.Error(), errType)
			})
			env.ExecuteWorkflow(ReportToParentWorkflow, reportIn)
			if err := env.GetWorkflowError(); err == nil || !strings.Contains(err.Error(), refusal.Error()) {
				t.Errorf("workflow error %v, want %q", err, refusal)
			}
			if run.posts != 1 || len(run.parent) != 0 || strings.Join(run.events, ",") != "fork1:"+EventForkReportFailed {
				t.Errorf("%d attempts, parent's reports %v, events %v", run.posts, run.parent, run.events)
			}
		})
	}
}

func TestReportWorkflowID(t *testing.T) {
	if got := ReportWorkflowID("f1", 0); got != "report-f1-0" {
		t.Errorf("first report %q", got)
	}
	if ReportWorkflowID("f1", 40) == ReportWorkflowID("f1", 0) {
		t.Error("a report after another must have its own ID")
	}
}
