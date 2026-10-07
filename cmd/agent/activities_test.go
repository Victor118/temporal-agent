package main

import (
	"fmt"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"

	"go.temporal.io/sdk/testsuite"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/tool"
)

// testActivities is the list newWorkerRuntime registers, built by the same
// function, from what a test can build.
func testActivities() []any {
	catalog := activity.NewCatalog()
	return workerActivities(activityDeps{
		catalog:   catalog,
		skills:    activity.NewSkillActivities(nil, catalog),
		code:      &activity.ClaudeCodeActivities{Stopper: &activity.RunStop{}},
		registry:  tool.NewRegistry(),
		notifiers: map[string]activity.Notifier{},
	})
}

// register registers acts as a worker does, and returns what the SDK
// refused: RegisterActivity runs the validation of worker.New's registry,
// and panics on an exported method that is no valid activity.
func register(acts ...any) (refused error) {
	defer func() {
		if r := recover(); r != nil {
			refused = fmt.Errorf("%v", r)
		}
	}()
	env := (&testsuite.WorkflowTestSuite{}).NewTestActivityEnvironment()
	for _, act := range acts {
		env.RegisterActivity(act)
	}
	return nil
}

// Every activity struct a worker registers passes the SDK's validation: one
// that does not panics at the startup of every worker, on every queue.
func TestWorkerActivities_Register(t *testing.T) {
	if err := register(testActivities()...); err != nil {
		t.Fatalf("a worker would not start: %v", err)
	}
}

// Every workflow a worker registers passes the SDK's validation, under the
// name the server and the relay start it by.
func TestWorkerWorkflows_Register(t *testing.T) {
	var names []string
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("a worker would not start: %v", r)
			}
		}()
		env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
		for _, wf := range workerWorkflows() {
			env.RegisterWorkflow(wf)
			name := runtime.FuncForPC(reflect.ValueOf(wf).Pointer()).Name()
			names = append(names, name[strings.LastIndexByte(name, '.')+1:])
		}
	}()
	for _, want := range []string{"ParticipantWorkflow", "AgentWorkflow", "AskUserWorkflow", "ForkSessionWorkflow", "ReportToParentWorkflow", "BackgroundTaskWorkflow"} {
		if !slices.Contains(names, want) {
			t.Errorf("workflows %v lack %s", names, want)
		}
	}
}

// The test above would catch it: a method with no result is refused.
type noResult struct{}

func (*noResult) Stop() {}

func TestRegister_RefusesAMethodWithNoResult(t *testing.T) {
	if err := register(&noResult{}); err == nil {
		t.Fatal("registered a method that returns nothing")
	}
}

// Every exported method of the registered structs is an activity, on every
// queue: one that is not meant to be (a helper the worker calls at startup)
// must not be an exported method there. A new activity goes in this list.
func TestWorkerActivities_AreTheActivities(t *testing.T) {
	want := []string{
		"CallLLM",
		"CallLLMOnMachine",
		"CheckTurn",
		"ChooseMachine",
		"CleanupWorkspace",
		"CodingRoute",
		"DeleteSchedule",
		"DeliverResult",
		"DropTask",
		"EndTurn",
		"ExecuteTool",
		"InspectWorkspace",
		"ListTools",
		"LoadSkillsForAgent",
		"NotifyStep",
		"PersistContext",
		"PickMachine",
		"PostForkReport",
		"PostForkSummary",
		"PostTaskResult",
		"PrepareWorkspace",
		"ProbeRunWorker",
		"PublishOutputs",
		"PushBranch",
		"RegisterTask",
		"Relay",
		"RunClaudeCode",
		"RunOnMachine",
		"SetMachineAside",
		"SummarizeConversation",
		"SummarizeForkReport",
	}
	var got []string
	for _, act := range testActivities() {
		typ := reflect.TypeOf(act)
		for i := range typ.NumMethod() {
			got = append(got, typ.Method(i).Name)
		}
	}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Errorf("activities = %v\nwant %v", got, want)
	}
}
