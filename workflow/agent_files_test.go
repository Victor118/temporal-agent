package workflow

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/tool"
)

// A file is attached to a session turn: a turn's call names its own, a
// sub-agent's the turn that launched it, and a run outside a session none.
func TestCallContext_Turn(t *testing.T) {
	turn := AgentWorkflowInput{SessionID: "s1", TurnKey: "m7.jarvis", AgentID: "jarvis"}
	cc := callContext(turn, []string{"jarvis"}, "", "agent")
	if cc.Turn == nil || *cc.Turn != (tool.TurnRef{SessionID: "s1", TurnKey: "m7.jarvis"}) {
		t.Fatalf("a turn's call: %+v", cc.Turn)
	}

	res := activity.ToolResolution{Kind: "workflow", AgentID: "analyst"}
	_, in, err := buildChildInput(json.RawMessage(`{"task":"t"}`), turn, "s1:p:jarvis:m7:tool:agent_analyst:1-0", &res, cc, "jarvis", "agent")
	if err != nil {
		t.Fatal(err)
	}
	sub := in.(AgentWorkflowInput)
	subCC := callContext(sub, []string{"jarvis", "analyst"}, "", "agent")
	if subCC.Turn == nil || *subCC.Turn != *cc.Turn {
		t.Errorf("a sub-agent's call: %+v, want its parent's turn", subCC.Turn)
	}
	// And its own sub-agent's, one level down.
	_, in, _ = buildChildInput(json.RawMessage(`{"task":"t"}`), sub, "child", &activity.ToolResolution{Kind: "workflow", AgentID: "writer"}, subCC, "analyst", "agent")
	if deeper := callContext(in.(AgentWorkflowInput), nil, "", "agent"); deeper.Turn == nil || *deeper.Turn != *cc.Turn {
		t.Errorf("a sub-sub-agent's call: %+v", deeper.Turn)
	}

	scheduled := AgentWorkflowInput{SessionID: "sched-1", AgentID: "jarvis", UserMessage: "report", LoadUserMemory: true}
	if cc := callContext(scheduled, nil, "", "agent"); cc.Turn != nil {
		t.Errorf("a scheduled run's call: %+v, want none", cc.Turn)
	}
}

// A step that published files tells the session's members, once written:
// the event names the turn and the files, never their content.
func TestAgentWorkflow_NotifiesPublishedFiles(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.ListToolsInput) (activity.ListToolsOutput, error) {
		return activity.ListToolsOutput{
			Tools: []provider.ToolDefinition{{Name: "publish_file", InputSchema: json.RawMessage(`{"type":"object"}`)}},
			Resolutions: map[string]activity.ToolResolution{
				"publish_file": {Kind: "activity", TaskQueue: "tools", NeedsCallContext: true},
			},
		}, nil
	}, sdkactivity.RegisterOptions{Name: "ListTools"})
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.LoadSkillsForAgentInput) (activity.LoadSkillsForAgentOutput, error) {
		return activity.LoadSkillsForAgentOutput{SystemPrompt: "prompt"}, nil
	}, sdkactivity.RegisterOptions{Name: "LoadSkillsForAgent"})

	var mu sync.Mutex
	var events []activity.NotifyInput
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.NotifyInput) error {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, in)
		return nil
	}, sdkactivity.RegisterOptions{Name: "NotifyStep"})

	var calls []activity.ExecuteToolInput
	env.RegisterActivityWithOptions(func(ctx context.Context, in activity.ExecuteToolInput) (activity.ExecuteToolOutput, error) {
		calls = append(calls, in)
		return activity.ExecuteToolOutput{
			Content: "Published rapport.md (10 B, id f1).",
			Files:   []tool.FileRef{{ID: "f1", Name: "rapport.md", Size: 10}},
		}, nil
	}, sdkactivity.RegisterOptions{Name: "ExecuteTool"})

	f := registerLLM(env, answers(provider.ChatResponse{ToolCalls: []provider.ToolCallInfo{
		{ID: "1", Name: "publish_file", Input: json.RawMessage(`{"name":"rapport.md","content":"# Rapport\n"}`)},
	}}, done))
	turn := store.TurnKey(f.session.add(store.HumanMessageKey("q"), store.Message{Role: store.RoleUser, Content: `"a report"`}), "jarvis")

	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{SessionID: "s1", AgentID: "jarvis", TurnKey: turn})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}

	// The call's own ID: a file it publishes twice under one name is one.
	if len(calls) != 1 || calls[0].Call == nil || calls[0].Call.Turn == nil || *calls[0].Call.Turn != (tool.TurnRef{SessionID: "s1", TurnKey: turn}) || calls[0].Call.CallID != "1" {
		t.Fatalf("publish_file called with %+v", calls)
	}
	var published []activity.NotifyInput
	for _, e := range events {
		if e.Event.Type == activity.EventFilePublished {
			published = append(published, e)
		}
	}
	if len(published) != 1 {
		t.Fatalf("file events %+v", published)
	}
	var data struct {
		Turn    string   `json:"turn"`
		AgentID string   `json:"agent_id"`
		Files   []string `json:"files"`
	}
	json.Unmarshal(published[0].Event.Data, &data)
	if published[0].SessionID != "s1" || published[0].Channel != "" || data.Turn != turn || data.AgentID != "jarvis" || len(data.Files) != 1 || data.Files[0] != "f1" {
		t.Errorf("file event %+v: %s", published[0], published[0].Event.Data)
	}
}
