package workflow

import (
	"encoding/json"
	"testing"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/worker"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/provider"
)

// A sub-agent running when a worker is deployed may have started a child
// that delegates back up its chain, which the old code allowed: its history
// must replay with the child, and a new one with the refusal instead.
func TestAgentWorkflow_ReplaysADelegationLoopOfEitherVersion(t *testing.T) {
	in := AgentWorkflowInput{SessionID: "s1-tool-agent_analyst-c0", AgentID: "analyst", AgentChain: []string{"root"}, UserMessage: "go"}
	call := provider.ToolCallInfo{ID: "c1", Name: "agent_root", Input: json.RawMessage(`{"task":"ask root"}`)}

	for _, tc := range []struct {
		name string
		// dispatch adds what the task dispatching the call recorded.
		dispatch func(h *replayHistory, task int64)
	}{
		{"before the change", func(h *replayHistory, task int64) {
			h.childStarted(task, childWorkflowID(in.SessionID, call.Name, call.ID, 0, 0), "AgentWorkflow")
		}},
		{"after the change", func(h *replayHistory, task int64) {
			// Refused: the version, then the next LLM call with the error.
			h.versionMarker(task, delegationChangeID, 1)
			h.scheduled(task, "CallLLM")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := &replayHistory{t: t}
			task := h.startedAs("AgentWorkflow", in)
			h.sideEffect(task, 1, map[string]string{})
			task = h.activityReturning(task, "LoadSkillsForAgent", activity.LoadSkillsForAgentOutput{})
			task = h.activityReturning(task, "ListTools", activity.ListToolsOutput{
				Tools:       []provider.ToolDefinition{{Name: call.Name}},
				Resolutions: map[string]activity.ToolResolution{call.Name: {Kind: "workflow", WorkflowName: "AgentWorkflow", TaskQueue: "agent", AgentID: "root"}},
			})
			task = h.activityReturning(task, "CallLLM", provider.ChatResponse{ToolCalls: []provider.ToolCallInfo{call}, StopReason: "tool_use"})
			task = h.activity(task, "NotifyStep") // the tool calls, shown
			tc.dispatch(h, task)

			replayer := worker.NewWorkflowReplayer()
			replayer.RegisterWorkflow(AgentWorkflow)
			if err := replayer.ReplayWorkflowHistory(nil, &historypb.History{Events: h.events}); err != nil {
				t.Fatalf("replay: %v", err)
			}
		})
	}
}
