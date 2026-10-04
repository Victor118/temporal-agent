package workflow

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/tool"
)

// The question reaches the user's channel signed by the agent that asks, when
// its answer would be, and the answer comes back to it.
func TestAskUserWorkflow_SignsTheQuestion(t *testing.T) {
	for _, signer := range []string{"", "Agent Smith"} {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		var sent activity.NotifyInput
		env.RegisterActivityWithOptions(func(ctx context.Context, in activity.NotifyInput) error {
			sent = in
			return nil
		}, sdkactivity.RegisterOptions{Name: "NotifyStep"})
		env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "s1:p:jarvis:m3:tool:ask_user:t1"})
		env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalUserAnswer, "main") }, time.Second)

		input, _ := tool.WithCallContext(json.RawMessage(`{"question":"Quelle branche ?"}`),
			tool.CallContext{Channel: "telegram", ChannelID: "42", Agent: signer})
		env.ExecuteWorkflow(AskUserWorkflow, input)

		var out tool.Result
		if err := env.GetWorkflowResult(&out); err != nil || out.Content != "main" {
			t.Fatalf("signer %q: result %+v, %v", signer, out, err)
		}
		var event struct {
			Question string `json:"question"`
			Agent    string `json:"agent"`
		}
		json.Unmarshal(sent.Event.Data, &event)
		if sent.Channel != "telegram" || event.Question != "Quelle branche ?" || event.Agent != signer {
			t.Errorf("signer %q: sent %+v %s", signer, sent, sent.Event.Data)
		}
	}
}
