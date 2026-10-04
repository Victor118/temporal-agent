package workflow

import (
	"encoding/json"
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/tool"
)

const SignalUserAnswer = "user-answer"

// QueryQuestion returns the PendingQuestion an AskUserWorkflow waits on.
const QueryQuestion = "question"

// PendingQuestion is a question waiting for an answer.
type PendingQuestion struct {
	Question   string   `json:"question"`
	AgentChain []string `json:"agent_chain,omitempty"`
}

// askUserTimeout bounds how long a question waits for its answer. Approvals and
// clarifications are measured in hours, not seconds: the workflow consumes
// nothing while it waits, so the bound only exists to release a question the
// user will never answer.
const askUserTimeout = 72 * time.Hour

// AskUserWorkflow is a child workflow tool that sends a question to the user
// via SSE and blocks until the user answers (via signal) or a timeout expires.
// It returns the user's answer to the calling agent. The agent chain and the
// channel come from the caller (tool.CallContext), the question from the model.
func AskUserWorkflow(ctx workflow.Context, rawInput json.RawMessage) (tool.Result, error) {
	var input struct {
		Question string `json:"question"`
		tool.CallContext
	}
	if err := json.Unmarshal(rawInput, &input); err != nil {
		return tool.Result{}, fmt.Errorf("parse input: %w", err)
	}

	// The session, from the workflow ID: "<turn>:tool:ask_user:<call>"
	wfID := workflow.GetInfo(ctx).WorkflowExecution.ID
	sessionID, ok := SessionOf(wfID)
	if !ok {
		return tool.Result{}, fmt.Errorf("cannot extract session ID from workflow ID: %s", wfID)
	}

	// A page rendered while the question waits reads it from here: the SSE
	// event below reaches only the members watching at that moment.
	if err := workflow.SetQueryHandler(ctx, QueryQuestion, func() (PendingQuestion, error) {
		return PendingQuestion{Question: input.Question, AgentChain: input.AgentChain}, nil
	}); err != nil {
		return tool.Result{}, fmt.Errorf("set query handler: %w", err)
	}

	// Notify the client via SSE so it can display the question with agent context
	payload := map[string]interface{}{
		"type":        activity.EventAskUser,
		"question":    input.Question,
		"workflow_id": wfID,
	}
	if len(input.AgentChain) > 0 {
		payload["agent_chain"] = input.AgentChain
	}
	if input.Agent != "" {
		payload["agent"] = input.Agent
	}
	data, _ := json.Marshal(payload)
	var notifAct *activity.NotificationActivities
	if err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: channelNotifyTimeout,
			RetryPolicy:         notifyRetry,
		}),
		notifAct.NotifyStep,
		activity.NotifyInput{
			SessionID: sessionID,
			Channel:   input.Channel,
			ChannelID: input.ChannelID,
			Event: activity.SSEEvent{
				Type: activity.EventAskUser,
				Data: data,
			},
		},
	).Get(ctx, nil); err != nil {
		return tool.Result{}, fmt.Errorf("notify user: %w", err)
	}

	// Wait for the user's answer or timeout
	answerCh := workflow.GetSignalChannel(ctx, SignalUserAnswer)
	timer := workflow.NewTimer(ctx, askUserTimeout)

	var answer string
	sel := workflow.NewSelector(ctx)

	var timedOut bool
	sel.AddReceive(answerCh, func(ch workflow.ReceiveChannel, _ bool) {
		ch.Receive(ctx, &answer)
	})
	sel.AddFuture(timer, func(workflow.Future) {
		timedOut = true
	})
	sel.Select(ctx)

	if timedOut {
		return tool.Result{}, fmt.Errorf("user did not answer within %s", askUserTimeout)
	}
	return tool.Result{Content: answer}, nil
}
