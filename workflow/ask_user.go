package workflow

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
)

const SignalUserAnswer = "user-answer"

// AskUserWorkflow is a child workflow tool that sends a question to the user
// via SSE and blocks until the user answers (via signal) or a timeout expires.
// It returns the user's answer as a plain string to the calling agent.
func AskUserWorkflow(ctx workflow.Context, rawInput json.RawMessage) (string, error) {
	var input struct {
		Question   string   `json:"question"`
		AgentChain []string `json:"agent_chain,omitempty"`
		Channel    string   `json:"channel,omitempty"`
		ChannelID  string   `json:"channel_id,omitempty"`
	}
	if err := json.Unmarshal(rawInput, &input); err != nil {
		return "", fmt.Errorf("parse input: %w", err)
	}

	// Extract session ID from workflow ID convention: "{sessionID}-tool-ask_user-{N}"
	info := workflow.GetInfo(ctx)
	wfID := info.WorkflowExecution.ID
	idx := strings.Index(wfID, "-tool-")
	if idx == -1 {
		return "", fmt.Errorf("cannot extract session ID from workflow ID: %s", wfID)
	}
	sessionID := wfID[:idx]

	// Notify the client via SSE so it can display the question with agent context
	payload := map[string]interface{}{
		"type":        "ask_user",
		"question":    input.Question,
		"workflow_id": wfID,
	}
	if len(input.AgentChain) > 0 {
		payload["agent_chain"] = input.AgentChain
	}
	data, _ := json.Marshal(payload)
	var notifAct *activity.NotificationActivities
	if err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 5 * time.Second,
		}),
		notifAct.NotifyStep,
		activity.NotifyInput{
			SessionID: sessionID,
			Channel:   input.Channel,
			ChannelID: input.ChannelID,
			Event: activity.SSEEvent{
				Type: "ask_user",
				Data: data,
			},
		},
	).Get(ctx, nil); err != nil {
		return "", fmt.Errorf("notify user: %w", err)
	}

	// Wait for the user's answer or timeout
	answerCh := workflow.GetSignalChannel(ctx, SignalUserAnswer)
	timer := workflow.NewTimer(ctx, 10*time.Minute)

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
		return "", fmt.Errorf("user did not answer within 10 minutes")
	}
	return answer, nil
}
