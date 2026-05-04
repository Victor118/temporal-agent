package workflow

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
)

const (
	SignalUserMessage   = "user-message"
	SignalCancelAgent   = "cancel-agent"
	QuerySessionState   = "session-state"
)

type SessionWorkflowInput struct {
	SessionID    string `json:"session_id"`
	UserID       string `json:"user_id"`
	SystemPrompt string `json:"system_prompt"`
	Model        string `json:"model"`
	Channel      string `json:"channel,omitempty"`    // "web", "telegram"
	ChannelID    string `json:"channel_id,omitempty"` // chat_id for telegram
}

type SessionState struct {
	SessionID string `json:"session_id"`
	Status    string `json:"status"` // "idle", "processing", "completed"
	TurnCount int    `json:"turn_count"`
}

// SessionWorkflow is the long-lived orchestration workflow for a conversation.
// It owns the context lifecycle: load, pass to agent, persist after.
func SessionWorkflow(ctx workflow.Context, input SessionWorkflowInput) error {
	logger := workflow.GetLogger(ctx)

	activityOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	}
	actCtx := workflow.WithActivityOptions(ctx, activityOpts)

	state := SessionState{
		SessionID: input.SessionID,
		Status:    "idle",
	}

	// Register query handler
	if err := workflow.SetQueryHandler(ctx, QuerySessionState, func() (SessionState, error) {
		return state, nil
	}); err != nil {
		return fmt.Errorf("set query handler: %w", err)
	}

	msgCh := workflow.GetSignalChannel(ctx, SignalUserMessage)
	idleTimeout := 30 * time.Minute

	for {
		state.Status = "idle"

		// Wait for a message or timeout
		var userMessage string
		ok, _ := msgCh.ReceiveWithTimeout(ctx, idleTimeout, &userMessage)
		if !ok {
			logger.Info("Session timed out", "session_id", input.SessionID)
			state.Status = "completed"
			return nil
		}

		if err := processTurn(actCtx, ctx, input, userMessage, &state); err != nil {
			logger.Error("Turn failed", "session_id", input.SessionID, "turn", state.TurnCount, "error", err)
			// Notify the error to the client, don't kill the session
			notifyResponse(ctx, input.SessionID, input.Channel, input.ChannelID, fmt.Sprintf("Error processing message: %v", err))
			continue
		}

		// Drain queued messages
		for msgCh.ReceiveAsync(&userMessage) {
			if err := processTurn(actCtx, ctx, input, userMessage, &state); err != nil {
				logger.Error("Turn failed", "error", err)
				notifyResponse(ctx, input.SessionID, input.Channel, input.ChannelID, fmt.Sprintf("Error processing message: %v", err))
			}
		}
	}
}

// processTurn handles a single user message: load context → agent → persist.
// It listens for cancel-agent signals to interrupt the agent mid-execution.
func processTurn(actCtx, ctx workflow.Context, input SessionWorkflowInput, userMessage string, state *SessionState) error {
	state.Status = "processing"
	state.TurnCount++

	var memAct *activity.MemoryActivities

	// 1. Load context (messages + user memory)
	var loadResult activity.LoadContextOutput
	if err := workflow.ExecuteActivity(actCtx, memAct.LoadContext, activity.LoadContextInput{
		SessionID: input.SessionID,
		UserID:    input.UserID,
	}).Get(ctx, &loadResult); err != nil {
		return fmt.Errorf("load context: %w", err)
	}

	// 2. Launch agent child workflow with a cancellable context
	childCtx, cancelChild := workflow.WithCancel(ctx)
	childCtx = workflow.WithChildOptions(childCtx, workflow.ChildWorkflowOptions{
		WorkflowID: fmt.Sprintf("%s-turn-%d", input.SessionID, state.TurnCount),
	})

	agentFuture := workflow.ExecuteChildWorkflow(childCtx, AgentWorkflow, AgentWorkflowInput{
		SessionID:    input.SessionID,
		UserID:       input.UserID,
		UserMessage:  userMessage,
		Messages:     loadResult.Messages,
		UserMemory:   loadResult.UserMemory,
		SystemPrompt: input.SystemPrompt,
		Model:        input.Model,
		Channel:      input.Channel,
		ChannelID:    input.ChannelID,
	})

	// Listen for cancel signal in parallel
	cancelCh := workflow.GetSignalChannel(ctx, SignalCancelAgent)
	cancelSel := workflow.NewSelector(ctx)

	var result AgentWorkflowOutput
	var agentErr error
	cancelled := false

	cancelSel.AddFuture(agentFuture, func(f workflow.Future) {
		agentErr = f.Get(ctx, &result)
	})

	cancelSel.AddReceive(cancelCh, func(ch workflow.ReceiveChannel, more bool) {
		// Drain the signal value
		ch.Receive(ctx, nil)
		cancelled = true
		cancelChild()
	})

	// Wait for either the agent to finish or a cancel signal
	cancelSel.Select(ctx)

	if cancelled {
		// Wait for the child to actually finish after cancellation
		_ = agentFuture.Get(ctx, &result)
		// Notify the user
		notifyResponse(ctx, input.SessionID, input.Channel, input.ChannelID, "Agent interrupted by user.")
	} else if agentErr != nil {
		return fmt.Errorf("agent workflow: %w", agentErr)
	}

	// 3. Always persist — even if cancelled, save the messages accumulated so far
	if len(result.Messages) > 0 {
		if err := workflow.ExecuteActivity(actCtx, memAct.PersistContext, activity.PersistContextInput{
			SessionID: input.SessionID,
			Messages:  result.Messages,
		}).Get(ctx, nil); err != nil {
			return fmt.Errorf("persist context: %w", err)
		}
	}

	// 4. Check goal
	if !cancelled && result.GoalAchieved {
		state.Status = "completed"
	}

	return nil
}
