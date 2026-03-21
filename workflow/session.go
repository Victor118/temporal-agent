package workflow

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
)

const (
	SignalUserMessage  = "user-message"
	QuerySessionState  = "session-state"
)

type SessionWorkflowInput struct {
	SessionID    string `json:"session_id"`
	SystemPrompt string `json:"system_prompt"`
	Model        string `json:"model"`
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
			notifyResponse(ctx, input.SessionID, fmt.Sprintf("Error processing message: %v", err))
			continue
		}

		// Drain queued messages
		for msgCh.ReceiveAsync(&userMessage) {
			if err := processTurn(actCtx, ctx, input, userMessage, &state); err != nil {
				logger.Error("Turn failed", "error", err)
				notifyResponse(ctx, input.SessionID, fmt.Sprintf("Error processing message: %v", err))
			}
		}
	}
}

// processTurn handles a single user message: load context → agent → persist.
func processTurn(actCtx, ctx workflow.Context, input SessionWorkflowInput, userMessage string, state *SessionState) error {
	state.Status = "processing"
	state.TurnCount++

	var memAct *activity.MemoryActivities

	// 1. Load context
	var loadResult activity.LoadContextOutput
	if err := workflow.ExecuteActivity(actCtx, memAct.LoadContext, activity.LoadContextInput{
		SessionID: input.SessionID,
	}).Get(ctx, &loadResult); err != nil {
		return fmt.Errorf("load context: %w", err)
	}

	// 2. Launch agent child workflow with the loaded messages
	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: fmt.Sprintf("%s-turn-%d", input.SessionID, state.TurnCount),
	})

	var result AgentWorkflowOutput
	if err := workflow.ExecuteChildWorkflow(childCtx, AgentWorkflow, AgentWorkflowInput{
		SessionID:    input.SessionID,
		UserMessage:  userMessage,
		Messages:     loadResult.Messages,
		SystemPrompt: input.SystemPrompt, // Optional override; empty = load from task queue skills
		Model:        input.Model,
	}).Get(ctx, &result); err != nil {
		return fmt.Errorf("agent workflow: %w", err)
	}

	// 3. Persist the updated messages returned by the agent
	if err := workflow.ExecuteActivity(actCtx, memAct.PersistContext, activity.PersistContextInput{
		SessionID: input.SessionID,
		Messages:  result.Messages,
	}).Get(ctx, nil); err != nil {
		return fmt.Errorf("persist context: %w", err)
	}

	// 4. Check goal
	if result.GoalAchieved {
		state.Status = "completed"
	}

	return nil
}
