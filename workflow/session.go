package workflow

import (
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
)

const (
	// Thresholds for continuing the session workflow as a new run.
	maxSessionHistoryEvents = 4000
	maxSessionHistoryBytes  = 4 * 1024 * 1024

	SignalUserMessage = "user-message"
	SignalCancelAgent = "cancel-agent"
	QuerySessionState = "session-state"
)

type SessionWorkflowInput struct {
	SessionID    string `json:"session_id"`
	UserID       string `json:"user_id"`
	AgentID      string `json:"agent_id,omitempty"` // Logical agent identity. Resolved by handlers when starting a session.
	SystemPrompt string `json:"system_prompt"`
	Model        string `json:"model"`                // Explicit model; empty = the worker's default (LLM_MODEL)
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

		// A long burst of turns grows this workflow's history without bound.
		// Start a fresh run: the conversation lives in the store, so the new run
		// reloads it and nothing is lost. Only do it with the channel drained —
		// a signal still queued would be dropped with the old run.
		if msgCh.Len() == 0 && sessionHistoryIsLarge(ctx) {
			logger.Info("Continuing session as new", "session_id", input.SessionID, "turns", state.TurnCount)
			return workflow.NewContinueAsNewError(ctx, SessionWorkflow, input)
		}
	}
}

// sessionHistoryIsLarge reports whether the workflow history is big enough to
// warrant a fresh run. The thresholds sit well under Temporal's hard limits
// (51200 events, 50MB), since a turn can add a lot at once.
func sessionHistoryIsLarge(ctx workflow.Context) bool {
	info := workflow.GetInfo(ctx)
	return info.GetCurrentHistoryLength() >= maxSessionHistoryEvents ||
		info.GetCurrentHistorySize() >= maxSessionHistoryBytes
}

// processTurn handles a single user message: run the agent, then persist what
// the turn produced. The agent loads the conversation itself. processTurn
// listens for cancel-agent signals to interrupt the agent mid-execution.
func processTurn(actCtx, ctx workflow.Context, input SessionWorkflowInput, userMessage string, state *SessionState) error {
	// Backstop for every channel that can signal a session: an empty user
	// message is rejected by the LLM API, and once persisted it breaks every
	// later turn of this session.
	if strings.TrimSpace(userMessage) == "" {
		workflow.GetLogger(ctx).Warn("Ignoring empty user message", "session_id", input.SessionID)
		return nil
	}

	state.Status = "processing"
	state.TurnCount++

	var memAct *activity.MemoryActivities

	// turnKey names this turn globally: the run ID keeps it distinct from the
	// same turn number in an earlier workflow run for this session, which a
	// resumed session would otherwise reuse.
	turnKey := fmt.Sprintf("%s-%d", workflow.GetInfo(ctx).WorkflowExecution.RunID, state.TurnCount)

	// 1. Launch agent child workflow with a cancellable context. It loads the
	// conversation itself: passing it here put a full copy of the history in
	// this workflow's event history on every turn, and this workflow is
	// long-lived.
	childCtx, cancelChild := workflow.WithCancel(ctx)
	childCtx = workflow.WithChildOptions(childCtx, workflow.ChildWorkflowOptions{
		WorkflowID: fmt.Sprintf("%s-turn-%d", input.SessionID, state.TurnCount),
	})

	agentFuture := workflow.ExecuteChildWorkflow(childCtx, AgentWorkflow, AgentWorkflowInput{
		SessionID:    input.SessionID,
		UserID:       input.UserID,
		AgentID:      input.AgentID,
		TurnKey:      turnKey,
		UserMessage:  userMessage,
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
	}

	// 2. Persist before reporting anything, so a failed or cancelled turn keeps
	// its transcript. The agent already flushed these messages as it produced
	// them; writing them again under the same keys is a no-op, and covers the
	// case where one of its flushes failed.
	if len(result.NewMessages) > 0 {
		if err := workflow.ExecuteActivity(actCtx, memAct.PersistContext, activity.PersistContextInput{
			SessionID: input.SessionID,
			TurnKey:   turnKey,
			Messages:  result.NewMessages,
		}).Get(ctx, nil); err != nil {
			return fmt.Errorf("persist context: %w", err)
		}
	}

	// 3. Report failures once the transcript is safe
	if !cancelled {
		if agentErr != nil {
			return fmt.Errorf("agent workflow: %w", agentErr)
		}
		if result.Error != "" {
			return fmt.Errorf("agent workflow: %s", result.Error)
		}
	}

	// 4. Check goal
	if !cancelled && result.GoalAchieved {
		state.Status = "completed"
	}

	return nil
}
