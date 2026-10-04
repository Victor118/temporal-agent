package activity

import (
	"context"
	"encoding/json"
	"fmt"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"

	"github.com/victor/temporal-agent/store"
)

// Error types of a turn's check, never retried: the participant answers
// them without a turn.
const (
	// ErrTypeAgentNotFound: the participant's agent was deleted, with
	// messages still waiting for it.
	ErrTypeAgentNotFound = "AgentNotFound"
	// ErrTypeSessionGone: the session was deleted; its participants stop.
	ErrTypeSessionGone = "SessionGone"
)

// TurnStore is what a participant's turns read and write of the store,
// besides what the turn itself writes.
type TurnStore interface {
	GetSession(ctx context.Context, sessionID string) (*store.Session, error)
	GetAgent(ctx context.Context, agentID string) (*store.Agent, error)
	HasTurnEnd(ctx context.Context, sessionID, turnKey string) (bool, error)
	AppendTurnEnd(ctx context.Context, sessionID, turnKey string, msg store.Message) (int64, error)
}

// TurnActivities keep a participant's turns: whether one may run, and its
// end.
type TurnActivities struct {
	Store TurnStore
}

type CheckTurnInput struct {
	SessionID string `json:"session_id"`
	AgentID   string `json:"agent_id"`
	TurnKey   string `json:"turn_key"`
}

type CheckTurnOutput struct {
	// Answered: the turn has its end already. The message was delivered
	// twice (a retried delivery or relay), and is not answered again.
	Answered bool `json:"answered,omitempty"`
	// AgentName is the agent's name, for the turn events.
	AgentName string `json:"agent_name,omitempty"`
}

// CheckTurn tells whether a participant's turn may run: its session is
// there, it did not answer the message already, and its agent exists. The
// agent is read from the store, not from the worker's catalog: refreshed
// every 30 s, the catalog would refuse an agent created a moment ago, which
// the server already resolved from the store.
func (a *TurnActivities) CheckTurn(ctx context.Context, in CheckTurnInput) (CheckTurnOutput, error) {
	sess, err := a.Store.GetSession(ctx, in.SessionID)
	if err != nil {
		return CheckTurnOutput{}, fmt.Errorf("read session: %w", err)
	}
	if sess == nil {
		return CheckTurnOutput{}, temporal.NewNonRetryableApplicationError("session deleted", ErrTypeSessionGone, nil)
	}
	answered, err := a.Store.HasTurnEnd(ctx, in.SessionID, in.TurnKey)
	if err != nil {
		return CheckTurnOutput{}, fmt.Errorf("read turn end: %w", err)
	}
	if answered {
		return CheckTurnOutput{Answered: true}, nil
	}
	agent, err := a.Store.GetAgent(ctx, in.AgentID)
	if err != nil {
		return CheckTurnOutput{}, fmt.Errorf("read agent: %w", err)
	}
	if agent == nil {
		return CheckTurnOutput{}, temporal.NewNonRetryableApplicationError(fmt.Sprintf("agent %q no longer exists", in.AgentID), ErrTypeAgentNotFound, nil)
	}
	return CheckTurnOutput{AgentName: agent.Name}, nil
}

type EndTurnInput struct {
	SessionID string        `json:"session_id"`
	TurnKey   string        `json:"turn_key"`
	Message   store.Message `json:"message"` // a store.TurnEnd
}

// EndTurn writes a turn's end (store.AppendTurnEnd).
func (a *TurnActivities) EndTurn(ctx context.Context, in EndTurnInput) (int64, error) {
	return a.Store.AppendTurnEnd(ctx, in.SessionID, in.TurnKey, in.Message)
}

// SignalStarter is what the relay needs of the Temporal client.
type SignalStarter interface {
	SignalWithStartWorkflow(ctx context.Context, workflowID, signalName string, signalArg interface{}, options client.StartWorkflowOptions, workflow interface{}, workflowArgs ...interface{}) (client.WorkflowRun, error)
}

// RelayActivities hand a message from a participant to the next one.
type RelayActivities struct {
	Client SignalStarter
}

// RelayInput is a delivery to a participant, started if it does not run:
// the signal and its argument, and the workflow, its queue and its input.
// The arguments are encoded by the workflow that relays: this package
// does not know its types.
type RelayInput struct {
	WorkflowID   string          `json:"workflow_id"`
	WorkflowType string          `json:"workflow_type"`
	TaskQueue    string          `json:"task_queue"`
	Signal       string          `json:"signal"`
	Message      json.RawMessage `json:"message"`
	Start        json.RawMessage `json:"start"`
}

// Relay delivers a message to a participant, as the server does: one
// SignalWithStart, which starts it when it does not run. A workflow cannot
// do it itself (a signal to another workflow starts nothing). A retry may
// deliver the message twice: the participant answers it once (CheckTurn).
func (a *RelayActivities) Relay(ctx context.Context, in RelayInput) error {
	if _, err := a.Client.SignalWithStartWorkflow(ctx, in.WorkflowID, in.Signal, in.Message, client.StartWorkflowOptions{
		ID:        in.WorkflowID,
		TaskQueue: in.TaskQueue,
	}, in.WorkflowType, in.Start); err != nil {
		return fmt.Errorf("deliver to %s: %w", in.WorkflowID, err)
	}
	return nil
}
