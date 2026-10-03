package workflow

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/conversation"
	"github.com/victor/temporal-agent/store"
)

const (
	// Thresholds for continuing the session workflow as a new run.
	maxSessionHistoryEvents = 4000
	maxSessionHistoryBytes  = 4 * 1024 * 1024

	SignalUserMessage = "user-message"
	SignalCancelAgent = "cancel-agent"
	QuerySessionState = "session-state"
)

// UserMessage is the payload of the user-message signal: the text and who
// wrote it. A session can have several users, so each message carries its
// author.
type UserMessage struct {
	Text     string `json:"text"`
	UserID   string `json:"user_id"`
	UserName string `json:"user_name"`
	// Stored: the server has written the message to the session's history
	// already. Every human message is stored as it arrives, whether or not it
	// calls the agent.
	Stored bool `json:"stored,omitempty"`
	// MessageID is the stored message's ID: the turns answering it read the
	// session up to it (activity.TurnHistory.UpTo). 0 when not stored.
	MessageID int64 `json:"message_id,omitempty"`
	// Agents answer the message one after another, in this order: the agents
	// it mentions, as the server resolved them. Empty: the session's agent
	// alone.
	Agents []AddressedAgent `json:"agents,omitempty"`
}

// AddressedAgent is an agent a message calls by its mention. The server
// resolves it: a workflow cannot read the agents.
type AddressedAgent struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Mention string `json:"mention"`
}

// Events an agent's turn sends to its session's members.
const (
	EventMessage   = "message"    // an answer
	EventToolCalls = "tool_calls" // the tools a step calls
	EventAskUser   = "ask_user"   // a question waits for a member's answer
)

// Turn events tell the web members when an agent starts a turn, and when it
// is over, its transcript persisted: the server knows at once, where the
// visibility queries lag. They go to the web whatever the session's channel.
const (
	EventTurnStarted = "turn_started"
	EventTurnDone    = "turn_done"
)

// TurnEvent is the data of a turn event.
type TurnEvent struct {
	AgentID   string `json:"agent_id"`
	AgentName string `json:"agent_name,omitempty"` // empty for the session's agent: the server names it
	Turn      string `json:"turn"`                 // the turn's key
}

type SessionWorkflowInput struct {
	SessionID    string `json:"session_id"`
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
		var userMessage UserMessage
		ok, _ := msgCh.ReceiveWithTimeout(ctx, idleTimeout, &userMessage)
		if !ok {
			logger.Info("Session timed out", "session_id", input.SessionID)
			state.Status = "completed"
			return nil
		}

		if err := processMessage(actCtx, ctx, input, userMessage, &state); err != nil {
			logger.Error("Turn failed", "session_id", input.SessionID, "turn", state.TurnCount, "error", err)
			// Notify the error to the client, don't kill the session
			notifyResponse(ctx, input.SessionID, input.Channel, input.ChannelID, "", fmt.Sprintf("Error processing message: %v", err))
			continue
		}

		// Drain queued messages
		for msgCh.ReceiveAsync(&userMessage) {
			if err := processMessage(actCtx, ctx, input, userMessage, &state); err != nil {
				logger.Error("Turn failed", "error", err)
				notifyResponse(ctx, input.SessionID, input.Channel, input.ChannelID, "", fmt.Sprintf("Error processing message: %v", err))
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

// processMessage runs the turns a human message calls for: one per agent it
// addresses, in order, or the session's agent's alone. Each turn reads the
// answers of the agents before it. A turn that fails or is stopped ends the
// message: the agents after it do not run.
func processMessage(actCtx, ctx workflow.Context, input SessionWorkflowInput, userMessage UserMessage, state *SessionState) error {
	// Backstop for every channel that can signal a session: an empty user
	// message is rejected by the LLM API, and once persisted it breaks every
	// later turn of this session.
	if strings.TrimSpace(userMessage.Text) == "" {
		workflow.GetLogger(ctx).Warn("Ignoring empty user message", "session_id", input.SessionID)
		return nil
	}

	// A cancel sent while no turn ran — the stop button clicked just as the
	// last turn ended — is about that turn, not this message: left in the
	// channel, it would interrupt the message before it starts.
	cancelCh := workflow.GetSignalChannel(ctx, SignalCancelAgent)
	for cancelCh.ReceiveAsync(nil) {
	}

	agents := userMessage.Agents
	if len(agents) == 0 {
		agents = []AddressedAgent{{ID: input.AgentID}}
	}

	// The turns read the conversation up to the message they answer, the
	// turns of the earlier messages, and each other's answers: a message
	// stored after it is the next one to answer, not part of this one
	// (activity.TurnHistory, store.TurnReads). A message the server did not
	// store (Stored false, no MessageID) falls back on the session's last
	// message: the turn writes it itself, after that.
	upTo := userMessage.MessageID
	if upTo == 0 {
		var memAct *activity.MemoryActivities
		if err := workflow.ExecuteActivity(actCtx, memAct.LastMessageID, activity.LastMessageIDInput{SessionID: input.SessionID}).Get(ctx, &upTo); err != nil {
			return fmt.Errorf("conversation snapshot: %w", err)
		}
	}
	// group names the turns answering the message, globally: the run ID
	// keeps it distinct from the same number in an earlier run of this
	// session, which a resumed session would otherwise reuse. It holds the
	// snapshot, so that a message stored while they run is read after them.
	// Each agent the message addresses has a turn of its own in it.
	group := store.TurnGroupKey(fmt.Sprintf("%s-%d", workflow.GetInfo(ctx).WorkflowExecution.RunID, state.TurnCount+1), upTo)
	var earlier []string

	for i, a := range agents {
		// A stop sent between two turns of the message is for the rest of it.
		if i > 0 && cancelCh.ReceiveAsync(nil) {
			notifyResponse(ctx, input.SessionID, input.Channel, input.ChannelID, "", "Agent interrupted by user.")
			return nil
		}
		turn := agentTurn{
			agentID:   a.ID,
			agentName: a.Name,
			key:       store.TurnKey(group, i),
			upTo:      upTo,
			earlier:   slices.Clone(earlier),
			partNote:  partNote(agents, i, userMessage),
			// On the channel, an answer that could be taken for another
			// agent's is signed.
			signReply: len(agents) > 1 || a.ID != input.AgentID,
		}
		// The session's prompt override is its own agent's: another agent
		// answers with its own prompt.
		if a.ID == input.AgentID {
			turn.systemPrompt = input.SystemPrompt
		}
		stopped, err := processTurn(actCtx, ctx, input, userMessage, turn, state, cancelCh)
		if err != nil || stopped {
			return err
		}
		earlier = append(earlier, turn.key)
	}
	return nil
}

// agentTurn is the agent one turn runs, and what it is told.
type agentTurn struct {
	agentID      string
	agentName    string   // empty for the session's agent
	key          string   // the turn's key, under which it writes
	upTo         int64    // the last message it reads besides the turns
	earlier      []string // the turns that answered the message before it
	systemPrompt string   // override; empty = the agent's own
	partNote     string   // see partNote
	signReply    bool
}

// maxQuotedMessageBytes bounds the quote of the message a part note is about.
const maxQuotedMessageBytes = 200

// partNote tells the i-th of the agents a message addresses which part is its
// own: they answer one after another, and each sees the whole message. Its
// text is not split, since a part can refer to another ("from there, tell
// me…"). The note quotes the message: one written meanwhile is stored after
// it, and the agent must still answer this one. Empty when the message
// addresses one agent.
func partNote(agents []AddressedAgent, i int, msg UserMessage) string {
	if len(agents) < 2 {
		return ""
	}
	names := make([]string, len(agents))
	for j, a := range agents {
		names[j] = strings.TrimPrefix(conversation.AgentLabel(cmp.Or(a.Name, a.ID), cmp.Or(a.Mention, a.ID)), "agent ")
	}
	quote := conversation.Clip(strings.Join(strings.Fields(msg.Text), " "), maxQuotedMessageBytes)
	from := ""
	if msg.UserName != "" {
		from = " from " + msg.UserName
	}
	var sb strings.Builder
	sb.WriteString("\n## Several agents addressed\n\n")
	fmt.Fprintf(&sb, "You are answering the message%s that reads “%s”. It addresses several agents, who answer it one after another, in this order: %s. You are %s: answer only the part meant for you.",
		from, quote, strings.Join(names, ", "), names[i])
	if i > 0 {
		sb.WriteString(" The agents before you have answered already, above, in their [agent …] blocks: build on what they said rather than repeat it.")
	}
	if i < len(agents)-1 {
		sb.WriteString(" The agents after you answer next and will see your reply: leave their part to them.")
	}
	sb.WriteString(" Messages written after it get their own turn: answer this one.\n")
	return sb.String()
}

// processTurn runs one agent on a user message, then persists what the turn
// produced. The agent's LLM calls load the conversation themselves.
// processTurn listens for cancel-agent signals to interrupt the agent
// mid-execution, and reports whether it was stopped.
func processTurn(actCtx, ctx workflow.Context, input SessionWorkflowInput, userMessage UserMessage, turn agentTurn, state *SessionState, cancelCh workflow.ReceiveChannel) (bool, error) {
	state.Status = "processing"
	state.TurnCount++

	var memAct *activity.MemoryActivities
	turnKey := turn.key

	// The members see the turn start before it runs, and end once it is
	// persisted, however it ends: failed, stopped, or its persist failing.
	event := TurnEvent{AgentID: turn.agentID, AgentName: turn.agentName, Turn: turnKey}
	notifyTurn(ctx, input.SessionID, EventTurnStarted, event)
	defer notifyTurn(ctx, input.SessionID, EventTurnDone, event)

	// 1. Launch agent child workflow with a cancellable context. It is given
	// the conversation by reference: passing it here put a full copy of the
	// history in this workflow's event history on every turn, and this
	// workflow is long-lived.
	childCtx, cancelChild := workflow.WithCancel(ctx)
	childCtx = workflow.WithChildOptions(childCtx, workflow.ChildWorkflowOptions{
		WorkflowID: fmt.Sprintf("%s-turn-%d", input.SessionID, state.TurnCount),
		// A cancelled turn still writes what it produced, then returns it,
		// and the session waits for that: a child's future settles only on
		// the child's close, cancelled or not (the SDK's child-workflow
		// semantics). This option says the intent; the SDK (v1.33) does not
		// read it for a child.
		WaitForCancellation: true,
	})

	agentFuture := workflow.ExecuteChildWorkflow(childCtx, AgentWorkflow, AgentWorkflowInput{
		SessionID: input.SessionID,
		// The turn answers its author: their memory is loaded, tools act for
		// them.
		UserID:            userMessage.UserID,
		UserName:          userMessage.UserName,
		AgentID:           turn.agentID,
		TurnKey:           turnKey,
		HistoryUpTo:       turn.upTo,
		EarlierTurns:      turn.earlier,
		UserMessage:       userMessage.Text,
		UserMessageStored: userMessage.Stored,
		SystemPrompt:      turn.systemPrompt,
		Model:             input.Model,
		Channel:           input.Channel,
		ChannelID:         input.ChannelID,
		PartNote:          turn.partNote,
		SignReply:         turn.signReply,
	})

	// Listen for cancel signal in parallel
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
		// Wait for the child to end. AgentWorkflow answers a cancel by
		// returning its transcript, not an error: the run completes, and its
		// output arrives here. A child cancelled before it produced anything
		// fails with a CanceledError instead, and leaves result empty.
		_ = agentFuture.Get(ctx, &result)
	}

	// 2. Persist before reporting anything, so a failed or cancelled turn keeps
	// its transcript. The agent already flushed these messages as it produced
	// them; writing them again under the same keys is a no-op, and covers the
	// case where one of its flushes failed.
	// A failure is written after them: the members see why the agent stopped,
	// on every channel and after a reload, not only in a notification.
	messages := result.NewMessages
	if !cancelled && result.Error != "" {
		messages = append(messages, store.Message{Role: store.RoleAssistant, Kind: store.KindTurnError, AgentID: turn.agentID, Content: turnErrorContent(result.Error)})
	}
	if len(messages) > 0 {
		if err := workflow.ExecuteActivity(actCtx, memAct.PersistContext, activity.PersistContextInput{
			SessionID: input.SessionID,
			TurnKey:   turnKey,
			Messages:  messages,
		}).Get(ctx, nil); err != nil {
			return cancelled, fmt.Errorf("persist context: %w", err)
		}
	}

	// 3. Report the stop or the failure once the transcript is safe: the
	// interface reloads the conversation when told.
	if cancelled {
		notifyResponse(ctx, input.SessionID, input.Channel, input.ChannelID, "", "Agent interrupted by user.")
		return true, nil
	}
	if agentErr != nil {
		return false, fmt.Errorf("agent workflow: %w", agentErr)
	}
	if result.Error != "" {
		return false, fmt.Errorf("agent workflow: %s", result.Error)
	}

	// 4. Check goal
	if result.GoalAchieved {
		state.Status = "completed"
	}

	return false, nil
}

// turnNotifyOptions are those of a turn event: one short attempt. The turn
// waits for it (a turn's start must not overtake the end of the one before),
// so a server slow or away must not hold the turn: a page that misses the
// event falls back on the visibility queries.
var turnNotifyOptions = workflow.ActivityOptions{
	StartToCloseTimeout: 3 * time.Second,
	RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
}

// notifyTurn sends a turn event to the session's web members. Best effort:
// its failure neither fails nor holds the turn.
func notifyTurn(ctx workflow.Context, sessionID, eventType string, e TurnEvent) {
	notifySessionWith(ctx, turnNotifyOptions, sessionID, eventType, map[string]string{"agent_id": e.AgentID, "agent_name": e.AgentName, "turn": e.Turn})
}

// maxTurnErrorBytes bounds the error kept in the conversation: an API error can
// carry a whole response body.
const maxTurnErrorBytes = 2000

// turnErrorContent is the stored content of a KindTurnError message: the error
// as a JSON string, like any message's text.
func turnErrorContent(reason string) string {
	if len(reason) > maxTurnErrorBytes {
		cut := maxTurnErrorBytes
		for cut > 0 && !utf8.RuneStart(reason[cut]) {
			cut--
		}
		reason = reason[:cut] + "…"
	}
	b, _ := json.Marshal(reason)
	return string(b)
}
