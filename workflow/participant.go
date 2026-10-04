package workflow

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/conversation"
	"github.com/victor/temporal-agent/store"
)

// A participant is an agent in a session. It answers its messages in order,
// one at a time, in a ParticipantWorkflow that runs only while it has some
// to answer; participants answer in parallel.
const (
	// SignalMessage delivers a message to a participant (ParticipantMessage),
	// by SignalWithStart: the participant starts if it does not run.
	SignalMessage = "message"
	// SignalStopTurn stops the turn running, not the messages waiting.
	SignalStopTurn = "stop-turn"
	// SignalClear stops the turn running and drops the messages waiting.
	SignalClear = "clear"
	// QueryState answers a ParticipantState.
	QueryState = "state"
)

// Workflow IDs reserve ':', which no session ID (a UUID) nor agent ID holds:
// the session of a workflow is what precedes its first ':' (SessionOf).
//
//	participant      <session>:p:<agent>
//	its turn         <session>:p:<agent>:m<message id>
//	a turn's tools   <turn>:tool:<tool>:<call>   (ask_user, sub-agents…)
const (
	participantMark = ":p:"
	toolMark        = ":tool:"
)

// ParticipantWorkflowID is the ID of the participant agentID in a session.
func ParticipantWorkflowID(sessionID, agentID string) string {
	return sessionID + participantMark + agentID
}

// turnWorkflowID is the ID of the participant's turn on a message.
func turnWorkflowID(participantID string, messageID int64) string {
	return fmt.Sprintf("%s:m%d", participantID, messageID)
}

// SessionOf is the session a workflow ID belongs to: what precedes its first
// ':'. False for a workflow of no session (a fork's summary, a report, a
// scheduled task's run).
func SessionOf(workflowID string) (string, bool) {
	i := strings.IndexByte(workflowID, ':')
	if i <= 0 {
		return "", false
	}
	return workflowID[:i], true
}

// ParticipantOf is the agent of the participant a workflow ID belongs to:
// "<session>:p:<agent>", or anything a turn of it started. False for any
// other ID.
func ParticipantOf(workflowID string) (string, bool) {
	session, ok := SessionOf(workflowID)
	rest, found := strings.CutPrefix(workflowID[len(session):], participantMark)
	if !ok || !found {
		return "", false
	}
	agent, _, _ := strings.Cut(rest, ":")
	return agent, agent != ""
}

// ParticipantInput starts a participant. It is fixed at the start: what
// varies from a message to the next travels with the message.
type ParticipantInput struct {
	SessionID string `json:"session_id"`
	AgentID   string `json:"agent_id"`
	// Channel and ChannelID are the session's: where an answer goes when the
	// message names no channel.
	Channel   string `json:"channel,omitempty"`
	ChannelID string `json:"channel_id,omitempty"`
	// Inbox holds the messages a run left unanswered when it continued as
	// new.
	Inbox []ParticipantMessage `json:"inbox,omitempty"`
}

// ParticipantMessage is a message delivered to a participant. It carries no
// text: every message is stored before it is delivered, and the turn loads
// it with the conversation. A signal, and an inbox carried by a
// continue-as-new, stay small however long the messages.
type ParticipantMessage struct {
	// MessageID is the message, stored already: the turn's anchor.
	MessageID int64 `json:"message_id"`
	// UserID and UserName are its author: their memory is loaded, the tools
	// act for them.
	UserID   string `json:"user_id"`
	UserName string `json:"user_name,omitempty"`
	// Next are the participants the message is relayed to after this one,
	// in order; EarlierTurns the turns that answered it before (a relay).
	Next         []AddressedAgent `json:"next,omitempty"`
	EarlierTurns []string         `json:"earlier_turns,omitempty"`
	// SignReply signs the answer on the channel: several agents answer.
	SignReply bool   `json:"sign_reply,omitempty"`
	Channel   string `json:"channel,omitempty"`
	ChannelID string `json:"channel_id,omitempty"`
	// Part is set when the message addresses several agents: each is told
	// its part (partNote).
	Part *Part `json:"part,omitempty"`
}

// Part is what a message addressed to several agents tells each of them:
// the agents in the order they answer, and the message, quoted.
type Part struct {
	Agents []AddressedAgent `json:"agents"`
	Quote  string           `json:"quote"`
}

// AddressedAgent is an agent a message calls by its mention. The server
// resolves it: a workflow cannot read the agents.
type AddressedAgent struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Mention string `json:"mention"`
}

// ParticipantState is what the state query answers: the message the
// participant answers, how many wait, and its background tasks (none yet).
type ParticipantState struct {
	Current    *CurrentMessage `json:"current"`
	Queued     int             `json:"queued"`
	Background []string        `json:"background"`
}

// CurrentMessage is the message a participant answers.
type CurrentMessage struct {
	MessageID int64     `json:"message_id"`
	UserName  string    `json:"user_name,omitempty"`
	Since     time.Time `json:"since"`
}

// Turn events tell the web members when a participant starts a turn, and
// when it is done with a message, the turn's end written: the server knows
// at once, where the visibility queries lag. They go to the web whatever the
// session's channel. A message answered without a turn (delivered twice,
// its agent gone, dropped by a clear) has its turn_done alone.
const (
	EventTurnStarted = "turn_started"
	EventTurnDone    = "turn_done"
)

// TurnEvent is the data of a turn event. The turn's key names the message
// and the participant (store.TurnAnchor, store.TurnParticipant).
type TurnEvent struct {
	AgentID   string `json:"agent_id"`
	AgentName string `json:"agent_name,omitempty"`
	Turn      string `json:"turn"`
}

// Thresholds for continuing a participant as a new run: a participant lives
// while it has messages, and messages may keep coming. Well under
// Temporal's hard limits (51200 events, 50MB), since a turn adds a lot at
// once.
const (
	maxParticipantHistoryEvents = 4000
	maxParticipantHistoryBytes  = 4 * 1024 * 1024
)

// Activity options of a participant's steps.
var (
	// checkTurnOptions: about twelve attempts, so that a store away for a
	// moment (a restart, a failover: about two minutes) loses no message;
	// only a missing agent or session is final (activity.ErrTypeAgentNotFound,
	// activity.ErrTypeSessionGone, never retried).
	checkTurnOptions = storeStepOptions
	// endTurnOptions: the same; past them the turn has no end (it stays
	// unread by the others, and answered again if delivered again).
	endTurnOptions = storeStepOptions
	// storeStepOptions are those of a step that only reads or writes the
	// store. A number of attempts, not a duration: a ScheduleToClose would
	// also count the wait for a worker, and a worker away a few minutes (a
	// redeploy) would fail the step; with attempts, the step waits for the
	// worker like the workflow does, and only the store being away is bounded
	// (attempts at 0, 1, 3, 7, 15, 30… 120 s).
	storeStepOptions = workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    15 * time.Second,
			MaximumAttempts:    12,
		},
	}
	// relayOptions: about five attempts over a minute; past them the relay
	// fails (relay).
	relayOptions = workflow.ActivityOptions{
		StartToCloseTimeout:    10 * time.Second,
		ScheduleToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    15 * time.Second,
			MaximumAttempts:    5,
		},
	}
	persistTurnOptions = workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	}
)

// clearedReason is the end of a message a clear dropped: shown in the
// thread, and what keeps a late delivery of it from being answered.
const clearedReason = "Annulé avant d'être traité : la file de l'agent a été vidée."

// ParticipantWorkflow answers a participant's messages, in order, one at a
// time, then ends: with nothing left in its inbox there is nothing to keep.
// The server delivers each message by SignalWithStart, which starts it
// again. A signal arriving while it ends is not lost: the Temporal server
// refuses to close a workflow with a signal it did not handle
// (UNHANDLED_COMMAND), and the next workflow task sees it.
func ParticipantWorkflow(ctx workflow.Context, in ParticipantInput) error {
	p := &participant{in: in, id: ParticipantWorkflowID(in.SessionID, in.AgentID), inbox: in.Inbox}
	messages := workflow.GetSignalChannel(ctx, SignalMessage)
	if err := workflow.SetQueryHandler(ctx, QueryState, func() (ParticipantState, error) { return p.state(messages), nil }); err != nil {
		return fmt.Errorf("set query handler: %w", err)
	}
	stops := workflow.GetSignalChannel(ctx, SignalStopTurn)
	clears := workflow.GetSignalChannel(ctx, SignalClear)

	for {
		p.receive(messages)
		if drained(clears) {
			p.clear(ctx, messages)
		}
		if len(p.inbox) == 0 || p.gone {
			drained(stops) // about no turn
			return p.close(ctx, nil)
		}
		// The inbox goes with the new run: a participant fed without pause
		// would never reach an empty one.
		if participantHistoryIsLarge(ctx) {
			drained(stops)
			next := p.in
			next.Inbox = p.inbox
			workflow.GetLogger(ctx).Info("Participant continues as new", "participant", p.id, "inbox", len(p.inbox))
			return p.close(ctx, workflow.NewContinueAsNewError(ctx, ParticipantWorkflow, next))
		}
		msg := p.inbox[0]
		p.inbox = p.inbox[1:]
		p.answer(ctx, msg, messages, stops, clears)
	}
}

// participant is a ParticipantWorkflow's state.
type participant struct {
	in      ParticipantInput
	id      string // its workflow ID
	inbox   []ParticipantMessage
	current *CurrentMessage
	// gone: the session was deleted; the participant ends.
	gone bool
}

// state is what the state query answers: the messages waiting are those of
// the inbox and those delivered but not yet received (the participant was
// in an activity).
func (p *participant) state(messages workflow.ReceiveChannel) ParticipantState {
	return ParticipantState{Current: p.current, Queued: len(p.inbox) + messages.Len(), Background: []string{}}
}

// receive moves the messages delivered so far to the inbox.
func (p *participant) receive(messages workflow.ReceiveChannel) {
	var m ParticipantMessage
	for messages.ReceiveAsync(&m) {
		p.inbox = append(p.inbox, m)
		m = ParticipantMessage{}
	}
}

// drained empties ch and reports whether it held anything.
func drained(ch workflow.ReceiveChannel) bool {
	held := false
	for ch.ReceiveAsync(nil) {
		held = true
	}
	return held
}

// close ends the run with err, once every signal is handled: none is left
// unread, or the SDK would warn of it, and it would be lost.
func (p *participant) close(ctx workflow.Context, err error) error {
	if left := workflow.GetUnhandledSignalNames(ctx); len(left) > 0 {
		workflow.GetLogger(ctx).Error("Participant ends with unhandled signals", "participant", p.id, "signals", left)
	}
	return err
}

// participantHistoryIsLarge reports whether the run's history is big enough
// to warrant a fresh one.
func participantHistoryIsLarge(ctx workflow.Context) bool {
	info := workflow.GetInfo(ctx)
	return info.GetCurrentHistoryLength() >= maxParticipantHistoryEvents ||
		info.GetCurrentHistorySize() >= maxParticipantHistoryBytes
}

// clear drops the inbox: each message dropped gets an end saying so, so
// that a late delivery of it (a relay retried) is not answered.
func (p *participant) clear(ctx workflow.Context, messages workflow.ReceiveChannel) {
	p.receive(messages)
	dropped := p.inbox
	p.inbox = nil
	for _, m := range dropped {
		if m.MessageID <= 0 {
			continue
		}
		turnKey := store.TurnKey(m.MessageID, p.in.AgentID)
		p.endTurn(ctx, turnKey, store.TurnEnd(p.in.AgentID, clearedReason))
		notifyTurn(ctx, p.in.SessionID, EventTurnDone, TurnEvent{AgentID: p.in.AgentID, Turn: turnKey})
	}
}

// answer runs the participant's turn on a message, and does what the turn
// itself cannot be trusted to: its events, its rewrite, its end, the error
// on the channel, and the relay to the next participant. The participant
// knows how the turn's workflow ended; the turn does not always (a failure
// before it wrote anything, a termination from outside).
func (p *participant) answer(ctx workflow.Context, msg ParticipantMessage, messages, stops, clears workflow.ReceiveChannel) {
	logger := workflow.GetLogger(ctx)
	if msg.MessageID <= 0 {
		// Every message is stored before it is delivered: one without an ID
		// cannot be read, and an empty one would poison every later turn.
		logger.Warn("Ignoring a message with no ID", "participant", p.id)
		return
	}
	turnKey := store.TurnKey(msg.MessageID, p.in.AgentID)
	// The message's channel, or the session's: a pair, never mixed.
	channel, channelID := p.in.Channel, p.in.ChannelID
	if msg.Channel != "" {
		channel, channelID = msg.Channel, msg.ChannelID
	}
	p.current = &CurrentMessage{MessageID: msg.MessageID, UserName: msg.UserName, Since: workflow.Now(ctx)}
	defer func() { p.current = nil }()
	event := TurnEvent{AgentID: p.in.AgentID, Turn: turnKey}
	// Done with the message, however: the server counts it out.
	defer func() { notifyTurn(ctx, p.in.SessionID, EventTurnDone, event) }()
	// With several agents answering, every word on the channel is signed,
	// its errors too: by the agent's ID until its name is read.
	signer := ""
	if msg.SignReply {
		signer = p.in.AgentID
	}

	var turnAct *activity.TurnActivities
	var check activity.CheckTurnOutput
	err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, checkTurnOptions), turnAct.CheckTurn, activity.CheckTurnInput{
		SessionID: p.in.SessionID, AgentID: p.in.AgentID, TurnKey: turnKey,
	}).Get(ctx, &check)
	switch {
	case hasErrorType(err, activity.ErrTypeSessionGone):
		// The messages left in the inbox get no turn_done: the server
		// forgot the session when it deleted it, its counts included.
		logger.Info("Session deleted: the participant ends", "participant", p.id)
		p.gone = true
		return
	case err != nil:
		reason := fmt.Sprintf("the agent could not answer: %s", failureText(err))
		p.endTurn(ctx, turnKey, store.TurnEnd(p.in.AgentID, reason))
		p.notifyError(ctx, msg, channel, channelID, signer, reason)
		return
	case check.Answered:
		logger.Info("Message answered already: not again", "participant", p.id, "message_id", msg.MessageID)
		return
	}
	if msg.SignReply {
		signer = cmp.Or(check.AgentName, p.in.AgentID)
	}
	// A stop is for a turn whose start was told: one sent before (the
	// button clicked as the last turn ended, or during the check) is about
	// another turn, not this one.
	drained(stops)
	event.AgentName = check.AgentName
	notifyTurn(ctx, p.in.SessionID, EventTurnStarted, event)

	result, stopped, cleared, reason := p.runTurn(ctx, msg, turnKey, channel, channelID, messages, stops, clears)

	// The turn wrote its messages as it went; written again under the same
	// keys, they change nothing, and cover a write of its that failed.
	if len(result.NewMessages) > 0 {
		var memAct *activity.MemoryActivities
		if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, persistTurnOptions), memAct.PersistContext, activity.PersistContextInput{
			SessionID: p.in.SessionID, TurnKey: turnKey, Messages: result.NewMessages,
		}).Get(ctx, nil); err != nil {
			logger.Error("Turn not rewritten", "participant", p.id, "turn", turnKey, "error", err)
		}
	}
	// Its end, in every case, after what it wrote: the other participants
	// read the turn from there, and a second delivery is not answered.
	p.endTurn(ctx, turnKey, store.TurnEnd(p.in.AgentID, reason))

	switch {
	case stopped:
		notifyResponse(ctx, p.in.SessionID, channel, channelID, signer, "Agent interrupted by user.")
	case reason != "":
		p.notifyError(ctx, msg, channel, channelID, signer, reason)
	case len(msg.Next) > 0:
		p.relay(ctx, msg, turnKey, channel, channelID, signer)
	}
	if cleared {
		p.clear(ctx, messages)
	}
}

// runTurn runs the turn's AgentWorkflow until it ends, stopped or not, and
// reports what it produced and why it failed ("" when it did not, or was
// stopped). Messages delivered meanwhile join the inbox: the state query
// counts them.
func (p *participant) runTurn(ctx workflow.Context, msg ParticipantMessage, turnKey, channel, channelID string, messages, stops, clears workflow.ReceiveChannel) (result AgentWorkflowOutput, stopped, cleared bool, reason string) {
	childCtx, cancel := workflow.WithCancel(ctx)
	childCtx = workflow.WithChildOptions(childCtx, workflow.ChildWorkflowOptions{
		WorkflowID: turnWorkflowID(p.id, msg.MessageID),
		// A stopped turn still writes what it produced and returns it; the
		// participant waits for that: a child's future settles only on its
		// close (the SDK's child-workflow semantics). This option says the
		// intent; the SDK (v1.33) does not read it for a child.
		WaitForCancellation: true,
	})
	future := workflow.ExecuteChildWorkflow(childCtx, AgentWorkflow, AgentWorkflowInput{
		SessionID:    p.in.SessionID,
		UserID:       msg.UserID,
		UserName:     msg.UserName,
		AgentID:      p.in.AgentID,
		TurnKey:      turnKey,
		EarlierTurns: msg.EarlierTurns,
		Channel:      channel,
		ChannelID:    channelID,
		PartNote:     msg.Part.noteFor(p.in.AgentID, msg.UserName),
		SignReply:    msg.SignReply,
	})

	var agentErr error
	done := false
	sel := workflow.NewSelector(ctx)
	sel.AddFuture(future, func(f workflow.Future) {
		agentErr = f.Get(ctx, &result)
		done = true
	})
	stop := func(clear bool) func(workflow.ReceiveChannel, bool) {
		return func(ch workflow.ReceiveChannel, _ bool) {
			ch.Receive(ctx, nil)
			if !stopped {
				cancel()
			}
			stopped, cleared = true, cleared || clear
		}
	}
	sel.AddReceive(stops, stop(false))
	sel.AddReceive(clears, stop(true))
	sel.AddReceive(messages, func(ch workflow.ReceiveChannel, _ bool) {
		var m ParticipantMessage
		ch.Receive(ctx, &m)
		p.inbox = append(p.inbox, m)
	})
	for !done {
		sel.Select(ctx)
	}

	switch {
	case stopped:
		// Stopped: no error to tell. A turn cancelled before it produced
		// anything fails with a CanceledError, and leaves result empty.
	case agentErr != nil:
		reason = "agent workflow: " + agentErr.Error()
	default:
		reason = result.Error
	}
	return result, stopped, cleared, reason
}

// endTurn writes a turn's end. Past its attempts, it is logged and the
// participant goes on: the turn stays unread by the others, and a second
// delivery of its message would answer it again.
func (p *participant) endTurn(ctx workflow.Context, turnKey string, end store.Message) {
	var turnAct *activity.TurnActivities
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, endTurnOptions), turnAct.EndTurn, activity.EndTurnInput{
		SessionID: p.in.SessionID, TurnKey: turnKey, Message: end,
	}).Get(ctx, nil); err != nil {
		workflow.GetLogger(ctx).Error("Turn end not written", "participant", p.id, "turn", turnKey, "error", err)
	}
}

// notifyError tells the message's channel why it got no answer, after the
// end saying so is written: the interface reloads the thread when told.
func (p *participant) notifyError(ctx workflow.Context, msg ParticipantMessage, channel, channelID, signer, reason string) {
	workflow.GetLogger(ctx).Error("Turn failed", "participant", p.id, "message_id", msg.MessageID, "error", reason)
	notifyResponse(ctx, p.in.SessionID, channel, channelID, signer, "Error processing message: "+reason)
}

// relay hands the message to the next participant it addresses, with this
// turn among those it reads. A relay is an activity: a workflow cannot start
// another one by signal. When it fails for good, the next participant's turn
// gets an end saying so, so that a late delivery is not answered; this
// participant goes on with its own messages either way.
//
// A failure is told signed by signer, the relaying agent: it is the one
// that speaks.
func (p *participant) relay(ctx workflow.Context, msg ParticipantMessage, turnKey, channel, channelID, signer string) {
	next := msg.Next[0]
	relayed := msg
	relayed.Next = slices.Clone(msg.Next[1:])
	relayed.EarlierTurns = append(slices.Clone(msg.EarlierTurns), turnKey)
	message, _ := json.Marshal(relayed)
	start, _ := json.Marshal(ParticipantInput{SessionID: p.in.SessionID, AgentID: next.ID, Channel: p.in.Channel, ChannelID: p.in.ChannelID})

	var relayAct *activity.RelayActivities
	err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, relayOptions), relayAct.Relay, activity.RelayInput{
		WorkflowID:   ParticipantWorkflowID(p.in.SessionID, next.ID),
		WorkflowType: "ParticipantWorkflow",
		TaskQueue:    workflow.GetInfo(ctx).TaskQueueName,
		Signal:       SignalMessage,
		Message:      message,
		Start:        start,
	}).Get(ctx, nil)
	if err == nil {
		return
	}
	reason := fmt.Sprintf("le relais vers @%s a échoué : %s", cmp.Or(next.Mention, next.ID), failureText(err))
	p.endTurn(ctx, store.TurnKey(msg.MessageID, next.ID), store.TurnEnd(next.ID, reason))
	p.notifyError(ctx, msg, channel, channelID, signer, reason)
}

// maxQuotedMessageBytes bounds the quote of the message a part note is about.
const maxQuotedMessageBytes = 200

// Quote is a message as a part note quotes it: on one line, clipped.
func Quote(text string) string {
	return conversation.Clip(strings.Join(strings.Fields(text), " "), maxQuotedMessageBytes)
}

// noteFor is the part note of agentID, "" when the message addresses it
// alone or does not name it.
func (part *Part) noteFor(agentID, userName string) string {
	if part == nil {
		return ""
	}
	i := slices.IndexFunc(part.Agents, func(a AddressedAgent) bool { return a.ID == agentID })
	if i < 0 {
		return ""
	}
	return partNote(part.Agents, i, part.Quote, userName)
}

// partNote tells the i-th of the agents a message addresses which part is its
// own: they answer one after another, and each sees the whole message. Its
// text is not split, since a part can refer to another ("from there, tell
// me…"). The note quotes the message (Quote): one written meanwhile is
// stored after it, and the agent must still answer this one. Empty when the
// message addresses one agent.
func partNote(agents []AddressedAgent, i int, quote, userName string) string {
	if len(agents) < 2 {
		return ""
	}
	names := make([]string, len(agents))
	for j, a := range agents {
		names[j] = strings.TrimPrefix(conversation.AgentLabel(cmp.Or(a.Name, a.ID), cmp.Or(a.Mention, a.ID)), "agent ")
	}
	from := ""
	if userName != "" {
		from = " from " + userName
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

// turnNotifyOptions are those of a turn event: one short attempt. The
// participant waits for it (a turn's start must not overtake the end of the
// one before), so a server slow or away must not hold it: a page that
// misses the event falls back on the visibility queries. ScheduleToClose
// bounds the wait for a worker to pick the attempt up, which StartToClose
// does not count.
var turnNotifyOptions = workflow.ActivityOptions{
	ScheduleToCloseTimeout: 5 * time.Second,
	StartToCloseTimeout:    3 * time.Second,
	RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 1},
}

// notifyTurn sends a turn event to the session's web members. Best effort:
// its failure neither fails nor holds the turn.
func notifyTurn(ctx workflow.Context, sessionID, eventType string, e TurnEvent) {
	notifySessionWith(ctx, turnNotifyOptions, sessionID, eventType, map[string]string{"agent_id": e.AgentID, "agent_name": e.AgentName, "turn": e.Turn})
}
