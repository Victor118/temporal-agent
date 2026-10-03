package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/tool"
)

const maxReActIterations = 50

// maxToolResultBytes caps what a single tool result contributes to the
// conversation. An unbounded result is recorded three times over — as the
// activity result, inside the next LLM request and as a persisted message — and
// one large enough to cross Temporal's 2MB payload limit fails the turn
// outright. It also keeps a runaway command from eating the model's context.
const maxToolResultBytes = 96 * 1024

// toolScheduleToStartTimeout bounds how long a tool call waits for a worker on
// its task queue before being reported to the LLM as unavailable.
const toolScheduleToStartTimeout = 60 * time.Second

type AgentWorkflowInput struct {
	SessionID   string `json:"session_id"`
	UserID      string `json:"user_id,omitempty"`   // Author of UserMessage: whose memory is loaded, who tools act for
	UserName    string `json:"user_name,omitempty"` // Author's name, shown to the model
	AgentID     string `json:"agent_id"`            // Required. Logical agent identity: prompt, skills and allowed tools
	UserMessage string `json:"user_message"`
	// UserMessageStored: the message is in the session's history already (the
	// server stores human messages as they arrive), so the turn loads it with
	// the rest instead of adding it.
	UserMessageStored bool   `json:"user_message_stored,omitempty"`
	SystemPrompt      string `json:"system_prompt"`
	Model             string `json:"model"` // Explicit model; empty = the worker's default (LLM_MODEL)
	// TurnKey identifies the session turn this run belongs to. When set, the
	// agent persists its messages as it produces them under that key, so a
	// crash, a cancel or a failed LLM call cannot lose the transcript. Sub-agents
	// and scheduled runs leave it empty: they own no session history.
	TurnKey string `json:"turn_key,omitempty"`
	// HistoryUpTo and EarlierTurns are what a session turn reads besides its
	// own messages: the session up to that message ID, where it stood when
	// the message this turn answers started its turns, and the turns that
	// answered it before this one (activity.TurnHistory).
	HistoryUpTo  int64    `json:"history_up_to,omitempty"`
	EarlierTurns []string `json:"earlier_turns,omitempty"`
	// LoadUserMemory gives UserID's memory to a run that has no turn key: a
	// scheduled task answers its user directly, as a session turn does. A
	// sub-agent leaves it unset: its context stays isolated, and its parent
	// already had the memory.
	LoadUserMemory bool     `json:"load_user_memory,omitempty"`
	AgentChain     []string `json:"agent_chain,omitempty"` // Chain of parent agent IDs for context propagation
	// Channel and ChannelID are where the session's user is reached ("web",
	// "telegram" and its chat_id). A sub-agent inherits them so its questions
	// reach the user; only the session's own turn sends its answer there.
	Channel   string `json:"channel,omitempty"`
	ChannelID string `json:"channel_id,omitempty"`
	// PartNote ends the system prompt of a run that answers a message
	// addressed to several agents: which part is its own (see partNote).
	PartNote string `json:"part_note,omitempty"`
	// SignReply signs the answer sent to the user's channel with the agent's
	// name, and its ask_user questions: when several agents answer in a
	// session, a reader there must know which one speaks. A sub-agent
	// inherits it: its answer goes to its parent only, but its questions to
	// the user.
	SignReply bool `json:"sign_reply,omitempty"`
}

type AgentWorkflowOutput struct {
	Response string `json:"response"`
	// NewMessages holds only what this run produced, not the history it was
	// given: the caller appends them. Returning the whole conversation made
	// every turn carry the full history back through Temporal, which grows
	// until it hits the payload limit.
	NewMessages  []store.Message `json:"new_messages"`
	GoalAchieved bool            `json:"goal_achieved"`
	// Error reports a turn that failed with a transcript worth keeping (the LLM
	// call gave up, for instance). The workflow returns no error in that case,
	// so NewMessages survives — a failed workflow returns no result at all.
	Error string `json:"error,omitempty"`
	// ErrorType is the type of the ApplicationError the LLM call failed with,
	// when the turn stopped on it (activity.ErrContextTooLong, for instance):
	// what a caller tests, Error being the text for the members.
	ErrorType string `json:"error_type,omitempty"`
}

// AgentWorkflow is a pure resolution workflow: ReAct loop only. It runs the
// LLM and the tools on a message, and returns the messages it produced.
func AgentWorkflow(ctx workflow.Context, input AgentWorkflowInput) (AgentWorkflowOutput, error) {
	// Capture activity → task queue mapping via SideEffect.
	// Reads from worker-cached config (no DB call). Recorded in history for deterministic replay.
	var queueMap map[string]string
	encoded := workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
		return activity.GetActivityQueues()
	})
	_ = encoded.Get(&queueMap)
	if queueMap == nil {
		queueMap = make(map[string]string)
	}

	// LLM calls: longer timeout, retry on transient errors (408, 429, 5xx, network)
	// No HeartbeatTimeout — CallLLM is a blocking HTTP call with no opportunity to heartbeat.
	llmOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 180 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        5 * time.Second,
			BackoffCoefficient:     3.0,
			MaximumInterval:        2 * time.Minute,
			MaximumAttempts:        6,
			NonRetryableErrorTypes: []string{"PermanentAPIError", activity.ErrContextTooLong},
		},
	}
	if q, ok := queueMap["CallLLM"]; ok {
		llmOpts.TaskQueue = q
	}
	llmCtx := workflow.WithActivityOptions(ctx, llmOpts)

	// Tool execution: no retry on application errors — let the LLM decide.
	// Each call is routed to its tool's task queue, and bounded by its
	// tool's timeout (see dispatch below).
	toolOpts := workflow.ActivityOptions{
		ScheduleToStartTimeout: toolScheduleToStartTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts:        1,
			NonRetryableErrorTypes: []string{"ApplicationError"},
		},
	}

	// The agent identity selects the prompt, skills and allowed tools.
	currentAgentID := input.AgentID
	if currentAgentID == "" {
		return AgentWorkflowOutput{}, temporal.NewNonRetryableApplicationError("agent_id is required", "MissingAgentID", nil)
	}
	var skillAct *activity.SkillActivities
	currentChain := append(input.AgentChain, currentAgentID)

	// turn is what this run produced, its own messages only. A session turn
	// does not hold the conversation: the LLM call loads it from references
	// in its input. Carried here, it was recorded in this workflow's history
	// with every call, and a long session ended up refused by Temporal. A run
	// that persists nothing (a sub-agent, a scheduled task) has no other
	// conversation than turn, given inline.
	var memAct *activity.MemoryActivities
	var turn []store.Message
	// persisted counts the messages of turn written to the store. Everything
	// is flushed as it is produced and also returned to the caller, which
	// appends it again. Both writes use the same keys, so the second one is a
	// no-op and either one alone is enough.
	persisted := 0

	if !input.UserMessageStored {
		contentJSON, _ := json.Marshal(input.UserMessage)
		turn = append(turn, store.Message{
			Role:    store.RoleUser,
			Content: string(contentJSON),
			UserID:  input.UserID,
			Author:  input.UserName,
		})
	}

	persistOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	}
	// flush writes the messages produced since the last call. It must only be
	// called where the transcript is valid on its own: a flushed assistant
	// message carrying tool calls whose results never landed would make the next
	// turn unreplayable by the LLM API. A failed flush is not fatal: the next
	// LLM call is given what is unwritten (TurnHistory.Tail), and the caller
	// receives NewMessages and writes them again.
	flush := func(c workflow.Context) {
		pending := turn[persisted:]
		if input.TurnKey == "" || len(pending) == 0 {
			return
		}
		if err := workflow.ExecuteActivity(
			workflow.WithActivityOptions(c, persistOpts),
			memAct.PersistContext,
			activity.PersistContextInput{
				SessionID:  input.SessionID,
				TurnKey:    input.TurnKey,
				StartIndex: persisted,
				Messages:   pending,
			},
		).Get(c, nil); err != nil {
			workflow.GetLogger(ctx).Error("Persist turn messages failed",
				"session_id", input.SessionID, "turn_key", input.TurnKey, "error", err)
			return
		}
		persisted += len(pending)
	}
	// cancelSafeFlush persists through a detached context once the workflow is
	// cancelled — a cancelled context refuses to schedule activities.
	cancelSafeFlush := func() {
		if ctx.Err() != nil {
			dctx, cancel := workflow.NewDisconnectedContext(ctx)
			defer cancel()
			flush(dctx)
			return
		}
		flush(ctx)
	}
	cancelSafeFlush()

	// Load the agent's name
	var skillsResult activity.LoadSkillsForAgentOutput
	if err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 5 * time.Second,
		}),
		skillAct.LoadSkillsForAgent,
		activity.LoadSkillsForAgentInput{AgentID: currentAgentID},
	).Get(ctx, &skillsResult); err != nil {
		if ctx.Err() != nil {
			return cancelledOutput(turn), nil
		}
		return AgentWorkflowOutput{}, fmt.Errorf("load skills: %w", err)
	}
	// The agent signs its messages: other agents answer in the same session,
	// and the interface and the next turns must tell them apart.
	agentName := skillsResult.Name
	if agentName == "" {
		agentName = currentAgentID
	}
	signed := ""
	if input.SignReply {
		signed = agentName
	}

	// Load the tools this agent may use, with the queue serving each one
	var toolAct *activity.ToolActivities
	var toolList activity.ListToolsOutput
	if err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 5 * time.Second,
		}),
		toolAct.ListTools,
		activity.ListToolsInput{AgentID: currentAgentID},
	).Get(ctx, &toolList); err != nil {
		if ctx.Err() != nil {
			return cancelledOutput(turn), nil
		}
		return AgentWorkflowOutput{}, fmt.Errorf("list tools: %w", err)
	}
	// The model is offered the tools this list dispatches, by name: the LLM
	// call reads their definitions from the catalog.
	toolNames := make([]string, len(toolList.Tools))
	for i, t := range toolList.Tools {
		toolNames[i] = t.Name
	}

	// Each LLM call builds its prompt from these references. A session turn
	// and a scheduled task write with their user's memory.
	prompt := activity.PromptRef{Override: input.SystemPrompt, UserName: input.UserName, PartNote: input.PartNote}
	if input.TurnKey != "" || input.LoadUserMemory {
		prompt.MemoryOf = input.UserID
	}
	llmRequest := func() activity.LLMTurnRequest {
		req := activity.LLMTurnRequest{Model: input.Model, AgentID: currentAgentID, UserID: input.UserID, Tools: toolNames, Prompt: prompt}
		if input.TurnKey == "" {
			req.Messages = turn
			return req
		}
		req.History = &activity.TurnHistory{
			SessionID:    input.SessionID,
			UpTo:         input.HistoryUpTo,
			EarlierTurns: input.EarlierTurns,
			TurnKey:      input.TurnKey,
			Tail:         turn[persisted:],
			TailStart:    persisted,
		}
		return req
	}

	// The answer and the tool calls go to the user's channel from the
	// session's own turn only. A sub-agent's answer is for its parent: sent to
	// a Telegram chat, it would read as the agent's reply. Its events stay on
	// the web hub, under its own ID, as before.
	replyChannel, replyChannelID := input.Channel, input.ChannelID
	if input.TurnKey == "" {
		replyChannel, replyChannelID = "", ""
	}

	// What a tool flagged NeedsCallContext receives of this run.
	call := callContext(input, currentChain, signed)

	// ReAct loop
	for i := 0; i < maxReActIterations; i++ {
		// Check for cancellation before each iteration
		if ctx.Err() != nil {
			cancelSafeFlush()
			return cancelledOutput(turn), nil
		}

		var llmAct *activity.LLMActivities
		var response activity.LLMTurnResponse
		if err := workflow.ExecuteActivity(llmCtx, llmAct.CallLLM, llmRequest()).Get(ctx, &response); err != nil {
			cancelSafeFlush()
			if ctx.Err() != nil {
				return cancelledOutput(turn), nil
			}
			// Soft failure: returning an error would discard everything the turn
			// produced, since a failed workflow carries no result.
			return AgentWorkflowOutput{
				NewMessages: turn,
				Error:       llmFailure(err),
				ErrorType:   failureType(err),
			}, nil
		}

		// The tools this answer calls act on what the model read: a memory
		// save replaces the version this call's prompt held, and none when
		// it held none.
		call.MemoryVersion, call.MemoryUnread = response.MemoryVersion, response.MemoryUnread

		// No tool calls → final response
		if len(response.ToolCalls) == 0 {
			if response.Content != "" {
				respJSON, _ := json.Marshal(response.Content)
				turn = append(turn, store.Message{
					Role:    store.RoleAssistant,
					Content: string(respJSON),
					UserID:  input.UserID,
					AgentID: currentAgentID,
					Author:  agentName,
				})
			}

			cancelSafeFlush()
			notifyResponse(ctx, input.SessionID, replyChannel, replyChannelID, signed, response.Content)

			return AgentWorkflowOutput{
				Response:     response.Content,
				NewMessages:  turn,
				GoalAchieved: response.StopReason == "end_turn",
			}, nil
		}

		// Tool calls — add assistant message
		toolCalls := make([]store.ToolCall, len(response.ToolCalls))
		for j, tc := range response.ToolCalls {
			toolCalls[j] = store.ToolCall{
				ID:    tc.ID,
				Name:  tc.Name,
				Input: tc.Input,
			}
		}
		assistantMsg := store.Message{
			Role:      store.RoleAssistant,
			ToolCalls: toolCalls,
			// The user this turn answers: its private tool blocks are
			// hidden from the agent's turns for another member.
			UserID:  input.UserID,
			AgentID: currentAgentID,
			Author:  agentName,
		}
		if response.Content != "" {
			cJSON, _ := json.Marshal(response.Content)
			assistantMsg.Content = string(cJSON)
		}
		turn = append(turn, assistantMsg)

		notifyToolCalls(ctx, input.SessionID, replyChannel, replyChannelID, response.ToolCalls, toolList.Resolutions)

		// Execute all tools in parallel, each on its tool's task queue
		type toolDispatch struct {
			future        workflow.Future
			kind          tool.ToolKind
			agent         bool // an agent_<id> tool: the child is an AgentWorkflow
			fireAndForget bool
			workflowID    string
			taskQueue     string
			unavailable   string // non-empty: error returned without dispatching
		}
		dispatches := make([]toolDispatch, len(response.ToolCalls))
		for j, tc := range response.ToolCalls {
			res, allowed := toolList.Resolutions[tc.Name]
			if !allowed {
				// Not in this agent's allowlist, or unknown (hallucinated) tool
				dispatches[j] = toolDispatch{unavailable: fmt.Sprintf("Tool %q is not available to this agent.", tc.Name)}
				continue
			}
			d := toolDispatch{kind: tool.ToolKind(res.Kind), agent: res.AgentID != "", fireAndForget: res.FireAndForget, taskQueue: res.TaskQueue}

			if d.kind == tool.ToolKindWorkflow {
				d.workflowID = childWorkflowID(input.SessionID, tc.Name, tc.ID, i, j)

				// Build input first — a sub-agent runs on the current workflow queue
				childWorkflow, childInput, err := buildChildInput(tc.Input, input, d.workflowID, &res, call, currentAgentID, workflow.GetInfo(ctx).TaskQueueName)
				if err == nil && d.agent {
					err = delegationRefusal(currentChain, res.AgentID)
				}
				if err != nil {
					dispatches[j] = toolDispatch{unavailable: err.Error()}
					continue
				}

				childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
					WorkflowID: d.workflowID,
					TaskQueue:  res.TaskQueue,
				})
				d.future = workflow.ExecuteChildWorkflow(childCtx, childWorkflow, childInput)
			} else {
				opts := toolOpts
				opts.TaskQueue = res.TaskQueue
				// The tool knows how long it may run: exec waits for a
				// command up to its own limit, which a single default would
				// cut short. Not a duration (a row edited by hand) is the
				// default: the SDK would refuse the activity.
				opts.StartToCloseTimeout = res.Timeout
				if opts.StartToCloseTimeout <= 0 {
					opts.StartToCloseTimeout = tool.DefaultTimeout
				}
				execCtx := workflow.WithActivityOptions(ctx, opts)
				execInput := activity.ExecuteToolInput{
					Name:      tc.Name,
					Input:     tc.Input,
					SessionID: input.SessionID,
					AgentID:   currentAgentID,
					UserID:    input.UserID,
				}
				if res.NeedsCallContext {
					cc := call
					execInput.Call = &cc
				}
				d.future = workflow.ExecuteActivity(execCtx, toolAct.ExecuteTool, execInput)
			}
			dispatches[j] = d
		}

		// Collect results
		for j, d := range dispatches {
			var content string
			var isError bool

			if d.unavailable != "" {
				content = d.unavailable
				isError = true
			} else if d.fireAndForget {
				// Don't wait — return the workflow ID so the LLM can query it later
				content = fmt.Sprintf("Workflow started (workflow_id: %s). Use query_workflow to check its status.", d.workflowID)
			} else if d.kind == tool.ToolKindWorkflow {
				var result json.RawMessage
				if err := d.future.Get(ctx, &result); err != nil {
					content = fmt.Sprintf("Workflow failed: %s", err.Error())
					isError = true
				} else if d.agent {
					content, isError = subAgentContent(result)
				} else {
					content, isError = tool.DecodeResult(result)
				}
			} else {
				var result activity.ExecuteToolOutput
				if err := d.future.Get(ctx, &result); err != nil {
					if isScheduleToStartTimeout(err) {
						content = fmt.Sprintf("Tool %q is unavailable: no worker is serving task queue %q.", response.ToolCalls[j].Name, d.taskQueue)
					} else {
						content = fmt.Sprintf("Tool execution failed: %s", err.Error())
					}
					isError = true
				} else {
					content = result.Content
					isError = result.IsError
				}
			}

			turn = append(turn, store.Message{
				Role: store.RoleTool,
				ToolResult: &store.ToolResult{
					ToolCallID: response.ToolCalls[j].ID,
					Content:    truncateToolResult(content),
					IsError:    isError,
				},
			})
		}

		// Flush the assistant message and its tool results together: a stored
		// tool call with no result would break the next turn.
		cancelSafeFlush()
	}

	cancelSafeFlush()
	return exhaustedOutput(turn), nil
}

// exhaustedOutput ends a run that used all its iterations without an answer.
// It carries no Response, which a parent would read as the answer to its
// task: its Error says why the run stopped.
func exhaustedOutput(newMessages []store.Message) AgentWorkflowOutput {
	return AgentWorkflowOutput{
		NewMessages: newMessages,
		Error:       fmt.Sprintf("stopped after %d iterations without a final answer", maxReActIterations),
	}
}

// cancelledOutput ends a cancelled run with what it produced, newMessages: a
// run that returns no error completes, and its session gets the transcript.
// Returning the cancellation as an error would end the run as cancelled,
// with no result at all.
func cancelledOutput(newMessages []store.Message) AgentWorkflowOutput {
	return AgentWorkflowOutput{Response: "Agent cancelled.", NewMessages: newMessages}
}

// truncateToolResult shortens an oversized tool result, keeping its head and
// its tail: the head says what the output is, the tail usually carries the error
// or the summary line. Cuts land on rune boundaries so the result stays valid
// UTF-8, which the JSON payloads downstream require.
func truncateToolResult(content string) string {
	if len(content) <= maxToolResultBytes {
		return content
	}

	head := runeStart(content, maxToolResultBytes*2/3)
	tail := runeStart(content, len(content)-(maxToolResultBytes-head))
	if tail <= head {
		tail = len(content)
	}
	return fmt.Sprintf("%s\n\n[... %d bytes omitted, output too large ...]\n\n%s",
		content[:head], tail-head, content[tail:])
}

// runeStart backs i up to the first byte of the rune it falls inside.
func runeStart(s string, i int) int {
	if i >= len(s) {
		return len(s)
	}
	for i > 0 && !utf8.RuneStart(s[i]) {
		i--
	}
	return i
}

// notifyRetry bounds the attempts at a notification. Without it the default
// policy retries for ever, and a channel that refuses a message for good would
// hold the turn that sends it.
var notifyRetry = &temporal.RetryPolicy{MaximumAttempts: 3}

// channelNotifyTimeout bounds a notification that reaches the user's channel.
// There an answer can be several messages (Telegram cuts at 4096 characters),
// each with its own timeout and retries: the bound covers them all, so that an
// attempt does not expire with part of the answer sent, to be sent again by
// the next.
const channelNotifyTimeout = time.Minute

// notifyResponse sends the agent's answer to the session's channel, signed by
// agent when it is not empty (the channel shows who speaks). A failure is
// logged, not returned: the answer is in the transcript already, and the turn
// must end.
func notifyResponse(ctx workflow.Context, sessionID, channel, channelID, agent, content string) {
	event := map[string]string{
		"type":    activity.EventMessage,
		"content": content,
	}
	if agent != "" {
		event["agent"] = agent
	}
	data, _ := json.Marshal(event)
	var notifAct *activity.NotificationActivities
	err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: channelNotifyTimeout,
			RetryPolicy:         notifyRetry,
		}),
		notifAct.NotifyStep,
		activity.NotifyInput{
			SessionID: sessionID,
			Channel:   channel,
			ChannelID: channelID,
			Event: activity.SSEEvent{
				Type: activity.EventMessage,
				Data: data,
			},
		},
	).Get(ctx, nil)
	if err != nil {
		workflow.GetLogger(ctx).Error("Answer not delivered", "session_id", sessionID, "channel", channel, "error", err)
	}
}

// buildChildInput constructs the proper input for child workflow tools, and
// the workflow to start.
// For an agent_<id> tool, it builds an AgentWorkflowInput for that agent and
// keeps the child on the current workflow queue. The target comes from the
// tool's resolution, never from the model's input: the catalog only offers the
// agents the allowlist grants, so there is no target left to validate.
// A tool published as needing the call context (ask_user) gets call added to
// its input. Any other gets the raw input.
func buildChildInput(rawInput json.RawMessage, parent AgentWorkflowInput, childID string, res *activity.ToolResolution, call tool.CallContext, currentAgentID string, currentQueue string) (childWorkflow interface{}, input interface{}, err error) {
	if res.AgentID != "" {
		return subAgentInput(rawInput, parent, childID, res, call.AgentChain, currentAgentID, currentQueue)
	}
	if res.NeedsCallContext {
		in, err := tool.WithCallContext(rawInput, call)
		if err != nil {
			return nil, nil, err
		}
		return res.WorkflowName, in, nil
	}
	return res.WorkflowName, rawInput, nil
}

// callContext is what a tool flagged NeedsCallContext receives of the run
// input: the agent chain, the user's channel, and signer, the name that signs
// on that channel (empty: unsigned). A workflow tool gets it in its input, an
// activity tool in its context (tool.CallFromContext).
func callContext(input AgentWorkflowInput, chain []string, signer string) tool.CallContext {
	return tool.CallContext{
		AgentChain: chain,
		Channel:    input.Channel,
		ChannelID:  input.ChannelID,
		Agent:      signer,
	}
}

// subAgentInput starts res.AgentID as a one-shot sub-agent: no session history,
// its own prompt, skills and allowlist, and the parent's model unless the call
// names one.
func subAgentInput(rawInput json.RawMessage, parent AgentWorkflowInput, childID string, res *activity.ToolResolution, agentChain []string, currentAgentID string, currentQueue string) (interface{}, interface{}, error) {
	var call struct {
		Task  string `json:"task"`
		Model string `json:"model,omitempty"`
	}
	if err := json.Unmarshal(rawInput, &call); err != nil {
		return nil, nil, fmt.Errorf("invalid input: %w", err)
	}
	if strings.TrimSpace(call.Task) == "" {
		return nil, nil, fmt.Errorf("task is required: describe what the agent should do")
	}
	// The catalog never offers an agent its own tool. Checked again because a
	// loop here would only burn tokens until the iteration limit.
	if res.AgentID == currentAgentID {
		return nil, nil, fmt.Errorf("an agent cannot delegate to itself: do the work in this turn")
	}

	model := call.Model
	if model == "" {
		model = parent.Model
	}

	// Sub-agents are orchestration: they run where their parent runs.
	res.TaskQueue = currentQueue

	return AgentWorkflow, AgentWorkflowInput{
		SessionID:   childID,
		AgentID:     res.AgentID,
		UserMessage: call.Task,
		Model:       model,
		AgentChain:  agentChain,
		// The sub-agent acts for the same user: its tools save that user's
		// memory, deliver to that user.
		UserID: parent.UserID,
		// And asks that user, where they are: an ask_user from a sub-agent of
		// a Telegram session goes to Telegram, signed when its parent's
		// answer is.
		Channel:   parent.Channel,
		ChannelID: parent.ChannelID,
		SignReply: parent.SignReply,
	}, nil
}

// maxDelegationDepth bounds how many levels of sub-agents run below the
// session's agent. Each level may take maxReActIterations LLM calls and has
// no run timeout: a deep chain is a cost, not a plan.
const maxDelegationDepth = 3

// delegationRefusal is why chain, the agents calling (the caller last), may
// not delegate to agentID, or nil: agentID already in the chain (A → B → A
// would only bounce the task until the iteration limits), or beyond
// maxDelegationDepth. The model gets the refusal as the tool's error.
func delegationRefusal(chain []string, agentID string) error {
	if slices.Contains(chain, agentID) {
		return fmt.Errorf("agent %q is already in the call chain (%s): delegating to it would loop; answer with what you have", agentID, strings.Join(chain, " → "))
	}
	if len(chain) > maxDelegationDepth {
		return fmt.Errorf("sub-agents are limited to %d levels: do the work in this turn", maxDelegationDepth)
	}
	return nil
}

func notifyToolCalls(ctx workflow.Context, sessionID, channel, channelID string, toolCalls []provider.ToolCallInfo, resolutions map[string]activity.ToolResolution) {
	shown := make([]provider.ToolCallInfo, len(toolCalls))
	for i, tc := range toolCalls {
		tc.Input = tool.DisplayInput(resolutions[tc.Name].PrivateInput, tc.Input)
		shown[i] = tc
	}
	data, _ := json.Marshal(map[string]interface{}{
		"type":       activity.EventToolCalls,
		"tool_calls": shown,
	})
	var notifAct *activity.NotificationActivities
	_ = workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 5 * time.Second,
			RetryPolicy:         notifyRetry,
		}),
		notifAct.NotifyStep,
		activity.NotifyInput{
			SessionID: sessionID,
			Channel:   channel,
			ChannelID: channelID,
			Event: activity.SSEEvent{
				Type: activity.EventToolCalls,
				Data: data,
			},
		},
	).Get(ctx, nil)
}

// isScheduleToStartTimeout reports whether err is an activity that no worker
// picked up in time.
func isScheduleToStartTimeout(err error) bool {
	var timeoutErr *temporal.TimeoutError
	return errors.As(err, &timeoutErr) && timeoutErr.TimeoutType() == enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START
}

// failureText is what an activity's failure says, without the envelope
// Temporal wraps it in (activity type, event IDs, the worker's pid@host): the
// members of the session read it.
func failureText(err error) string {
	var actErr *temporal.ActivityError
	if errors.As(err, &actErr) && actErr.Unwrap() != nil {
		err = actErr.Unwrap()
	}
	if appErr, ok := err.(*temporal.ApplicationError); ok {
		return appErr.Message()
	}
	return err.Error()
}

// llmFailure is why a turn stopped on its LLM call, for the members. A
// conversation too long for the model says so alone: it is not a failure of
// the call to retry, but of the session to fork.
func llmFailure(err error) string {
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) && appErr.Type() == activity.ErrContextTooLong {
		return appErr.Message()
	}
	return "call LLM: " + failureText(err)
}

// failureType is the type of the ApplicationError err carries, "" if none.
func failureType(err error) string {
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) {
		return appErr.Type()
	}
	return ""
}

// subAgentTooLong is what the parent reads of a sub-agent whose conversation
// outgrew its model: the fork advice the members read is not for it.
const subAgentTooLong = "The agent stopped without an answer: its conversation grew too long for its model. Give it a smaller task, or fewer and shorter tool outputs to read."

// subAgentContent is what the parent reads of a sub-agent's run: its final
// response, or why it stopped without one, as an error. A sub-agent is the
// parent's own workflow type, so this is the one result decoded by type;
// every other workflow tool returns a tool.Result.
func subAgentContent(result json.RawMessage) (content string, isError bool) {
	var agent AgentWorkflowOutput
	if err := json.Unmarshal(result, &agent); err == nil {
		switch {
		case agent.Response != "":
			return agent.Response, false
		case agent.ErrorType == activity.ErrContextTooLong:
			return subAgentTooLong, true
		case agent.Error != "":
			return "The agent stopped without an answer: " + agent.Error, true
		}
	}
	return string(result), false
}

// childWorkflowID names a workflow tool call "{sessionID}-tool-{tool}-{callID}".
// The prefix is relied upon (ask_user extracts the session ID from it); the tool
// call ID keeps parallel calls and later turns distinct.
func childWorkflowID(sessionID, toolName, callID string, iteration, index int) string {
	if callID == "" {
		callID = fmt.Sprintf("%d-%d", iteration, index)
	}
	return fmt.Sprintf("%s-tool-%s-%s", sessionID, toolName, callID)
}
