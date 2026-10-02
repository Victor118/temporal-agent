package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
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
	TurnKey    string   `json:"turn_key,omitempty"`
	AgentChain []string `json:"agent_chain,omitempty"` // Chain of parent agent IDs for context propagation
	Channel    string   `json:"channel,omitempty"`     // "web", "telegram"
	ChannelID  string   `json:"channel_id,omitempty"`  // chat_id for telegram
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
}

// AgentWorkflow is a pure resolution workflow: ReAct loop only.
// It receives messages from the session, runs LLM + tools, and returns the updated messages.
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

	// LLM calls: longer timeout, retry on transient errors (429, 529, network)
	// No HeartbeatTimeout — CallLLM is a blocking HTTP call with no opportunity to heartbeat.
	llmOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 180 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        5 * time.Second,
			BackoffCoefficient:     3.0,
			MaximumInterval:        2 * time.Minute,
			MaximumAttempts:        6,
			NonRetryableErrorTypes: []string{"PermanentAPIError"},
		},
	}
	if q, ok := queueMap["CallLLM"]; ok {
		llmOpts.TaskQueue = q
	}
	llmCtx := workflow.WithActivityOptions(ctx, llmOpts)

	// Tool execution: no retry on application errors — let the LLM decide
	toolOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 120 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts:        1,
			NonRetryableErrorTypes: []string{"ApplicationError"},
		},
	}
	// Each call is routed to its tool's task queue (see dispatch below).
	toolOpts.ScheduleToStartTimeout = toolScheduleToStartTimeout

	// The agent identity selects the prompt, skills and allowed tools.
	currentAgentID := input.AgentID
	if currentAgentID == "" {
		return AgentWorkflowOutput{}, temporal.NewNonRetryableApplicationError("agent_id is required", "MissingAgentID", nil)
	}
	var skillAct *activity.SkillActivities
	currentChain := append(input.AgentChain, currentAgentID)

	// Load the conversation this turn continues. The session used to pass it in,
	// which recorded a full copy of the history in the session workflow's event
	// history on every turn; loading it here keeps that copy inside this
	// short-lived run instead. A sub-agent has no session history: its context is
	// isolated by design, so it loads nothing.
	var memAct *activity.MemoryActivities
	var messages []store.Message
	var userMemory string
	if input.TurnKey != "" {
		var loaded activity.LoadContextOutput
		if err := workflow.ExecuteActivity(
			workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 30 * time.Second,
			}),
			memAct.LoadContext,
			activity.LoadContextInput{SessionID: input.SessionID, UserID: input.UserID},
		).Get(ctx, &loaded); err != nil {
			return AgentWorkflowOutput{}, fmt.Errorf("load context: %w", err)
		}
		messages, userMemory = loaded.Messages, loaded.UserMemory
	}

	// Everything appended from here on is this turn's output: it is flushed to
	// the store as it is produced and also returned to the caller, which appends
	// it again. Both writes use the same keys, so the second one is a no-op and
	// either one alone is enough.
	newStart := len(messages)
	persisted := 0

	if !input.UserMessageStored {
		contentJSON, _ := json.Marshal(input.UserMessage)
		messages = append(messages, store.Message{
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
	// turn unreplayable by the LLM API. A failed flush is not fatal — the caller
	// receives NewMessages and writes them again.
	flush := func(c workflow.Context) {
		pending := messages[newStart+persisted:]
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

	// Load the agent's prompt (unless overridden) and the known agents
	var skillsResult activity.LoadSkillsForAgentOutput
	if err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 5 * time.Second,
		}),
		skillAct.LoadSkillsForAgent,
		activity.LoadSkillsForAgentInput{AgentID: currentAgentID},
	).Get(ctx, &skillsResult); err != nil {
		return AgentWorkflowOutput{}, fmt.Errorf("load skills: %w", err)
	}
	systemPrompt := input.SystemPrompt
	if systemPrompt == "" {
		systemPrompt = skillsResult.SystemPrompt
	}
	// Append user memory to system prompt if available
	if userMemory != "" {
		systemPrompt += userMemorySection(input.UserName, userMemory)
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
		return AgentWorkflowOutput{}, fmt.Errorf("list tools: %w", err)
	}

	// ReAct loop
	for i := 0; i < maxReActIterations; i++ {
		// Check for cancellation before each iteration
		if ctx.Err() != nil {
			cancelSafeFlush()
			return AgentWorkflowOutput{
				Response:    "Agent cancelled.",
				NewMessages: messages[newStart:],
			}, nil
		}

		chatMessages := convertMessages(messages)

		// Mark cache breakpoints:
		// 1. System prompt (stable across iterations)
		// 2. Last tool definition (stable across iterations)
		// 3. Second-to-last message (conversation prefix, grows but stable within a turn)
		tools := toolList.Tools
		if len(tools) > 0 {
			tools[len(tools)-1].CacheBreakpoint = true
		}
		if len(chatMessages) >= 2 {
			chatMessages[len(chatMessages)-2].CacheBreakpoint = true
		}

		request := provider.ChatRequest{
			Model:       input.Model,
			System:      systemPrompt,
			Messages:    chatMessages,
			Tools:       tools,
			MaxTokens:   16384,
			CacheSystem: true,
		}

		var llmAct *activity.LLMActivities
		var response provider.ChatResponse
		if err := workflow.ExecuteActivity(llmCtx, llmAct.CallLLM, request).Get(ctx, &response); err != nil {
			cancelSafeFlush()
			if ctx.Err() != nil {
				return AgentWorkflowOutput{
					Response:    "Agent cancelled.",
					NewMessages: messages[newStart:],
				}, nil
			}
			// Soft failure: returning an error would discard everything the turn
			// produced, since a failed workflow carries no result.
			return AgentWorkflowOutput{
				NewMessages: messages[newStart:],
				Error:       fmt.Sprintf("call LLM: %s", err),
			}, nil
		}

		// No tool calls → final response
		if len(response.ToolCalls) == 0 {
			if response.Content != "" {
				respJSON, _ := json.Marshal(response.Content)
				messages = append(messages, store.Message{
					Role:    store.RoleAssistant,
					Content: string(respJSON),
				})
			}

			cancelSafeFlush()
			notifyResponse(ctx, input.SessionID, input.Channel, input.ChannelID, response.Content)

			return AgentWorkflowOutput{
				Response:     response.Content,
				NewMessages:  messages[newStart:],
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
		}
		if response.Content != "" {
			cJSON, _ := json.Marshal(response.Content)
			assistantMsg.Content = string(cJSON)
		}
		messages = append(messages, assistantMsg)

		notifyToolCalls(ctx, input.SessionID, input.Channel, input.ChannelID, response.ToolCalls)

		// Execute all tools in parallel, each on its tool's task queue
		type toolDispatch struct {
			future        workflow.Future
			kind          tool.ToolKind
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
			d := toolDispatch{kind: tool.ToolKind(res.Kind), fireAndForget: res.FireAndForget, taskQueue: res.TaskQueue}

			if d.kind == tool.ToolKindWorkflow {
				d.workflowID = childWorkflowID(input.SessionID, tc.Name, tc.ID, i, j)

				// Build input first — a sub-agent runs on the current workflow queue
				childWorkflow, childInput, err := buildChildInput(tc.Name, tc.Input, input, d.workflowID, &res, currentChain, currentAgentID, workflow.GetInfo(ctx).TaskQueueName)
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
				execCtx := workflow.WithActivityOptions(ctx, opts)
				d.future = workflow.ExecuteActivity(execCtx, toolAct.ExecuteTool, activity.ExecuteToolInput{
					Name:      tc.Name,
					Input:     tc.Input,
					SessionID: input.SessionID,
					AgentID:   currentAgentID,
					UserID:    input.UserID,
				})
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
				} else {
					content = workflowToolContent(result)
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

			messages = append(messages, store.Message{
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
	return AgentWorkflowOutput{
		Response:    "Maximum iterations reached.",
		NewMessages: messages[newStart:],
		Error:       fmt.Sprintf("stopped after %d iterations without a final answer", maxReActIterations),
	}, nil
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

// deferInterleaved moves a human message that landed between an assistant's
// tool calls and their results to after the results. Humans write to a shared
// session while the agent works, so their messages can be stored in the middle
// of a turn; the LLM API rejects a tool call not followed by its results.
func deferInterleaved(messages []store.Message) []store.Message {
	out := make([]store.Message, 0, len(messages))
	var deferred []store.Message
	pending := map[string]bool{} // tool call IDs awaiting their result
	for _, m := range messages {
		switch {
		case m.ToolResult != nil:
			delete(pending, m.ToolResult.ToolCallID)
			out = append(out, m)
		case len(pending) > 0 && m.Role == store.RoleUser:
			deferred = append(deferred, m)
			continue
		default:
			out = append(out, m)
			for _, tc := range m.ToolCalls {
				pending[tc.ID] = true
			}
		}
		if len(pending) == 0 && len(deferred) > 0 {
			out = append(out, deferred...)
			deferred = nil
		}
	}
	return append(out, deferred...)
}

func convertMessages(messages []store.Message) []provider.ChatMessage {
	messages = deferInterleaved(messages)
	result := make([]provider.ChatMessage, 0, len(messages))
	for _, msg := range messages {
		content := json.RawMessage(msg.Content)
		if len(content) == 0 {
			content = nil
		}
		switch {
		case msg.Kind == store.KindForkSummary:
			content = asForkContext(content)
		case msg.Role == store.RoleUser && msg.Author != "":
			content = withAuthor(content, msg.Author)
		}
		cm := provider.ChatMessage{
			Role:    string(msg.Role),
			Content: content,
		}
		if len(msg.ToolCalls) > 0 {
			for _, tc := range msg.ToolCalls {
				cm.ToolCalls = append(cm.ToolCalls, provider.ToolCallInfo{
					ID:    tc.ID,
					Name:  tc.Name,
					Input: tc.Input,
				})
			}
		}
		if msg.ToolResult != nil {
			cm.ToolResult = &provider.ToolResultInfo{
				ToolCallID: msg.ToolResult.ToolCallID,
				Content:    msg.ToolResult.Content,
				IsError:    msg.ToolResult.IsError,
			}
		}
		result = append(result, cm)
	}
	return result
}

// userMemorySection is the prompt section holding the memory of the user the
// turn answers. It names that user: in a shared session, the model must not
// take one member's memory for everyone's, nor save the others into it.
func userMemorySection(userName, memory string) string {
	who := "this user"
	if userName != "" {
		who = userName + ", the author of the latest message"
	}
	return "\n## User Memory\n\nThe following is what you remember about " + who +
		" from previous conversations. Use it to personalize your responses. It is private to them: do not reveal it to other participants.\n\n" +
		memory + "\n\n"
}

// asForkContext presents a fork's starting summary to the model for what it
// is: context carried over, not something a user just said.
func asForkContext(content json.RawMessage) json.RawMessage {
	var text string
	if json.Unmarshal(content, &text) != nil {
		return content
	}
	framed, _ := json.Marshal("[Context carried over from an earlier conversation this one was forked from. A summary, not a message from the user.]\n\n" + text)
	return framed
}

// withAuthor prefixes a user message with its author's name, so the model
// knows who speaks when a session has several users. Only the text sent to the
// model changes: the stored message keeps the author in its own field.
func withAuthor(content json.RawMessage, author string) json.RawMessage {
	var text string
	if json.Unmarshal(content, &text) != nil {
		return content // not plain text: leave it alone
	}
	prefixed, _ := json.Marshal("[" + author + "] " + text)
	return prefixed
}

func notifyResponse(ctx workflow.Context, sessionID, channel, channelID, content string) {
	data, _ := json.Marshal(map[string]string{
		"type":    "message",
		"content": content,
	})
	var notifAct *activity.NotificationActivities
	_ = workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
		}),
		notifAct.NotifyStep,
		activity.NotifyInput{
			SessionID: sessionID,
			Channel:   channel,
			ChannelID: channelID,
			Event: activity.SSEEvent{
				Type: "message",
				Data: data,
			},
		},
	).Get(ctx, nil)
}

// buildChildInput constructs the proper input for child workflow tools, and
// the workflow to start.
// For an agent_<id> tool, it builds an AgentWorkflowInput for that agent and
// keeps the child on the current workflow queue. The target comes from the
// tool's resolution, never from the model's input: the catalog only offers the
// agents the allowlist grants, so there is no target left to validate.
// For ask_user, it enriches the raw input with the agent chain.
// For other workflow tools, it passes the raw input unchanged.
func buildChildInput(toolName string, rawInput json.RawMessage, parent AgentWorkflowInput, childID string, res *activity.ToolResolution, agentChain []string, currentAgentID string, currentQueue string) (childWorkflow interface{}, input interface{}, err error) {
	if res.AgentID != "" {
		return subAgentInput(rawInput, parent, childID, res, agentChain, currentAgentID, currentQueue)
	}
	switch toolName {
	case "ask_user":
		// Enrich the raw input with agent chain and channel info
		var enriched map[string]interface{}
		json.Unmarshal(rawInput, &enriched)
		enriched["agent_chain"] = agentChain
		enriched["channel"] = parent.Channel
		enriched["channel_id"] = parent.ChannelID
		enrichedJSON, _ := json.Marshal(enriched)
		return res.WorkflowName, json.RawMessage(enrichedJSON), nil

	default:
		return res.WorkflowName, rawInput, nil
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
	}, nil
}

func notifyToolCalls(ctx workflow.Context, sessionID, channel, channelID string, toolCalls []provider.ToolCallInfo) {
	shown := make([]provider.ToolCallInfo, len(toolCalls))
	for i, tc := range toolCalls {
		tc.Input = tool.DisplayInput(tc.Name, tc.Input)
		shown[i] = tc
	}
	data, _ := json.Marshal(map[string]interface{}{
		"type":       "tool_calls",
		"tool_calls": shown,
	})
	var notifAct *activity.NotificationActivities
	_ = workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 5 * time.Second,
		}),
		notifAct.NotifyStep,
		activity.NotifyInput{
			SessionID: sessionID,
			Channel:   channel,
			ChannelID: channelID,
			Event: activity.SSEEvent{
				Type: "tool_calls",
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

// workflowToolContent turns a workflow tool result into tool_result content:
// a string result is used as is, an agent result gives its final response,
// anything else is passed as raw JSON.
func workflowToolContent(result json.RawMessage) string {
	var text string
	if err := json.Unmarshal(result, &text); err == nil {
		return text
	}
	var agent AgentWorkflowOutput
	if err := json.Unmarshal(result, &agent); err == nil && agent.Response != "" {
		return agent.Response
	}
	var coding ClaudeCodeOutput
	if err := json.Unmarshal(result, &coding); err == nil && (coding.Report != "" || coding.Error != "") {
		return coding.Summary()
	}
	return string(result)
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
