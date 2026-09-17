package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
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
	SessionID    string   `json:"session_id"`
	UserID       string   `json:"user_id,omitempty"`
	AgentID      string   `json:"agent_id"` // Required. Logical agent identity: prompt, skills and allowed tools
	UserMessage  string   `json:"user_message"`
	SystemPrompt string   `json:"system_prompt"`
	Model        string   `json:"model"`                   // Explicit model; empty = the worker's default (LLM_MODEL)
	SessionTools []string `json:"session_tools,omitempty"` // Tools that persist through a session
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
// When SessionTools is set, a Temporal session is created on the queue serving those
// tools to pin their activities to a single worker (required for stateful tools like
// filesystem operations).
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

	contentJSON, _ := json.Marshal(input.UserMessage)
	messages = append(messages, store.Message{
		Role:    store.RoleUser,
		Content: string(contentJSON),
	})

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
	knownAgents := make(map[string]bool, len(skillsResult.AgentIDs))
	for _, id := range skillsResult.AgentIDs {
		knownAgents[id] = true
	}

	// Append user memory to system prompt if available
	if userMemory != "" {
		systemPrompt += "\n## User Memory\n\nThe following is what you remember about this user from previous conversations. Use it to personalize your responses.\n\n" + userMemory + "\n\n"
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

	// Stateful tools: pin their calls to one worker of their queue with a session
	sessionToolSet := make(map[string]bool, len(input.SessionTools))
	var sessionToolCtx workflow.Context
	if len(input.SessionTools) > 0 {
		queue, err := sessionToolsQueue(input.SessionTools, toolList.Resolutions)
		if err != nil {
			return AgentWorkflowOutput{}, temporal.NewNonRetryableApplicationError(err.Error(), "InvalidSessionTools", nil)
		}
		sessCtx, err := workflow.CreateSession(
			workflow.WithActivityOptions(ctx, workflow.ActivityOptions{TaskQueue: queue}),
			&workflow.SessionOptions{
				CreationTimeout:  time.Minute,
				ExecutionTimeout: 30 * time.Minute,
			})
		if err != nil {
			return AgentWorkflowOutput{}, fmt.Errorf("create session on %q: %w", queue, err)
		}
		defer workflow.CompleteSession(sessCtx)

		for _, t := range input.SessionTools {
			sessionToolSet[t] = true
		}
		sessionToolCtx = workflow.WithActivityOptions(sessCtx, toolOpts)
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

				// Build input first — spawn_session runs on the current workflow queue
				workflowName, childInput, err := buildChildInput(tc.Name, tc.Input, input, d.workflowID, &res, currentChain, currentAgentID, knownAgents, workflow.GetInfo(ctx).TaskQueueName)
				if err != nil {
					dispatches[j] = toolDispatch{unavailable: err.Error()}
					continue
				}

				childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
					WorkflowID: d.workflowID,
					TaskQueue:  res.TaskQueue,
				})
				d.future = workflow.ExecuteChildWorkflow(childCtx, workflowName, childInput)
			} else {
				opts := toolOpts
				opts.TaskQueue = res.TaskQueue
				execCtx := workflow.WithActivityOptions(ctx, opts)
				// Route to session worker if this tool is in session_tools
				if sessionToolSet[tc.Name] && sessionToolCtx != nil {
					execCtx = sessionToolCtx
				}
				d.future = workflow.ExecuteActivity(execCtx, toolAct.ExecuteTool, activity.ExecuteToolInput{
					Name:      tc.Name,
					Input:     tc.Input,
					SessionID: input.SessionID,
					AgentID:   currentAgentID,
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

func convertMessages(messages []store.Message) []provider.ChatMessage {
	result := make([]provider.ChatMessage, 0, len(messages))
	for _, msg := range messages {
		content := json.RawMessage(msg.Content)
		if len(content) == 0 {
			content = nil
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

// buildChildInput constructs the proper input for child workflow tools.
// For spawn_session, it builds an AgentWorkflowInput for the target agent (the
// current one if none is given) and keeps the child on the current workflow queue;
// an unknown agent is an error reported to the LLM.
// For ask_user, it enriches the raw input with the agent chain.
// For other workflow tools, it passes the raw input unchanged.
func buildChildInput(toolName string, rawInput json.RawMessage, parent AgentWorkflowInput, childID string, res *activity.ToolResolution, agentChain []string, currentAgentID string, knownAgents map[string]bool, currentQueue string) (workflowName string, input interface{}, err error) {
	switch toolName {
	case "spawn_session":
		var spawnInput struct {
			Task         string   `json:"task"`
			AgentID      string   `json:"agent_id,omitempty"`
			SessionTools []string `json:"session_tools"`
			Model        string   `json:"model,omitempty"`
		}
		if err := json.Unmarshal(rawInput, &spawnInput); err != nil {
			return "", nil, fmt.Errorf("invalid spawn_session input: %w", err)
		}

		childAgentID := spawnInput.AgentID
		if childAgentID == "" {
			childAgentID = currentAgentID
		}
		if !knownAgents[childAgentID] {
			return "", nil, fmt.Errorf("unknown agent_id %q: use an agent from the agents directory", childAgentID)
		}

		model := spawnInput.Model
		if model == "" {
			model = parent.Model
		}

		// Sub-agents are orchestration: they run where their parent runs.
		res.TaskQueue = currentQueue

		return res.WorkflowName, AgentWorkflowInput{
			SessionID:    childID,
			AgentID:      childAgentID,
			UserMessage:  spawnInput.Task,
			Model:        model,
			SessionTools: spawnInput.SessionTools,
			AgentChain:   agentChain,
		}, nil

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

func notifyToolCalls(ctx workflow.Context, sessionID, channel, channelID string, toolCalls []provider.ToolCallInfo) {
	data, _ := json.Marshal(map[string]interface{}{
		"type":       "tool_calls",
		"tool_calls": toolCalls,
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

// sessionToolsQueue checks that the session tools are allowed activity tools
// served by a single task queue, and returns that queue.
func sessionToolsQueue(names []string, resolutions map[string]activity.ToolResolution) (string, error) {
	queue := ""
	for _, name := range names {
		res, ok := resolutions[name]
		if !ok {
			return "", fmt.Errorf("session tool %q is not available to this agent", name)
		}
		if tool.ToolKind(res.Kind) != tool.ToolKindActivity {
			return "", fmt.Errorf("session tool %q is not an activity tool", name)
		}
		if queue != "" && res.TaskQueue != queue {
			return "", fmt.Errorf("session tools must share one task queue: %q is on %q, not %q", name, res.TaskQueue, queue)
		}
		queue = res.TaskQueue
	}
	return queue, nil
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
