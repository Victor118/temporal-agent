package workflow

import (
	"encoding/json"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/tool"
)

const maxReActIterations = 50

type AgentWorkflowInput struct {
	SessionID    string          `json:"session_id"`
	UserID       string          `json:"user_id,omitempty"`
	AgentID      string          `json:"agent_id,omitempty"`    // Logical agent identity (loads its skills/prompt). If empty, the worker resolves a default from the current task queue.
	UserMessage  string          `json:"user_message"`
	Messages     []store.Message `json:"messages"`      // Context loaded by session
	UserMemory   string          `json:"user_memory,omitempty"` // Persistent user memory injected into system prompt
	SystemPrompt string          `json:"system_prompt"`
	Model        string          `json:"model"`
	SessionTools []string        `json:"session_tools,omitempty"` // Tools that persist through a session
	AgentChain   []string        `json:"agent_chain,omitempty"`   // Chain of parent agent IDs for context propagation
	Channel      string          `json:"channel,omitempty"`       // "web", "telegram"
	ChannelID    string          `json:"channel_id,omitempty"`    // chat_id for telegram
}

type AgentWorkflowOutput struct {
	Response     string          `json:"response"`
	Messages     []store.Message `json:"messages"`      // Updated messages to persist
	GoalAchieved bool            `json:"goal_achieved"`
}

// AgentWorkflow is a pure resolution workflow: ReAct loop only.
// It receives messages from the session, runs LLM + tools, and returns the updated messages.
// When SessionTools is set, a Temporal session is created to pin those tools' activities
// to a single worker (required for stateful tools like filesystem operations).
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
	if q, ok := queueMap["ExecuteTool"]; ok {
		toolOpts.TaskQueue = q
	}
	toolCtx := workflow.WithActivityOptions(ctx, toolOpts)

	// Build session tools lookup
	sessionToolSet := make(map[string]bool, len(input.SessionTools))
	for _, t := range input.SessionTools {
		sessionToolSet[t] = true
	}

	// If session tools are set, create a Temporal session to pin activities to one worker
	var sessionToolCtx workflow.Context
	if len(sessionToolSet) > 0 {
		sessCtx, err := workflow.CreateSession(ctx, &workflow.SessionOptions{
			CreationTimeout:  time.Minute,
			ExecutionTimeout: 30 * time.Minute,
		})
		if err != nil {
			return AgentWorkflowOutput{}, fmt.Errorf("create session: %w", err)
		}
		defer workflow.CompleteSession(sessCtx)

		// Apply same tool activity options on the session context
		sessionToolCtx = workflow.WithActivityOptions(sessCtx, workflow.ActivityOptions{
			StartToCloseTimeout: 120 * time.Second,
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts:        1,
				NonRetryableErrorTypes: []string{"ApplicationError"},
			},
		})
	}

	// Resolve the current agent identity. Prefer the explicit AgentID from input;
	// fall back to a lookup by current task queue (for legacy/edge cases).
	currentAgentID := input.AgentID
	var skillAct *activity.SkillActivities
	if currentAgentID == "" {
		currentQueue := workflow.GetInfo(ctx).TaskQueueName
		var resolved string
		if err := workflow.ExecuteActivity(
			workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 5 * time.Second,
			}),
			skillAct.ResolveAgentByQueue,
			currentQueue,
		).Get(ctx, &resolved); err != nil {
			return AgentWorkflowOutput{}, fmt.Errorf("resolve agent by queue: %w", err)
		}
		currentAgentID = resolved
	}
	currentChain := append(input.AgentChain, currentAgentID)

	// Load skills for the current agent (unless a system prompt override is provided)
	systemPrompt := input.SystemPrompt
	queueToAgentID := map[string]string{}
	if systemPrompt == "" || currentAgentID != "" {
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
		queueToAgentID = skillsResult.QueueToAgentID
		if systemPrompt == "" {
			systemPrompt = skillsResult.SystemPrompt
		}
	}

	// Append user memory to system prompt if available
	if input.UserMemory != "" {
		systemPrompt += "\n## User Memory\n\nThe following is what you remember about this user from previous conversations. Use it to personalize your responses.\n\n" + input.UserMemory + "\n\n"
	}

	// Load available tools
	var toolAct *activity.ToolActivities
	var toolList activity.ListToolsOutput
	if err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 5 * time.Second,
		}),
		toolAct.ListTools,
	).Get(ctx, &toolList); err != nil {
		return AgentWorkflowOutput{}, fmt.Errorf("list tools: %w", err)
	}


	// Start from the context provided by the session
	messages := make([]store.Message, len(input.Messages))
	copy(messages, input.Messages)

	// Append user message
	contentJSON, _ := json.Marshal(input.UserMessage)
	messages = append(messages, store.Message{
		Role:    store.RoleUser,
		Content: string(contentJSON),
	})

	// ReAct loop
	for i := 0; i < maxReActIterations; i++ {
		// Check for cancellation before each iteration
		if ctx.Err() != nil {
			return AgentWorkflowOutput{
				Response: "Agent cancelled.",
				Messages: messages,
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
			return AgentWorkflowOutput{}, fmt.Errorf("call LLM: %w", err)
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

			notifyResponse(ctx, input.SessionID, input.Channel, input.ChannelID, response.Content)

			return AgentWorkflowOutput{
				Response:     response.Content,
				Messages:     messages,
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

		// Resolve tool kinds to know how to dispatch each tool
		toolNames := make([]string, len(response.ToolCalls))
		for j, tc := range response.ToolCalls {
			toolNames[j] = tc.Name
		}
		var resolveResult activity.ResolveToolKindsOutput
		if err := workflow.ExecuteActivity(toolCtx, toolAct.ResolveToolKinds, activity.ResolveToolKindsInput{
			Names: toolNames,
		}).Get(ctx, &resolveResult); err != nil {
			return AgentWorkflowOutput{}, fmt.Errorf("resolve tool kinds: %w", err)
		}

		// Execute all tools in parallel, routing by kind
		type toolDispatch struct {
			future        workflow.Future
			kind          tool.ToolKind
			fireAndForget bool
			workflowID    string
		}
		dispatches := make([]toolDispatch, len(response.ToolCalls))
		for j, tc := range response.ToolCalls {
			res := resolveResult.Tools[tc.Name]
			d := toolDispatch{kind: tool.ToolKind(res.Kind), fireAndForget: res.FireAndForget}

			if d.kind == tool.ToolKindWorkflow {
				d.workflowID = fmt.Sprintf("%s-tool-%s-%d", input.SessionID, tc.Name, i)

				// Build input first — may override task queue for spawn_session
				workflowName, childInput := buildChildInput(tc.Name, tc.Input, input, d.workflowID, &res, currentChain, queueToAgentID)

				opts := workflow.ChildWorkflowOptions{
					WorkflowID: d.workflowID,
				}
				if res.TaskQueue != "" {
					opts.TaskQueue = res.TaskQueue
				}
				childCtx := workflow.WithChildOptions(ctx, opts)
				d.future = workflow.ExecuteChildWorkflow(childCtx, workflowName, childInput)
			} else {
				// Route to session worker if this tool is in session_tools
				execCtx := toolCtx
				if sessionToolSet[tc.Name] && sessionToolCtx != nil {
					execCtx = sessionToolCtx
				}
				d.future = workflow.ExecuteActivity(execCtx, toolAct.ExecuteTool, activity.ExecuteToolInput{
					Name:      tc.Name,
					Input:     tc.Input,
					SessionID: input.SessionID,
				})
			}
			dispatches[j] = d
		}

		// Collect results
		for j, d := range dispatches {
			var content string
			var isError bool

			if d.fireAndForget {
				// Don't wait — return the workflow ID so the LLM can query it later
				content = fmt.Sprintf("Workflow started (workflow_id: %s). Use query_workflow to check its status.", d.workflowID)
			} else if d.kind == tool.ToolKindWorkflow {
				// Workflow tools return their result as a string
				var result string
				if err := d.future.Get(ctx, &result); err != nil {
					content = fmt.Sprintf("Workflow failed: %s", err.Error())
					isError = true
				} else {
					content = result
				}
			} else {
				var result activity.ExecuteToolOutput
				if err := d.future.Get(ctx, &result); err != nil {
					content = fmt.Sprintf("Tool execution failed: %s", err.Error())
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
					Content:    content,
					IsError:    isError,
				},
			})
		}

	}

	return AgentWorkflowOutput{
		Response: "Maximum iterations reached.",
		Messages: messages,
	}, nil
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
// For spawn_session, it builds an AgentWorkflowInput with the agent chain and
// resolves the target task queue to an agent_id via queueToAgentID.
// For ask_user, it enriches the raw input with the agent chain.
// For other workflow tools, it passes the raw input unchanged.
func buildChildInput(toolName string, rawInput json.RawMessage, parent AgentWorkflowInput, childID string, res *activity.ToolResolution, agentChain []string, queueToAgentID map[string]string) (workflowName string, input interface{}) {
	switch toolName {
	case "spawn_session":
		var spawnInput struct {
			Task         string   `json:"task"`
			TaskQueue    string   `json:"task_queue,omitempty"`
			SessionTools []string `json:"session_tools"`
			Model        string   `json:"model,omitempty"`
		}
		json.Unmarshal(rawInput, &spawnInput)

		model := spawnInput.Model
		if model == "" {
			model = parent.Model
		}

		// Override the task queue if specified by the LLM
		if spawnInput.TaskQueue != "" {
			res.TaskQueue = spawnInput.TaskQueue
		}

		// Resolve target agent_id from the chosen queue (Phase 1: spawn_session API
		// still takes task_queue; we map it to agent_id internally).
		childAgentID := queueToAgentID[res.TaskQueue]

		return res.WorkflowName, AgentWorkflowInput{
			SessionID:    childID,
			AgentID:      childAgentID,
			UserMessage:  spawnInput.Task,
			Model:        model,
			SessionTools: spawnInput.SessionTools,
			AgentChain:   agentChain,
		}

	case "ask_user":
		// Enrich the raw input with agent chain and channel info
		var enriched map[string]interface{}
		json.Unmarshal(rawInput, &enriched)
		enriched["agent_chain"] = agentChain
		enriched["channel"] = parent.Channel
		enriched["channel_id"] = parent.ChannelID
		enrichedJSON, _ := json.Marshal(enriched)
		return res.WorkflowName, json.RawMessage(enrichedJSON)

	default:
		return res.WorkflowName, rawInput
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
