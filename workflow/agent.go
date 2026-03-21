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
	UserMessage  string          `json:"user_message"`
	Messages     []store.Message `json:"messages"`      // Context loaded by session
	SystemPrompt string          `json:"system_prompt"`
	Model        string          `json:"model"`
	SessionTools []string        `json:"session_tools,omitempty"` // Tools that persist through a session
	AgentChain   []string        `json:"agent_chain,omitempty"`   // Chain of parent agent names (task queues) for context propagation
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
	// LLM calls: longer timeout, retry on transient errors
	llmCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 180 * time.Second,
		HeartbeatTimeout:    60 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	})

	// Tool execution: no retry on application errors — let the LLM decide
	toolCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 120 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts:        1,
			NonRetryableErrorTypes: []string{"ApplicationError"},
		},
	})

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

	// Build the current agent chain for context propagation to sub-agents and ask_user
	currentQueue := workflow.GetInfo(ctx).TaskQueueName
	currentChain := append(input.AgentChain, currentQueue)

	// Load skills for the current task queue (unless a system prompt override is provided)
	systemPrompt := input.SystemPrompt
	if systemPrompt == "" {
		taskQueue := workflow.GetInfo(ctx).TaskQueueName
		var skillAct *activity.SkillActivities
		var skillsResult activity.LoadSkillsForQueueOutput
		if err := workflow.ExecuteActivity(
			workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 5 * time.Second,
			}),
			skillAct.LoadSkillsForQueue,
			activity.LoadSkillsForQueueInput{TaskQueue: taskQueue},
		).Get(ctx, &skillsResult); err != nil {
			return AgentWorkflowOutput{}, fmt.Errorf("load skills: %w", err)
		}
		systemPrompt = skillsResult.SystemPrompt
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
		chatMessages := convertMessages(messages)

		request := provider.ChatRequest{
			Model:     input.Model,
			System:    systemPrompt,
			Messages:  chatMessages,
			Tools:     toolList.Tools,
			MaxTokens: 4096,
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

			notifyResponse(ctx, input.SessionID, response.Content)

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

		notifyToolCalls(ctx, input.SessionID, response.ToolCalls)

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
				workflowName, childInput := buildChildInput(tc.Name, tc.Input, input, d.workflowID, &res, currentChain)

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
					Name:  tc.Name,
					Input: tc.Input,
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

func notifyResponse(ctx workflow.Context, sessionID, content string) {
	data, _ := json.Marshal(map[string]string{
		"type":    "message",
		"content": content,
	})
	var notifAct *activity.NotificationActivities
	_ = workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 5 * time.Second,
		}),
		notifAct.NotifyStep,
		activity.NotifyInput{
			SessionID: sessionID,
			Event: activity.SSEEvent{
				Type: "message",
				Data: data,
			},
		},
	).Get(ctx, nil)
}

// buildChildInput constructs the proper input for child workflow tools.
// For spawn_session, it builds an AgentWorkflowInput with the agent chain.
// For ask_user, it enriches the raw input with the agent chain.
// For other workflow tools, it passes the raw input unchanged.
func buildChildInput(toolName string, rawInput json.RawMessage, parent AgentWorkflowInput, childID string, res *activity.ToolResolution, agentChain []string) (workflowName string, input interface{}) {
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

		return res.WorkflowName, AgentWorkflowInput{
			SessionID:    childID,
			UserMessage:  spawnInput.Task,
			Model:        model,
			SessionTools: spawnInput.SessionTools,
			AgentChain:   agentChain,
		}

	case "ask_user":
		// Enrich the raw input with the agent chain for UX context
		var enriched map[string]interface{}
		json.Unmarshal(rawInput, &enriched)
		enriched["agent_chain"] = agentChain
		enrichedJSON, _ := json.Marshal(enriched)
		return res.WorkflowName, json.RawMessage(enrichedJSON)

	default:
		return res.WorkflowName, rawInput
	}
}

func notifyToolCalls(ctx workflow.Context, sessionID string, toolCalls []provider.ToolCallInfo) {
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
			Event: activity.SSEEvent{
				Type: "tool_calls",
				Data: data,
			},
		},
	).Get(ctx, nil)
}
