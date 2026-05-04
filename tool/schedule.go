package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/victor/temporal-agent/store"
)

// RegisterScheduleTools registers schedule_task, list_schedules, and cancel_schedule tools.
// These tools allow the LLM to create, list, and cancel Temporal Schedules.
func RegisterScheduleTools(registry *Registry, temporalClient client.Client, st store.Store, scheduledWorkflowFunc interface{}, taskQueue string) {
	registerScheduleTask(registry, temporalClient, st, scheduledWorkflowFunc, taskQueue)
	registerListSchedules(registry, temporalClient, st)
	registerCancelSchedule(registry, temporalClient, st)
}

func registerScheduleTask(registry *Registry, temporalClient client.Client, st store.Store, scheduledWorkflowFunc interface{}, taskQueue string) {
	registry.Register(&Tool{
		Name:        "schedule_task",
		Description: "Schedule a recurring or one-time task. The agent will execute the prompt at the specified time and deliver the result via the chosen channel.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"cron": {
					"type": "string",
					"description": "Cron expression for recurring tasks (e.g. '0 9 * * *' for every day at 9am). Leave empty for one-shot tasks."
				},
				"delay": {
					"type": "string",
					"description": "For one-shot tasks: delay before execution (e.g. '2h', '30m'). Ignored if cron is set."
				},
				"prompt": {
					"type": "string",
					"description": "What the agent should do when triggered"
				},
				"delivery_channel": {
					"type": "string",
					"enum": ["app_notification"],
					"description": "How to deliver the result (default: app_notification)"
				},
				"description": {
					"type": "string",
					"description": "Human-readable description of the scheduled task"
				}
			},
			"required": ["prompt", "description"]
		}`),
		Kind: ToolKindActivity,
		Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
			var params struct {
				Cron            string `json:"cron"`
				Delay           string `json:"delay"`
				Prompt          string `json:"prompt"`
				DeliveryChannel string `json:"delivery_channel"`
				Description     string `json:"description"`
			}
			if err := json.Unmarshal(input, &params); err != nil {
				return "", fmt.Errorf("parse input: %w", err)
			}

			if params.DeliveryChannel == "" {
				params.DeliveryChannel = "app_notification"
			}

			scheduleID := fmt.Sprintf("schedule-%s-%d", slugify(params.Description), time.Now().UnixMilli())

			spec := client.ScheduleSpec{}
			if params.Cron != "" {
				spec.CronExpressions = []string{params.Cron}
			} else if params.Delay != "" {
				delay, err := time.ParseDuration(params.Delay)
				if err != nil {
					return fmt.Sprintf("Invalid delay format %q: %s", params.Delay, err.Error()), nil
				}
				// StartAt gates when the schedule becomes active,
				// Intervals provides the actual trigger mechanism,
				// EndAt prevents repeated firings.
				now := time.Now()
				spec.StartAt = now.Add(delay)
				spec.EndAt = now.Add(delay).Add(2 * time.Minute)
				spec.Intervals = []client.ScheduleIntervalSpec{
					{Every: 1 * time.Minute},
				}
			}

			// Resolve the user who owns this session
			var userID string
			if sid := SessionIDFromContext(ctx); sid != "" {
				if uid, err := st.GetSessionUser(ctx, sid); err == nil {
					userID = uid
				}
			}

			workflowInput := ScheduledAgentInput{
				Prompt:          params.Prompt,
				DeliveryChannel: params.DeliveryChannel,
				ScheduleID:      scheduleID,
				UserID:          userID,
			}

			// For one-shot tasks, limit to a single action
			remainingActions := 0
			if params.Cron == "" && params.Delay != "" {
				remainingActions = 1
			}

			_, err := temporalClient.ScheduleClient().Create(ctx, client.ScheduleOptions{
				ID:               scheduleID,
				Spec:             spec,
				RemainingActions: remainingActions,
				Action: &client.ScheduleWorkflowAction{
					ID:        fmt.Sprintf("cron-%s", scheduleID),
					Workflow:  scheduledWorkflowFunc,
					TaskQueue: taskQueue,
					Args:      []interface{}{workflowInput},
				},
			})
			if err != nil {
				return fmt.Sprintf("Failed to create schedule: %s", err.Error()), nil
			}

			st.SaveTaskLog(ctx, store.TaskLog{
				ScheduleID:  scheduleID,
				Type:        "schedule",
				Description: params.Description,
				Cron:        params.Cron,
				Delay:       params.Delay,
				Prompt:      params.Prompt,
				Channel:     params.DeliveryChannel,
				Status:      "scheduled",
			})

			if params.Cron != "" {
				return fmt.Sprintf("Scheduled recurring task: %s (cron: %s, schedule_id: %s)", params.Description, params.Cron, scheduleID), nil
			}
			if params.Delay != "" {
				return fmt.Sprintf("Scheduled one-shot task: %s (in %s, schedule_id: %s)", params.Description, params.Delay, scheduleID), nil
			}
			return fmt.Sprintf("Scheduled task: %s (schedule_id: %s)", params.Description, scheduleID), nil
		},
	})
}

func registerListSchedules(registry *Registry, temporalClient client.Client, st store.Store) {
	registry.Register(&Tool{
		Name:        "list_schedules",
		Description: "List all scheduled tasks (recurring and one-shot).",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {}
		}`),
		Kind: ToolKindActivity,
		Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
			logs, err := st.ListTaskLogs(ctx)
			if err != nil {
				return fmt.Sprintf("Failed to list schedules: %s", err.Error()), nil
			}

			if len(logs) == 0 {
				return "No scheduled tasks.", nil
			}

			var sb strings.Builder
			for _, l := range logs {
				sb.WriteString(fmt.Sprintf("- %s (id: %s, status: %s", l.Description, l.ScheduleID, l.Status))
				if l.Cron != "" {
					sb.WriteString(fmt.Sprintf(", cron: %s", l.Cron))
				}
				if l.Delay != "" {
					sb.WriteString(fmt.Sprintf(", delay: %s", l.Delay))
				}
				if l.Channel != "" {
					sb.WriteString(fmt.Sprintf(", channel: %s", l.Channel))
				}
				sb.WriteString(")\n")
			}
			return sb.String(), nil
		},
	})
}

func registerCancelSchedule(registry *Registry, temporalClient client.Client, st store.Store) {
	registry.Register(&Tool{
		Name:        "cancel_schedule",
		Description: "Cancel a scheduled task by its schedule ID.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"schedule_id": {
					"type": "string",
					"description": "The schedule ID to cancel"
				}
			},
			"required": ["schedule_id"]
		}`),
		Kind: ToolKindActivity,
		Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
			var params struct {
				ScheduleID string `json:"schedule_id"`
			}
			if err := json.Unmarshal(input, &params); err != nil {
				return "", fmt.Errorf("parse input: %w", err)
			}

			handle := temporalClient.ScheduleClient().GetHandle(ctx, params.ScheduleID)
			if err := handle.Delete(ctx); err != nil {
				return fmt.Sprintf("Failed to cancel schedule: %s", err.Error()), nil
			}

			st.UpdateTaskLogStatus(ctx, params.ScheduleID, "cancelled")

			return fmt.Sprintf("Schedule %s cancelled.", params.ScheduleID), nil
		},
	})
}

// ScheduledAgentInput is the input for the ScheduledAgentWorkflow.
type ScheduledAgentInput struct {
	Prompt          string `json:"prompt"`
	UserID          string `json:"user_id,omitempty"`
	DeliveryChannel string `json:"delivery_channel"`
	ScheduleID      string `json:"schedule_id"`
}

var nonAlphaNum = regexp.MustCompile(`[^a-z0-9]+`)

func slugify(s string) string {
	s = strings.ToLower(s)
	s = nonAlphaNum.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")
	if len(s) > 40 {
		s = s[:40]
	}
	return s
}
