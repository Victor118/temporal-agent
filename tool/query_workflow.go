package tool

import (
	"context"
	"encoding/json"
	"fmt"

	"go.temporal.io/sdk/client"
)

// RegisterQueryWorkflowTool registers a tool that queries the state of a running
// workflow by its ID. Useful to check on fire-and-forget workflows.
func RegisterQueryWorkflowTool(registry *Registry, temporalClient client.Client) {
	registry.Register(&Tool{
		Name:        "query_workflow",
		Description: "Query the current state of a running workflow by its ID. Use this to check the status or progress of a previously launched fire-and-forget workflow.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"workflow_id": {
					"type": "string",
					"description": "The workflow ID to query"
				},
				"query_name": {
					"type": "string",
					"description": "The query handler name (default: session-state)"
				}
			},
			"required": ["workflow_id"]
		}`),
		Kind: ToolKindActivity,
		Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
			var params struct {
				WorkflowID string `json:"workflow_id"`
				QueryName  string `json:"query_name"`
			}
			if err := json.Unmarshal(input, &params); err != nil {
				return "", fmt.Errorf("parse input: %w", err)
			}
			if params.QueryName == "" {
				params.QueryName = "session-state"
			}

			resp, err := temporalClient.QueryWorkflow(ctx, params.WorkflowID, "", params.QueryName)
			if err != nil {
				return fmt.Sprintf("Query failed: %s", err.Error()), nil
			}

			var result json.RawMessage
			if err := resp.Get(&result); err != nil {
				return fmt.Sprintf("Failed to decode query result: %s", err.Error()), nil
			}

			return string(result), nil
		},
	})
}
