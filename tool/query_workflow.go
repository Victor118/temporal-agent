package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"go.temporal.io/sdk/converter"
)

// WorkflowQuerier is what query_workflow needs of the Temporal client.
type WorkflowQuerier interface {
	QueryWorkflow(ctx context.Context, workflowID, runID, queryType string, args ...interface{}) (converter.EncodedValue, error)
}

// RegisterQueryWorkflowTool registers a tool that queries the state of a running
// workflow by its ID. Useful to check on fire-and-forget workflows.
func RegisterQueryWorkflowTool(registry *Registry, temporalClient WorkflowQuerier) {
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
					"description": "The query handler name (default: state, a participant's state)"
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
			if !ownWorkflow(SessionIDFromContext(ctx), params.WorkflowID) {
				return fmt.Sprintf("Workflow %s is not one of this session's.", params.WorkflowID), nil
			}
			if params.QueryName == "" {
				params.QueryName = "state"
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

// ownWorkflow reports whether workflowID belongs to the calling session:
// every workflow of a session (its participants, their turns, their tools)
// has an ID starting with the session's and ':'. The caller's own ID may be a
// sub-agent's, itself such an ID: its session is what precedes its first
// ':'. Workflows of other sessions, other users' among them, are out of
// reach.
func ownWorkflow(sessionID, workflowID string) bool {
	session, _, _ := strings.Cut(sessionID, ":")
	return session != "" && strings.HasPrefix(workflowID, session+":")
}
