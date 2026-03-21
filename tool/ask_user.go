package tool

import "encoding/json"

// RegisterAskUserTool registers the ask_user tool that poses a question to the
// user and waits for their answer. It runs as a child workflow so it blocks
// the agent's ReAct loop until the user responds (or times out).
func RegisterAskUserTool(registry *Registry, askUserWorkflowFunc interface{}) {
	registry.Register(&Tool{
		Name:        "ask_user",
		Description: "Ask the user a question and wait for their response. Use this when you need clarification, confirmation, or any input from the user before you can proceed.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"question": {
					"type": "string",
					"description": "The question to ask the user"
				}
			},
			"required": ["question"]
		}`),
		Kind:         ToolKindWorkflow,
		WorkflowFunc: askUserWorkflowFunc,
	})
}
