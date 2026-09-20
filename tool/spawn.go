package tool

import "encoding/json"

// RegisterSpawnTool registers the spawn_session tool that launches a sub-agent.
// The schema published here is agent-agnostic: activity.Catalog.AllowedTools pins
// agent_id to an enum of the legal targets when it builds an agent's tool list.
// With session_tools empty: lightweight one-shot AgentWorkflow (no persistence).
// With session_tools set: same AgentWorkflow but tools in the list persist through a session.
func RegisterSpawnTool(registry *Registry, agentWorkflowFunc interface{}) {
	registry.Register(&Tool{
		Name:        "spawn_session",
		Description: "Delegate a task to another agent, which handles it autonomously and returns its final answer. Use agent_id to name the agent to delegate to. If session_tools is empty, runs a one-shot agent. If session_tools lists tool names, those tools persist through a session.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"task": {
					"type": "string",
					"description": "The task description for the sub-agent to accomplish"
				},
				"agent_id": {
					"type": "string",
					"description": "ID of the agent to delegate to. Must be one of the IDs listed in the agents directory of your system prompt — an agent cannot delegate to itself. Each agent has its own system prompt, skills, and tools."
				},
				"session_tools": {
					"type": "array",
					"items": { "type": "string" },
					"description": "Tool names that persist through a session, all served by the same worker (e.g. read_file, write_file, exec). Leave empty for a lightweight one-shot agent."
				},
				"model": {
					"type": "string",
					"description": "Optional model override for the sub-agent"
				}
			},
			"required": ["agent_id", "task"]
		}`),
		Kind:         ToolKindWorkflow,
		WorkflowFunc: agentWorkflowFunc,
	})
}
