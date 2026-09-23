package tool

import "encoding/json"

// RegisterClaudeCodeTools registers the coding tools backed by the Claude Code
// CLI. They are workflow-kind tools, not activities: a coding run lasts minutes
// to tens of minutes, well past the 120s cap on a tool activity, and the steps
// around the run — clone, cleanup — must survive a failure of the run itself.
//
// analyze_repo is read-only by construction. The permission mode is set by the
// workflow and is not part of this schema, so an agent cannot ask for write
// access: writing is a different tool, with a different workflow behind it.
func RegisterClaudeCodeTools(registry *Registry, analyzeWorkflowFunc interface{}) {
	registry.Register(&Tool{
		Name: "analyze_repo",
		Description: "Read a Git repository and answer a question about it, using a coding agent that explores the code on its own. " +
			"Use it to understand an unfamiliar codebase, locate where something is implemented, review changes, or diagnose a problem. " +
			"It never modifies the repository: the clone is read-only and is deleted afterwards. " +
			"Ask a precise question — the answer comes back as a written report, and the agent cannot ask you for clarification mid-run.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"repo": {
					"type": "string",
					"description": "Repository to clone: a URL or a local path"
				},
				"task": {
					"type": "string",
					"description": "What to find out. Be specific about what the report should contain — this is the only instruction the coding agent gets."
				},
				"ref": {
					"type": "string",
					"description": "Branch, tag or commit to analyze. Defaults to the repository's default branch."
				},
				"model": {
					"type": "string",
					"description": "Optional model override for the coding agent"
				}
			},
			"required": ["repo", "task"]
		}`),
		Kind:         ToolKindWorkflow,
		WorkflowFunc: analyzeWorkflowFunc,
	})
}
