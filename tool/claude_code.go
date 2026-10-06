package tool

import (
	"encoding/json"
	"fmt"
	"time"
)

// The coding tools are backed by the Claude Code CLI. They are workflow-kind
// tools, not activities: a coding run lasts minutes to tens of minutes, well
// past the 120s cap on a tool activity, and the steps around the run —
// clone, cleanup — must survive a failure of the run itself. Both need the
// call's context (NeedsCallContext): whose machine may run it, and where to
// tell its user what the run waits for.

// AnalyzeRoute is where analyze_repo may run, as the worker that publishes
// it is configured: what its description tells the model.
type AnalyzeRoute struct {
	// Machines: on the user's own machine, when one is connected.
	Machines bool
	// Fallback: on the installation's coding workers otherwise.
	Fallback bool
}

// RegisterAnalyzeRepoTool registers analyze_repo, run by workflowFunc
// (CodingRunWorkflow): on the user's machine, else on the installation's
// fallback (route). It is read-only by construction: the permission mode is
// set where the run happens, on a worker or a machine, and is not part of
// this schema, so an agent cannot ask for write access.
func RegisterAnalyzeRepoTool(registry *Registry, workflowFunc interface{}, route AnalyzeRoute) {
	var where string
	switch {
	case route.Machines && route.Fallback:
		where = "It runs on the user's own machine when one is connected (agent connect), with their Claude Code login (their subscription or key) and their git access; " +
			"otherwise on the installation's coding workers, which may make it wait for a free one, paid as the installation's operator chose."
	case route.Machines:
		where = "It runs on the user's own machine (agent connect), with their Claude Code login (their subscription or key) and their git access; " +
			"when none is connected, it fails at once saying so."
	default:
		where = "It runs on the installation's coding workers, which may make it wait for a free one, paid as the installation's operator chose."
	}
	registry.Register(&Tool{
		Name: "analyze_repo",
		Description: "Read a Git repository and answer a question about it, using a coding agent that explores the code on its own. " +
			"Use it to understand an unfamiliar codebase, locate where something is implemented, review changes, or diagnose a problem. " +
			"It never modifies the repository: the clone is read-only and is deleted afterwards. " +
			"It cannot fix what it finds: changing the code is implement_feature's job, if you have that tool. " +
			"Ask a precise question — the answer comes back as a written report, and the agent cannot ask you for clarification mid-run. " +
			where,
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
				}
			},
			"required": ["repo", "task"]
		}`),
		Kind:             ToolKindWorkflow,
		WorkflowFunc:     workflowFunc,
		NeedsCallContext: true,
	})
}

// RegisterImplementFeatureTool registers implement_feature, run on the
// installation's coding workers (phase 2 moves it to machines too).
// costNote, from the worker's way of paying for runs, ends its description:
// the model weighs a run against what it costs now, not what it remembers.
// queueWait, how long a run waits for a free worker, is said too.
func RegisterImplementFeatureTool(registry *Registry, workflowFunc interface{}, costNote string, queueWait time.Duration) {
	wait := waitNote(queueWait)
	registry.Register(&Tool{
		Name: "implement_feature",
		Description: "Make a change to a Git repository and publish it as a branch, using a coding agent that writes and commits the change itself. " +
			"Use it to implement a feature, fix a bug, or carry out a refactor described in prose. " +
			"It branches from base, commits its own work, and pushes the branch — it never writes to the base branch and never opens a pull request. " +
			"Describe the outcome you want and any constraint that matters; the agent cannot ask you for clarification mid-run. " +
			"A run that produces no commit is reported as a failure. " +
			wait + withSpace(costNote),
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"repo": {
					"type": "string",
					"description": "Repository to clone and push to: a URL or a local path"
				},
				"task": {
					"type": "string",
					"description": "The change to make. Include the constraints that matter — tests to keep passing, conventions to follow, files to leave alone."
				},
				"base": {
					"type": "string",
					"description": "Branch, tag or commit to start from. Defaults to the repository's default branch."
				},
				"title": {
					"type": "string",
					"description": "A few words naming the change; used for the branch name. Defaults to the start of the task."
				},
				"max_budget_usd": {
					"type": "number",
					"description": "Stop the run once it has spent this much on API calls. It can only lower the cap the worker sets; leave unset for that cap."
				}
			},
			"required": ["repo", "task"]
		}`),
		Kind:             ToolKindWorkflow,
		Sensitive:        true,
		WorkflowFunc:     workflowFunc,
		NeedsCallContext: true,
	})
}

// waitNote tells the model that a run may wait for a worker, and how long.
func waitNote(wait time.Duration) string {
	return fmt.Sprintf("When every coding worker is busy, the run waits up to %s for one to free up before it starts; "+
		"if none does, it fails, and can be tried again later.", wait)
}

// withSpace prefixes a non-empty sentence with the space that joins it.
func withSpace(s string) string {
	if s == "" {
		return ""
	}
	return " " + s
}
