package machine

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Coding runs on a machine: analyze_repo (phase 1), implement_feature
// (phase 2).
const (
	// CapClaudeCode: the machine has the claude CLI and a login it can use.
	CapClaudeCode = "claude-code"
	// CapGitPush: the machine's owner lets its runs push, with their git
	// identity (agent connect --allow-push). Without it, an implementation
	// goes elsewhere: its branch must be pushed, or it is lost.
	CapGitPush = "git-push"
	// CapRunSkills: the machine's CLI loads a run's skills (it has
	// --plugin-dir, read in its --help at agent connect's start). Announced
	// with CapClaudeCode, required by nothing: a run with skills on a
	// machine without it is refused there, and goes elsewhere.
	CapRunSkills = "run-skills"
	// KindAnalyzeRepo is a read-only analysis of a repository, run by the
	// machine's own CLI, with its owner's subscription or key.
	KindAnalyzeRepo = "analyze_repo"
	// KindImplementFeature is a change to a repository, committed by the
	// machine's own CLI on a branch the machine then pushes with its
	// owner's git identity.
	KindImplementFeature = "implement_feature"
)

// CapabilityOf is the capability a directive's kind needs first: the one
// whose executor runs it.
func CapabilityOf(kind string) string {
	switch kind {
	case KindAnalyzeRepo, KindImplementFeature:
		return CapClaudeCode
	}
	return kind
}

// CapabilitiesOf are all the capabilities a directive's kind needs: an
// implementation pushes.
func CapabilitiesOf(kind string) []string {
	if kind == KindImplementFeature {
		return []string{CapClaudeCode, CapGitPush}
	}
	return []string{CapabilityOf(kind)}
}

// What keeps an analysis read-only, on a worker (AnalyzeRepoWorkflow) as on
// a machine. Never taken from a directive: a machine applies them itself,
// whatever the server says, and so does the workflow, whatever the model
// asks.
const (
	AnalyzePermissionMode = "plan"
	// AnalyzeSystemPrompt tells the run what it is: the CLI otherwise
	// behaves as in an interactive session, and ends a report offering to
	// make the changes or asking what to do next — which no one will
	// answer, and which the agent reading the report repeats to its user as
	// a promise.
	AnalyzeSystemPrompt = `This is a one-shot, read-only analysis. Nothing you change is kept: the clone is deleted when you finish, and nothing can be committed or pushed.
Your final message is a report read by another agent, not by a person, and no one will reply to it. End with the report: do not offer to make changes, to start on fixes, or to continue, and do not ask questions.
If a command you need is refused, say that it was refused, not that a tool is missing.`
)

// What an implementation may do, on a worker (ImplementFeatureWorkflow) as on
// a machine: set there, never taken from a directive nor from the model.
const (
	// ImplementPermissionMode auto-accepts edits. It does not cover Bash,
	// which is why the git commands of ImplementAllowedTools are named:
	// without them the run edits files and its commit is denied, leaving
	// changes that die with the clone.
	ImplementPermissionMode = "acceptEdits"
	// ImplementSystemPrompt tells the run what it is, and what happens to
	// its work: the commits on its branch are published, nothing else.
	ImplementSystemPrompt = `This is a one-shot run on a fresh clone, on the branch it was given. Commit your work on that branch yourself (git add, git commit): once you finish, the commits on it are published as that branch, and anything left uncommitted is lost with the clone.
Do not push, do not switch to another branch, and do not change the repository's git configuration: a run that does is not published.
Your final message is a report read by another agent, not by a person, and no one will reply to it. Say what you changed and what you could not do; do not ask questions.`
	// BranchPrefix starts every branch a run publishes: never the base
	// branch, never a branch a person works on.
	BranchPrefix = "agent/"
)

// ImplementAllowedTools are the git commands the run needs to commit its own
// work: no more. git log, diff and show are left out on purpose: their
// --output=<file> writes any file the run's user can write (a machine's
// owner's ~/.ssh/authorized_keys), and --ext-diff runs a program; the run
// reads the tree with its file tools. What remains writes no file of the
// caller's choosing (commit's -F, --template and add's or commit's
// --pathspec-from-file only read one) and runs no program but those of the
// configuration, which RunGitEnv neutralizes. Splitting the changes and
// writing the messages is the part worth paying a coding agent for;
// publishing them is the workflow's, or the machine's, job.
var ImplementAllowedTools = []string{
	"Bash(git add:*)",
	"Bash(git commit:*)",
	"Bash(git status:*)",
}

// ImplementDeniedTools keeps publishing out of the run's hands, and the
// clone's git directory out of its edits (its configuration and hooks are
// commands git would run). A guard rail, not a wall: a run that can execute
// commands can reach a credential by other means. What bounds the damage is
// the credential's own scope, and on a machine, its owner's --repos and
// --allow-push.
var ImplementDeniedTools = []string{
	"Bash(git push:*)",
	"Bash(git remote:*)",
	"Bash(git config:*)",
	"Edit(.git/**)",
}

// runGitConfig is the git configuration every git a run's CLI starts reads
// last, over the clone's (RunGitEnv): what would make git add, commit or
// status — or the CLI's own git calls — run a program. A run in acceptEdits
// could otherwise write .git/config or .git/hooks and have it run at its
// next commit, before anything restores the configuration.
var runGitConfig = [][2]string{
	{"core.hooksPath", "/dev/null"},
	{"core.fsmonitor", "false"},
	{"core.editor", "true"},
	{"sequence.editor", "true"},
	{"core.pager", "cat"},
	// Empty: a diff that would run an external program fails instead.
	{"diff.external", ""},
	{"interactive.diffFilter", "cat"},
	{"commit.gpgSign", "false"},
	{"tag.gpgSign", "false"},
	{"gpg.program", "false"},
	{"gpg.openpgp.program", "false"},
	{"gpg.ssh.program", "false"},
	{"gpg.x509.program", "false"},
	{"gpg.ssh.defaultKeyCommand", "false"},
	{"submodule.recurse", "false"},
	{"status.submoduleSummary", "false"},
}

// RunGitEnv is the environment that gives a run's git runGitConfig
// (GIT_CONFIG_COUNT, GIT_CONFIG_KEY_n, GIT_CONFIG_VALUE_n: they win over
// every configuration file). What it cannot cover are the drivers a
// configuration names freely (filter.<driver>.clean, trailer.<token>.cmd):
// the clone's own configuration cannot be edited by the run
// (ImplementDeniedTools), and its owner's is theirs.
func RunGitEnv() []string {
	env := []string{fmt.Sprintf("GIT_CONFIG_COUNT=%d", len(runGitConfig))}
	for i, kv := range runGitConfig {
		env = append(env, fmt.Sprintf("GIT_CONFIG_KEY_%d=%s", i, kv[0]), fmt.Sprintf("GIT_CONFIG_VALUE_%d=%s", i, kv[1]))
	}
	return env
}

// The outputs of an implementation: a directory next to the clone, never in
// it, where the CLI may leave files for the user (a report, a diagram, a
// patch), added to its working directories (--add-dir: acceptEdits writes
// there). They are published to the session's turn after the run (on a
// machine through PUT /machines/files, on a worker through its file store),
// regular files only, links refused, within these bounds. An analysis has
// none: its plan mode refuses every write, before any permission rule.
const (
	// OutputsDir names it on a machine, next to the clone in the run's
	// directory.
	OutputsDir = "outputs"
	// MaxOutputFiles is how many are published at most, MaxOutputDepth how
	// deep they are looked for, MaxOutputEntries how many entries are read
	// at most (directories included).
	MaxOutputFiles   = 20
	MaxOutputDepth   = 4
	MaxOutputEntries = 200
)

// OutputsPrompt tells a run where it may leave files for the user. Its
// directory is the one the run was given (machine or worker), not the
// model's choice.
func OutputsPrompt(dir string) string {
	return fmt.Sprintf("To hand files back to the user (a report, a diagram, a patch, data), write them in %s, not in the repository: "+
		"once you finish, the files there (at most %d, regular files, no links) are published to the user's session, by their name. "+
		"Nothing else you write outside the repository is kept.", dir, MaxOutputFiles)
}

// What keeps an implementation's commits when they cannot be pushed, on a
// worker as on a machine. Before the CLI, a git push --dry-run of the base to
// the run's branch, as the real push would go (its identity, its options):
// it reaches the remote and authenticates as a push does (receive-pack), so
// a run whose work could not be published is refused before anything is
// paid. It runs no hook of the server's and checks no branch protection:
// what those refuse is only known at the push. A push that fails anyway
// publishes the commits as a git bundle of the branch (BundleName), for the
// user to push themselves. PushCheckTimeout bounds the dry run.
const PushCheckTimeout = 2 * time.Minute

// BundleName is the file the commits of a branch that could not be pushed
// are published as: the branch, its slashes as dashes (agent-fix-login-ab12.bundle).
func BundleName(branch string) string {
	return strings.ReplaceAll(branch, "/", "-") + ".bundle"
}

// maxAnalyzeTask bounds an analysis's task.
const maxAnalyzeTask = 64 << 10

// AnalyzeInput is an analyze_repo directive: what the calling model chose.
type AnalyzeInput struct {
	Repo string `json:"repo"`
	Ref  string `json:"ref,omitempty"`
	Task string `json:"task"`
	// Skills are the calling agent's skills for the run (PickMachine reads
	// them where it runs); none = none. SkillsVersion is what they were
	// loaded from (a repository's commit), kept for the run's result: the
	// machine does nothing with it.
	Skills        []RunSkill `json:"skills,omitempty"`
	SkillsVersion string     `json:"skills_version,omitempty"`
}

// Check refuses what git or the CLI would read as something else: a
// repository or ref starting with a dash, control characters, no task;
// and skills past a run's bounds (CheckRunSkills).
func (in AnalyzeInput) Check() error {
	if err := CheckRunSkills(in.Skills); err != nil {
		return err
	}
	switch {
	case strings.TrimSpace(in.Repo) == "" || strings.TrimSpace(in.Task) == "":
		return fmt.Errorf("analyze_repo needs a repo and a task")
	case strings.HasPrefix(in.Repo, "-") || strings.ContainsAny(in.Repo, "\x00\r\n"):
		return fmt.Errorf("invalid repository %q", in.Repo)
	case strings.HasPrefix(in.Ref, "-") || strings.ContainsAny(in.Ref, "\x00\r\n"):
		return fmt.Errorf("invalid ref %q", in.Ref)
	case len(in.Task) > maxAnalyzeTask:
		return fmt.Errorf("task over %d bytes", maxAnalyzeTask)
	}
	return nil
}

// ImplementInput is an implement_feature directive: what the calling model
// chose (Repo, Base, Task, MaxBudgetUSD, which can only lower the machine's
// own cap), and the branch the workflow named.
type ImplementInput struct {
	Repo         string  `json:"repo"`
	Base         string  `json:"base,omitempty"`
	Task         string  `json:"task"`
	Branch       string  `json:"branch"`
	MaxBudgetUSD float64 `json:"max_budget_usd,omitempty"`
	// Skills and SkillsVersion, as AnalyzeInput's.
	Skills        []RunSkill `json:"skills,omitempty"`
	SkillsVersion string     `json:"skills_version,omitempty"`
}

// branchPattern is a branch a run may publish: under BranchPrefix, of the
// characters branchName makes.
var branchPattern = regexp.MustCompile(`^agent/[a-z0-9][a-z0-9-]{0,63}$`)

// Check refuses what git or the CLI would read as something else, and a
// branch outside BranchPrefix: a machine never pushes to a branch a person
// works on, whatever the directive says.
func (in ImplementInput) Check() error {
	if err := CheckRunSkills(in.Skills); err != nil {
		return err
	}
	switch {
	case strings.TrimSpace(in.Repo) == "" || strings.TrimSpace(in.Task) == "":
		return fmt.Errorf("implement_feature needs a repo and a task")
	case strings.HasPrefix(in.Repo, "-") || strings.ContainsAny(in.Repo, "\x00\r\n"):
		return fmt.Errorf("invalid repository %q", in.Repo)
	case strings.HasPrefix(in.Base, "-") || strings.ContainsAny(in.Base, "\x00\r\n"):
		return fmt.Errorf("invalid base %q", in.Base)
	case !branchPattern.MatchString(in.Branch):
		return fmt.Errorf("invalid branch %q: a run publishes under %s only", in.Branch, BranchPrefix)
	case len(in.Task) > maxAnalyzeTask:
		return fmt.Errorf("task over %d bytes", maxAnalyzeTask)
	case in.MaxBudgetUSD < 0:
		return fmt.Errorf("max_budget_usd %g", in.MaxBudgetUSD)
	}
	return nil
}

// Commit is a commit a run made, as the machine's inspection lists it.
type Commit struct {
	SHA     string `json:"sha"`
	Subject string `json:"subject"`
}

// CodingOutput is what a machine's coding run returns: what the workflow
// needs to say what happened (workflow.ClaudeCodeOutput), nothing it would
// have to trust more than text. Error is a failure the machine reports
// (a repository it refuses, a clone that failed, a run that ended without
// the CLI's result); the report may still be there.
type CodingOutput struct {
	Report     string         `json:"report,omitempty"`
	Error      string         `json:"error,omitempty"`
	Commit     string         `json:"commit,omitempty"`
	IsError    bool           `json:"is_error,omitempty"`
	Subtype    string         `json:"subtype,omitempty"`
	NumTurns   int            `json:"num_turns,omitempty"`
	DurationMS int64          `json:"duration_ms,omitempty"`
	CostUSD    float64        `json:"cost_usd,omitempty"`
	PaidBy     string         `json:"paid_by,omitempty"`
	ToolUses   map[string]int `json:"tool_uses,omitempty"`
	// Interrupted: the run ended without the CLI's result (stopped, stuck,
	// cancelled); ToolCalls, LastTool and Events say how far it got.
	Interrupted bool   `json:"interrupted,omitempty"`
	ToolCalls   int    `json:"tool_calls,omitempty"`
	LastTool    string `json:"last_tool,omitempty"`
	Events      int    `json:"events,omitempty"`

	// An implementation: its branch, the commits on it (newest first), and
	// whether they were pushed; Dirty, changes left uncommitted.
	Branch  string   `json:"branch,omitempty"`
	Commits []Commit `json:"commits,omitempty"`
	Pushed  bool     `json:"pushed,omitempty"`
	Dirty   bool     `json:"dirty,omitempty"`
	// Bundle is the name of the git bundle of the branch the machine
	// published when the push failed (BundleName): the gateway lists the
	// file itself. BundleError says why there is none, when the push failed.
	Bundle      string `json:"bundle,omitempty"`
	BundleError string `json:"bundle_error,omitempty"`
	// Unpublished are the run's outputs the machine did not publish, and
	// why ("name: reason"). What it published, the gateway lists itself
	// (Result.Files): the machine's word is not needed for that.
	Unpublished []string `json:"unpublished,omitempty"`
	// SkillsMissing are the directive's skills the CLI did not load, and
	// why ("name: reason", SkillNotLoaded).
	SkillsMissing []string `json:"skills_missing,omitempty"`
}

// CloneProgress is a coding run's progress while it clones: the CLI has not
// started (nothing paid yet). PushProgress, while an implementation pushes.
const (
	CloneProgress = "clone"
	PushProgress  = "push"
)

// CodingProgress is a coding run's progress, as its user reads it on the
// turn's line: "34 outils (dernier : Grep)".
func CodingProgress(toolCalls int, lastTool string) string {
	s := fmt.Sprintf("%d outil", toolCalls)
	if toolCalls > 1 {
		s += "s"
	}
	if lastTool != "" {
		s += " (dernier : " + lastTool + ")"
	}
	return s
}
