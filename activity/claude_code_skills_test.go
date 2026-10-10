package activity

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"go.temporal.io/sdk/temporal"

	"github.com/victor/temporal-agent/claudecode"
	"github.com/victor/temporal-agent/machine"
	"github.com/victor/temporal-agent/skill"
	"github.com/victor/temporal-agent/subproc"
	"github.com/victor/temporal-agent/subproc/subproctest"
)

// fileSkills are the skills of a SKILLS_DIR, as a coding worker reads them.
func fileSkills(t *testing.T, files map[string]string) *Prompts {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		if err := os.MkdirAll(filepath.Join(dir, name), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name, "SKILL.md"), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	skills, err := (&skill.FileStore{Dir: dir}).LoadAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return NewSkillActivities(skills, NewCatalog()).Prompts
}

// On a worker of the fallback, the run's skills are read from its own
// source and written as the run's plugin, next to the workspace: the
// frontmatter composed, never the source's. A retry starts afresh, and the
// cleanup takes the plugin with the workspace.
func TestPrepareWorkspace_WritesThePlugin(t *testing.T) {
	src := initRepo(t)
	a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir(), Skills: fileSkills(t, map[string]string{
		"tdd":    "---\nname: tdd\ndescription: Test first\nruns: true\nallowed-tools: Bash(*)\n---\nRED, GREEN, REFACTOR.",
		"review": "---\nname: review\ndescription: Look\n---\nLook twice.",
	})}
	in := PrepareWorkspaceInput{Name: "run-1", Repo: src, Skills: []string{"tdd", "review"}}
	out, err := a.PrepareWorkspace(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(out.Skills, []string{"tdd"}) || !slices.Equal(out.SkillsMissing, []string{"review: " + machine.SkillNotFound}) {
		t.Errorf("%+v", out)
	}
	got, err := os.ReadFile(filepath.Join(pluginDir(out.Dir), "skills", "tdd", "SKILL.md"))
	if err != nil || !strings.Contains(string(got), "RED, GREEN, REFACTOR.") || strings.Contains(string(got), "allowed-tools") ||
		strings.Contains(string(got), "runs:") {
		t.Errorf("SKILL.md %q %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(pluginDir(out.Dir), ".claude-plugin", "plugin.json")); err != nil {
		t.Error(err)
	}
	if _, err := a.PrepareWorkspace(context.Background(), in); err != nil {
		t.Fatalf("retried: %v", err)
	}
	if err := a.CleanupWorkspace(context.Background(), CleanupWorkspaceInput{Dir: out.Dir}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(pluginDir(out.Dir)); !os.IsNotExist(err) {
		t.Errorf("the plugin outlived the workspace: %v", err)
	}

	// No skills named: no plugin.
	out, err = a.PrepareWorkspace(context.Background(), PrepareWorkspaceInput{Name: "run-2", Repo: src})
	if _, serr := os.Stat(pluginDir(out.Dir)); err != nil || !os.IsNotExist(serr) || len(out.Skills) != 0 {
		t.Errorf("a plugin with no skills: %+v %v %v", out, err, serr)
	}
}

// Skills a run cannot take fail the preparation for good.
func TestPrepareWorkspace_RefusesSkillsPastTheBounds(t *testing.T) {
	src := initRepo(t)
	a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir(), Skills: skillReader{"Bad": {Name: "Bad", Content: "x"}}}
	_, err := a.PrepareWorkspace(context.Background(), PrepareWorkspaceInput{Name: "run-1", Repo: src, Skills: []string{"Bad"}})
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) || !appErr.NonRetryable() || !strings.Contains(err.Error(), "skills cannot go") {
		t.Errorf("%v", err)
	}
}

// A run with skills loads its plugin, and its system prompt names them.
func TestRunClaudeCode_Skills(t *testing.T) {
	id := subproctest.Identity(t)
	if os.Geteuid() == 0 && id == nil {
		t.Skip("no identity to run as")
	}
	bin := filepath.Join(subproctest.Dir(t, nil), "fake-claude")
	script := `#!/bin/sh
plugin=none prompt=none
while [ $# -gt 0 ]; do
  case "$1" in
    --plugin-dir) plugin=$2 ;;
    --append-system-prompt) prompt=$2 ;;
  esac
  shift
done
found=no
[ -r "$plugin/skills/tdd/SKILL.md" ] && found=yes
named=no
case "$prompt" in *temporal-agent:tdd*) named=yes ;; esac
printf '{"type":"system","subtype":"init","session_id":"s","slash_commands":["temporal-agent:tdd"]}\n'
printf '{"type":"result","subtype":"success","is_error":false,"result":"plugin=%s found=%s named=%s","session_id":"s"}\n' "$plugin" "$found" "$named"
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: subproctest.Dir(t, nil), RunAs: id, Runner: &claudecode.Runner{Binary: bin}}
	if id != nil {
		a.Runs = subproc.NewRuns(id)
	}
	dir := filepath.Join(a.Root, "run-1")
	os.Mkdir(dir, 0o755)
	if id != nil {
		if err := id.Give(dir); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := machine.WritePlugin(pluginDir(dir), []machine.RunSkill{{Name: "tdd", Content: "RED."}}); err != nil {
		t.Fatal(err)
	}
	res, err := a.RunClaudeCode(context.Background(), RunClaudeCodeInput{Dir: dir, Task: "x", Skills: []string{"tdd"}})
	if err != nil {
		t.Fatal(err)
	}
	if want := "plugin=" + pluginDir(dir) + " found=yes named=yes"; res.Report != want || !slices.Equal(res.SlashCommands, []string{"temporal-agent:tdd"}) {
		t.Errorf("the CLI says %q (%v), want %q", res.Report, res.SlashCommands, want)
	}
	if res, _ := a.RunClaudeCode(context.Background(), RunClaudeCodeInput{Dir: dir, Task: "x"}); res.Report != "plugin=none found=no named=no" {
		t.Errorf("without skills: %q", res.Report)
	}
}

// The operator's managed settings forbid --plugin-dir: the run fails, and
// says why.
func TestRunClaudeCode_PluginRefusedByPolicy(t *testing.T) {
	id := subproctest.Identity(t)
	if os.Geteuid() == 0 && id == nil {
		t.Skip("no identity to run as")
	}
	bin := filepath.Join(subproctest.Dir(t, nil), "fake-claude")
	script := "#!/bin/sh\necho \"--plugin-dir is disabled by your organization's managed settings (disableSideloadFlags).\" >&2\nexit 1\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: subproctest.Dir(t, nil), RunAs: id, Runner: &claudecode.Runner{Binary: bin}}
	if id != nil {
		a.Runs = subproc.NewRuns(id)
	}
	dir := filepath.Join(a.Root, "run-1")
	os.Mkdir(dir, 0o755)
	if id != nil {
		if err := id.Give(dir); err != nil {
			t.Fatal(err)
		}
	}
	_, err := a.RunClaudeCode(context.Background(), RunClaudeCodeInput{Dir: dir, Task: "x", Skills: []string{"tdd"}})
	if !hasType(err, ErrRunFailed) || !strings.Contains(err.Error(), "managed settings forbid --plugin-dir") {
		t.Errorf("%v", err)
	}
}
