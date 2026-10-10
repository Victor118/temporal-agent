//go:build unix

package connect

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/victor/temporal-agent/machine"
)

// skillsStream is a stand-in CLI that reads the plugin it was given, as the
// real one does, and lists its skill in its init when it finds it.
const skillsStream = `plugin=none
while [ $# -gt 0 ]; do
  [ "$1" = --plugin-dir ] && plugin=$2
  shift
done
cp "$plugin/skills/tdd/SKILL.md" "$(dirname "$0")/skill.md" 2>/dev/null
cp "$plugin/.claude-plugin/plugin.json" "$(dirname "$0")/plugin.json" 2>/dev/null
cmds='"compact"'
[ -r "$plugin/skills/tdd/SKILL.md" ] && cmds='"compact","temporal-agent:tdd"'
echo '{"type":"system","subtype":"init","session_id":"s","slash_commands":['"$cmds"']}'
echo '{"type":"result","subtype":"success","is_error":false,"result":"ok","session_id":"s"}'
`

var tddSkill = machine.RunSkill{Name: "tdd", Description: "Test first", Content: "RED, GREEN, REFACTOR: one commit per step."}

func skillsInput(repo string, skills ...machine.RunSkill) json.RawMessage {
	raw, _ := json.Marshal(machine.AnalyzeInput{Repo: repo, Task: "Where is the handler?", Skills: skills})
	return raw
}

// A directive's skills reach the machine's CLI as a plugin of the run
// alone: --plugin-dir, the skill under it with a frontmatter of the
// machine's, its name in the system prompt; deleted with the run.
func TestCoder_AnalyzeWithSkills(t *testing.T) {
	a, repo, seen := newAnalyzer(t, skillsStream)
	a.PluginDir = true
	raw, err := a.Analyze(context.Background(), skillsInput(repo, tddSkill), func(string) {})
	var out machine.CodingOutput
	if err != nil || json.Unmarshal(raw, &out) != nil || out.Report != "ok" || len(out.SkillsMissing) != 0 {
		t.Fatalf("run: %s %v", raw, err)
	}
	args, _ := os.ReadFile(filepath.Join(seen, "args"))
	for _, want := range []string{"--plugin-dir " + a.WorkDir, "/plugin ", "gave it skills: temporal-agent:tdd."} {
		if !strings.Contains(string(args), want) {
			t.Errorf("args lack %q: %s", want, args)
		}
	}
	bin := filepath.Dir(a.Runner.Binary)
	skill, _ := os.ReadFile(filepath.Join(bin, "skill.md"))
	if want := "---\nname: \"tdd\"\ndescription: \"Test first\"\n---\n\nRED, GREEN, REFACTOR: one commit per step.\n"; string(skill) != want {
		t.Errorf("SKILL.md %q", skill)
	}
	if manifest, _ := os.ReadFile(filepath.Join(bin, "plugin.json")); !strings.Contains(string(manifest), `"name": "temporal-agent"`) {
		t.Errorf("manifest %s", manifest)
	}
	if entries, _ := os.ReadDir(a.WorkDir); len(entries) != 0 {
		t.Errorf("run left behind: %v", entries)
	}

	// No skills: no plugin, no line.
	if _, err := a.Analyze(context.Background(), skillsInput(repo), func(string) {}); err != nil {
		t.Fatal(err)
	}
	if args, _ := os.ReadFile(filepath.Join(seen, "args")); strings.Contains(string(args), "--plugin-dir") || strings.Contains(string(args), "skills:") {
		t.Errorf("a plugin without skills: %s", args)
	}
}

// A skill the CLI did not list in its init is said in the output: the run
// went on without it.
func TestCoder_SkillNotLoaded(t *testing.T) {
	a, repo, _ := newAnalyzer(t, analyzeStream)
	a.PluginDir = true
	raw, err := a.Analyze(context.Background(), skillsInput(repo, tddSkill), func(string) {})
	var out machine.CodingOutput
	if err != nil || json.Unmarshal(raw, &out) != nil || !slices.Equal(out.SkillsMissing, []string{"tdd: " + machine.SkillNotLoaded}) {
		t.Errorf("output %s %v", raw, err)
	}
}

// A CLI without --plugin-dir refuses a run that has skills (it goes
// elsewhere), and runs one that has none.
func TestCoder_SkillsWithAnOldCLI(t *testing.T) {
	a, repo, _ := newAnalyzer(t, analyzeStream)
	_, err := a.Analyze(context.Background(), skillsInput(repo, tddSkill), func(string) {})
	var refusal *Refusal
	if !errors.As(err, &refusal) || !strings.Contains(err.Error(), "too old") {
		t.Errorf("an old CLI: %v", err)
	}
	if _, err := a.Analyze(context.Background(), skillsInput(repo), func(string) {}); err != nil {
		t.Errorf("an old CLI, no skills: %v", err)
	}
	b, remote, _, _ := newImplementer(t, analyzeStream)
	raw, _ := json.Marshal(machine.ImplementInput{Repo: remote, Base: "main", Task: "t", Branch: "agent/t-1", Skills: []machine.RunSkill{tddSkill}})
	if _, err := b.Implement(context.Background(), raw, func(string) {}); !errors.As(err, &refusal) {
		t.Errorf("an implementation with an old CLI: %v", err)
	}
}

// Managed settings that forbid --plugin-dir make the CLI exit before any
// tool: a refusal, the run goes elsewhere.
func TestCoder_SkillsRefusedByPolicy(t *testing.T) {
	a, repo, _ := newAnalyzer(t, `echo "--plugin-dir is disabled by your organization's managed settings (disableSideloadFlags)." >&2
exit 1
`)
	a.PluginDir = true
	_, err := a.Analyze(context.Background(), skillsInput(repo, tddSkill), func(string) {})
	var refusal *Refusal
	if !errors.As(err, &refusal) || !strings.Contains(err.Error(), "managed settings") {
		t.Errorf("policy: %v", err)
	}
	if entries, _ := os.ReadDir(a.WorkDir); len(entries) != 0 {
		t.Errorf("run left behind: %v", entries)
	}
}

// Skills past a run's bounds are refused by the machine itself, whatever
// the server let through.
func TestCoder_SkillsPastTheBounds(t *testing.T) {
	a, repo, _ := newAnalyzer(t, analyzeStream)
	a.PluginDir = true
	if _, err := a.Analyze(context.Background(), skillsInput(repo, machine.RunSkill{Name: "../tdd", Content: "x"}), func(string) {}); err == nil {
		t.Error("a skill's name read as a path")
	}
}
