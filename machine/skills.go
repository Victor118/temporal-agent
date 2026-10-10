package machine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

// The skills of a coding run (docs/design/run-skills.md): an agent's skills
// marked "runs: true" reach the CLI of its analyze_repo and
// implement_feature runs, as a local plugin given by --plugin-dir, for that
// run alone. One code writes it, on a machine as on a worker: WritePlugin.
const (
	// PluginName names the plugin: the CLI calls its skills
	// "temporal-agent:<skill>".
	PluginName = "temporal-agent"
	// pluginVersion is the manifest's: the plugin lives as long as its run.
	pluginVersion = "1.0.0"
	// MaxRunSkills is how many skills a run takes at most, MaxRunSkillsBytes
	// how much they weigh at most, names, descriptions and bodies together.
	MaxRunSkills      = 16
	MaxRunSkillsBytes = 64 << 10
	// maxSkillDescription bounds the description a skill's frontmatter is
	// given.
	maxSkillDescription = 1024
)

// skillNamePattern is a skill's name a run accepts: it names a directory of
// the plugin, and the CLI wants that directory's name as the skill's.
var skillNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

// RunSkill is a skill a run takes along: what the agent's skill says, never
// its source's frontmatter (WritePlugin composes its own).
type RunSkill struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Content     string `json:"content"`
}

// What a run's skills lacked, as its output says it: "<name>: <reason>".
const (
	// SkillNotFound: the one who prepared the run (PickMachine, or the
	// worker of the fallback) has no such skill marked runs.
	SkillNotFound = "not found among the skills where the run was prepared"
	// SkillNotLoaded: the CLI did not list it (its init's slash_commands):
	// the plugin was ignored.
	SkillNotLoaded = "not loaded by the CLI"
)

// UniqueSkills is skills without the repeats of a name: the first of each.
func UniqueSkills(skills []RunSkill) []RunSkill {
	var out []RunSkill
	seen := map[string]bool{}
	for _, s := range skills {
		if !seen[s.Name] {
			seen[s.Name] = true
			out = append(out, s)
		}
	}
	return out
}

// CheckRunSkills refuses skills a run cannot take: a name that is no
// directory name of the plugin's, more than MaxRunSkills, more than
// MaxRunSkillsBytes. A machine checks them itself (Check): it does not
// count on the server for what protects it.
func CheckRunSkills(skills []RunSkill) error {
	if len(skills) > MaxRunSkills {
		return fmt.Errorf("%d skills, at most %d per run", len(skills), MaxRunSkills)
	}
	size := 0
	for _, s := range skills {
		if !skillNamePattern.MatchString(s.Name) {
			return fmt.Errorf("skill name %q: a run's skill is named in lower case letters, digits, - and _ (64 at most)", s.Name)
		}
		size += len(s.Name) + len(s.Description) + len(s.Content)
	}
	if size > MaxRunSkillsBytes {
		return fmt.Errorf("skills of %d bytes, at most %d per run", size, MaxRunSkillsBytes)
	}
	return nil
}

// WritePlugin writes skills as a plugin in dir, which it creates (it must
// not exist), and returns the names it wrote, repeats dropped. What it
// writes is its own, never a skill's source: the manifest, and for each
// skill a SKILL.md whose frontmatter it composes (name, description)
// followed by the body. A skill's source frontmatter could carry
// allowed-tools, hooks or mcpServers, which the CLI would honor: a skill is
// an instruction, nothing more. Nothing else either (no hooks/, .mcp.json,
// agents/, commands/). Directories 0755 and files 0644: the run's user reads
// them, on a worker another than the one that writes them.
func WritePlugin(dir string, skills []RunSkill) ([]string, error) {
	skills = UniqueSkills(skills)
	if err := CheckRunSkills(skills); err != nil {
		return nil, err
	}
	if err := os.Mkdir(dir, 0o755); err != nil {
		return nil, err
	}
	manifest, err := json.MarshalIndent(map[string]string{
		"name":        PluginName,
		"version":     pluginVersion,
		"description": "The skills the agent that started this run gave it, for this run only.",
	}, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := writeNew(filepath.Join(dir, ".claude-plugin"), "plugin.json", manifest); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(skills))
	for _, s := range skills {
		if err := writeNew(filepath.Join(dir, "skills", s.Name), "SKILL.md", skillFile(s)); err != nil {
			return nil, err
		}
		names = append(names, s.Name)
	}
	return names, nil
}

// writeNew writes a new file name in dir, which it creates.
func writeNew(dir, name string, content []byte) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(content); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// skillFile is a skill's SKILL.md in the plugin: a frontmatter of two keys,
// each a quoted scalar (JSON's is YAML's), then the body.
func skillFile(s RunSkill) []byte {
	desc := strings.TrimSpace(s.Description)
	if desc == "" {
		desc = "Skill " + s.Name + " of the agent that started this run."
	}
	var sb strings.Builder
	sb.WriteString("---\nname: ")
	sb.WriteString(quoteScalar(s.Name))
	sb.WriteString("\ndescription: ")
	sb.WriteString(quoteScalar(Cut(desc, maxSkillDescription)))
	sb.WriteString("\n---\n\n")
	sb.WriteString(s.Content)
	sb.WriteString("\n")
	return []byte(sb.String())
}

// quoteScalar is s as a double-quoted YAML scalar.
func quoteScalar(s string) string {
	var sb strings.Builder
	enc := json.NewEncoder(&sb)
	enc.SetEscapeHTML(false)
	enc.Encode(s) // a string always encodes
	return strings.TrimSuffix(sb.String(), "\n")
}

// PluginSkill is how the CLI names a skill of the plugin.
func PluginSkill(name string) string { return PluginName + ":" + name }

// SkillsPrompt is the line of a run's system prompt that names its skills:
// one the CLI may use is not one it opens (in plan mode it did not, §9 of
// the design). From the names WritePlugin wrote alone: never a description
// nor a body.
func SkillsPrompt(names []string) string {
	full := make([]string, len(names))
	for i, n := range names {
		full[i] = PluginSkill(n)
	}
	return "The agent that started this run gave it skills: " + strings.Join(full, ", ") +
		". Load each with the Skill tool before you start, and follow them."
}

// SkillsNotLoaded are the names, among those written, the CLI did not list
// in its init's slash_commands ("<name>: not loaded by the CLI").
func SkillsNotLoaded(names, slashCommands []string) []string {
	var missing []string
	for _, n := range names {
		if !slices.ContainsFunc(slashCommands, func(c string) bool { return strings.TrimPrefix(c, "/") == PluginSkill(n) }) {
			missing = append(missing, n+": "+SkillNotLoaded)
		}
	}
	return missing
}
