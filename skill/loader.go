package skill

import "strings"

// frontmatter is what a SKILL.md's frontmatter says that the skills read.
type frontmatter struct {
	name, description string
	// runs is "runs: true", exactly: a coding run takes along only what was
	// meant for it, never a value that might mean yes.
	runs bool
}

// parseFrontmatter extracts the YAML frontmatter (name, description, runs)
// and the body.
func parseFrontmatter(content string) (fm frontmatter, body string) {
	if !strings.HasPrefix(content, "---") {
		return fm, content
	}

	rest := content[3:]
	endIdx := strings.Index(rest, "---")
	if endIdx < 0 {
		return fm, content
	}

	header := rest[:endIdx]
	body = strings.TrimSpace(rest[endIdx+3:])

	for _, line := range strings.Split(header, "\n") {
		line = strings.TrimSpace(line)
		if v, ok := strings.CutPrefix(line, "name:"); ok {
			fm.name = strings.TrimSpace(v)
		} else if v, ok := strings.CutPrefix(line, "description:"); ok {
			fm.description = strings.TrimSpace(v)
		} else if v, ok := strings.CutPrefix(line, "runs:"); ok {
			fm.runs = strings.TrimSpace(v) == "true"
		}
	}

	return fm, body
}
