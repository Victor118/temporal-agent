package skill

import "strings"

// parseFrontmatter extracts YAML frontmatter (name, description) and the body.
func parseFrontmatter(content string) (name, description, body string) {
	if !strings.HasPrefix(content, "---") {
		return "", "", content
	}

	rest := content[3:]
	endIdx := strings.Index(rest, "---")
	if endIdx < 0 {
		return "", "", content
	}

	frontmatter := rest[:endIdx]
	body = strings.TrimSpace(rest[endIdx+3:])

	for _, line := range strings.Split(frontmatter, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "name:") {
			name = strings.TrimSpace(strings.TrimPrefix(line, "name:"))
		} else if strings.HasPrefix(line, "description:") {
			description = strings.TrimSpace(strings.TrimPrefix(line, "description:"))
		}
	}

	return name, description, body
}
