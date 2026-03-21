package tool

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

func RegisterGrepTool(r *Registry, workspacePath string) {
	absWorkspace, _ := filepath.Abs(workspacePath)

	r.Register(&Tool{
		Name: "grep",
		Description: `Search file contents using regex patterns. Returns matching lines with file paths and line numbers.
Use "include" to filter by glob pattern (e.g. "*.go", "**/*.ts").
Output modes: "content" (matching lines, default), "files" (file paths only), "count" (match counts per file).
Context lines: use "before", "after", or "context" to show surrounding lines.`,
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"pattern": {"type": "string", "description": "Regex pattern to search for"},
				"path": {"type": "string", "description": "Directory to search in, relative to workspace (default: root)"},
				"include": {"type": "string", "description": "Glob pattern to filter files (e.g. \"*.go\", \"*.{ts,tsx}\")"},
				"ignore_case": {"type": "boolean", "description": "Case insensitive search (default: false)"},
				"output_mode": {"type": "string", "enum": ["content", "files", "count"], "description": "Output mode (default: content)"},
				"before": {"type": "integer", "description": "Lines to show before each match"},
				"after": {"type": "integer", "description": "Lines to show after each match"},
				"context": {"type": "integer", "description": "Lines to show before and after each match"},
				"max_results": {"type": "integer", "description": "Maximum number of matches to return (default: 200)"}
			},
			"required": ["pattern"]
		}`),
		Kind: ToolKindActivity,
		Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
			var params struct {
				Pattern    string `json:"pattern"`
				Path       string `json:"path"`
				Include    string `json:"include"`
				IgnoreCase bool   `json:"ignore_case"`
				OutputMode string `json:"output_mode"`
				Before     int    `json:"before"`
				After      int    `json:"after"`
				Context    int    `json:"context"`
				MaxResults int    `json:"max_results"`
			}
			if err := json.Unmarshal(input, &params); err != nil {
				return "", err
			}

			if params.OutputMode == "" {
				params.OutputMode = "content"
			}
			if params.MaxResults <= 0 {
				params.MaxResults = 200
			}
			if params.Context > 0 {
				if params.Before == 0 {
					params.Before = params.Context
				}
				if params.After == 0 {
					params.After = params.Context
				}
			}

			flags := ""
			if params.IgnoreCase {
				flags = "(?i)"
			}
			re, err := regexp.Compile(flags + params.Pattern)
			if err != nil {
				return "", fmt.Errorf("grep: invalid regex: %w", err)
			}

			searchRoot := absWorkspace
			if params.Path != "" {
				searchRoot, err = safePath(absWorkspace, params.Path)
				if err != nil {
					return "", err
				}
			}

			var out strings.Builder
			totalMatches := 0

			walkErr := filepath.Walk(searchRoot, func(path string, info os.FileInfo, err error) error {
				if err != nil || info.IsDir() {
					return err
				}
				if totalMatches >= params.MaxResults {
					return filepath.SkipAll
				}

				relPath, _ := filepath.Rel(absWorkspace, path)

				// Skip hidden dirs and common non-text dirs
				for _, part := range strings.Split(relPath, string(os.PathSeparator)) {
					if strings.HasPrefix(part, ".") || part == "node_modules" || part == "vendor" {
						return nil
					}
				}

				// Apply include glob filter
				if params.Include != "" {
					matched, _ := filepath.Match(params.Include, filepath.Base(path))
					if !matched {
						return nil
					}
				}

				// Skip binary files (check first 512 bytes)
				f, err := os.Open(path)
				if err != nil {
					return nil
				}
				defer f.Close()

				probe := make([]byte, 512)
				n, _ := f.Read(probe)
				if n > 0 {
					for _, b := range probe[:n] {
						if b == 0 {
							return nil // binary file
						}
					}
				}
				f.Seek(0, 0)

				// Read lines
				var lines []string
				scanner := bufio.NewScanner(f)
				scanner.Buffer(make([]byte, 256*1024), 256*1024)
				for scanner.Scan() {
					lines = append(lines, scanner.Text())
				}

				// Find matches
				var matchLineNums []int
				for i, line := range lines {
					if re.MatchString(line) {
						matchLineNums = append(matchLineNums, i)
					}
				}
				if len(matchLineNums) == 0 {
					return nil
				}

				switch params.OutputMode {
				case "files":
					fmt.Fprintln(&out, relPath)
					totalMatches++

				case "count":
					fmt.Fprintf(&out, "%s:%d\n", relPath, len(matchLineNums))
					totalMatches++

				default: // "content"
					// Collect lines to display (with context)
					show := make(map[int]bool)
					for _, lineNum := range matchLineNums {
						start := lineNum - params.Before
						if start < 0 {
							start = 0
						}
						end := lineNum + params.After
						if end >= len(lines) {
							end = len(lines) - 1
						}
						for i := start; i <= end; i++ {
							show[i] = true
						}
					}

					matchSet := make(map[int]bool)
					for _, ln := range matchLineNums {
						matchSet[ln] = true
					}

					prevShown := -2
					for i := 0; i < len(lines); i++ {
						if !show[i] {
							continue
						}
						if totalMatches >= params.MaxResults {
							fmt.Fprintf(&out, "\n... (truncated at %d matches)\n", params.MaxResults)
							return filepath.SkipAll
						}
						if prevShown >= 0 && i > prevShown+1 {
							fmt.Fprintln(&out, "--")
						}
						fmt.Fprintf(&out, "%s:%d:%s\n", relPath, i+1, lines[i])
						if matchSet[i] {
							totalMatches++
						}
						prevShown = i
					}
				}

				return nil
			})
			if walkErr != nil && walkErr != filepath.SkipAll {
				return "", fmt.Errorf("grep: %w", walkErr)
			}

			if totalMatches == 0 {
				return "No matches found.", nil
			}
			return out.String(), nil
		},
	})
}
