package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func RegisterGlobTool(r *Registry, workspacePath string) {
	absWorkspace, _ := filepath.Abs(workspacePath)

	r.Register(&Tool{
		Name: "glob",
		Description: `Find files by name pattern using glob syntax. Supports "*" (any segment), "**" (recursive), "?" (single char).
Examples: "**/*.go" (all Go files), "cmd/**/*.go" (Go files under cmd/), "*.yaml" (YAML files in root).`,
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"pattern": {"type": "string", "description": "Glob pattern to match files (e.g. \"**/*.go\", \"src/**/*.ts\")"},
				"path": {"type": "string", "description": "Directory to search in, relative to workspace (default: root)"}
			},
			"required": ["pattern"]
		}`),
		Kind: ToolKindActivity,
		Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
			var params struct {
				Pattern string `json:"pattern"`
				Path    string `json:"path"`
			}
			if err := json.Unmarshal(input, &params); err != nil {
				return "", err
			}

			searchRoot := absWorkspace
			if params.Path != "" {
				var err error
				searchRoot, err = safePath(absWorkspace, params.Path)
				if err != nil {
					return "", err
				}
			}

			// Check if pattern uses ** (recursive)
			recursive := strings.Contains(params.Pattern, "**")

			// Split pattern on ** to get prefix dir and file pattern
			var dirPattern, filePattern string
			if recursive {
				parts := strings.SplitN(params.Pattern, "**/", 2)
				if len(parts) == 2 {
					dirPattern = parts[0]
					filePattern = parts[1]
				} else {
					// Pattern like "**" alone
					filePattern = "*"
				}
				if dirPattern != "" {
					var err error
					searchRoot, err = safePath(searchRoot, dirPattern)
					if err != nil {
						return "", err
					}
				}
			} else {
				// Non-recursive: might contain directory parts like "cmd/*.go"
				dir := filepath.Dir(params.Pattern)
				filePattern = filepath.Base(params.Pattern)
				if dir != "." {
					var err error
					searchRoot, err = safePath(searchRoot, dir)
					if err != nil {
						return "", err
					}
				}
				recursive = false
			}

			var matches []string
			const maxResults = 1000

			if recursive {
				err := filepath.Walk(searchRoot, func(path string, info os.FileInfo, err error) error {
					if err != nil {
						return nil
					}
					if info.IsDir() {
						name := info.Name()
						if strings.HasPrefix(name, ".") || name == "node_modules" || name == "vendor" {
							return filepath.SkipDir
						}
						return nil
					}

					matched, _ := filepath.Match(filePattern, info.Name())
					if matched {
						relPath, _ := filepath.Rel(absWorkspace, path)
						matches = append(matches, relPath)
						if len(matches) >= maxResults {
							return filepath.SkipAll
						}
					}
					return nil
				})
				if err != nil && err != filepath.SkipAll {
					return "", fmt.Errorf("glob: %w", err)
				}
			} else {
				entries, err := os.ReadDir(searchRoot)
				if err != nil {
					return "", fmt.Errorf("glob: %w", err)
				}
				for _, e := range entries {
					if e.IsDir() {
						continue
					}
					matched, _ := filepath.Match(filePattern, e.Name())
					if matched {
						relPath, _ := filepath.Rel(absWorkspace, filepath.Join(searchRoot, e.Name()))
						matches = append(matches, relPath)
						if len(matches) >= maxResults {
							break
						}
					}
				}
			}

			if len(matches) == 0 {
				return "No files found.", nil
			}
			result := strings.Join(matches, "\n")
			if len(matches) >= maxResults {
				result += fmt.Sprintf("\n\n... (truncated at %d results)", maxResults)
			}
			return result, nil
		},
	})
}
