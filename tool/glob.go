package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"time"
)

// globTimeout bounds a recursive search, as grepTimeout does grep's: a search
// stopped by it returns the files found so far. The tool's Timeout derives
// from it.
const globTimeout = 60 * time.Second

// maxGlobResults caps the files one search lists.
const maxGlobResults = 1000

func RegisterGlobTool(r *Registry, workspacePath string) {
	ws := newWorkspace(workspacePath, nil)

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
		Kind:    ToolKindActivity,
		Timeout: globTimeout + TimeoutMargin,
		Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
			var params struct {
				Pattern string `json:"pattern"`
				Path    string `json:"path"`
			}
			if err := json.Unmarshal(input, &params); err != nil {
				return "", err
			}

			start := "."
			if params.Path != "" {
				var err error
				if start, err = relPath(params.Path); err != nil {
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
					sub, err := relPath(dirPattern)
					if err != nil {
						return "", err
					}
					start = filepath.Join(start, sub)
				}
			} else {
				// Non-recursive: might contain directory parts like "cmd/*.go"
				dir := filepath.Dir(params.Pattern)
				filePattern = filepath.Base(params.Pattern)
				if dir != "." {
					sub, err := relPath(dir)
					if err != nil {
						return "", err
					}
					start = filepath.Join(start, sub)
				}
				recursive = false
			}

			root, err := ws.open()
			if err != nil {
				return "", fmt.Errorf("glob: %w", err)
			}
			defer root.Close()

			var matches []string
			stopped := false
			if recursive {
				ctx, cancel := context.WithTimeout(ctx, globTimeout)
				defer cancel()
				if matches, stopped, err = globWalk(ctx, walkFS{root}, start, filePattern); err != nil {
					return "", fmt.Errorf("glob: %w", err)
				}
			} else {
				entries, err := readDir(root, start)
				if err != nil {
					return "", fmt.Errorf("glob: %w", err)
				}
				for _, e := range entries {
					if e.IsDir() {
						continue
					}
					matched, _ := filepath.Match(filePattern, e.Name())
					if matched {
						matches = append(matches, filepath.Join(start, e.Name()))
						if len(matches) >= maxGlobResults {
							break
						}
					}
				}
			}

			if stopped && len(matches) == 0 {
				return "", fmt.Errorf("glob: no file found before the search stopped after %s: narrow it with path or pattern", globTimeout)
			}
			if len(matches) == 0 {
				return "No files found.", nil
			}
			result := strings.Join(matches, "\n")
			if len(matches) >= maxGlobResults {
				result += fmt.Sprintf("\n\n... (truncated at %d results)", maxGlobResults)
			}
			if stopped {
				result += fmt.Sprintf("\n\n... (search stopped after %s: narrow it with path or pattern)", globTimeout)
			}
			return result, nil
		},
	})
}

// globWalk lists the files under start whose name matches filePattern,
// skipping hidden directories, node_modules and vendor. It stops at ctx's end
// with the files found so far (stopped): a walk the activity no longer waits
// for would otherwise go on on the worker.
func globWalk(ctx context.Context, fsys fs.FS, start, filePattern string) (matches []string, stopped bool, err error) {
	err = fs.WalkDir(fsys, start, func(rel string, d fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			stopped = true
			return fs.SkipAll
		}
		if err != nil {
			return nil
		}
		if d.IsDir() {
			name := d.Name()
			if rel != "." && (strings.HasPrefix(name, ".") || name == "node_modules" || name == "vendor") {
				return fs.SkipDir
			}
			return nil
		}

		matched, _ := filepath.Match(filePattern, d.Name())
		if matched {
			matches = append(matches, rel)
			if len(matches) >= maxGlobResults {
				return fs.SkipAll
			}
		}
		return nil
	})
	if err == fs.SkipAll {
		err = nil
	}
	return matches, stopped, err
}
