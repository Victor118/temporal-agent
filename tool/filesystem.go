package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func RegisterFilesystemTools(r *Registry, workspacePath string) {
	absWorkspace, _ := filepath.Abs(workspacePath)

	r.Register(&Tool{
		Name:        "read_file",
		Description: "Read the contents of a file at the given path.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"path": {"type": "string", "description": "File path relative to workspace"}
			},
			"required": ["path"]
		}`),
		Kind: ToolKindActivity,
		Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
			var params struct{ Path string `json:"path"` }
			if err := json.Unmarshal(input, &params); err != nil {
				return "", err
			}
			fullPath, err := safePath(absWorkspace, params.Path)
			if err != nil {
				return "", err
			}
			data, err := os.ReadFile(fullPath)
			if err != nil {
				return "", fmt.Errorf("read_file: %w", err)
			}
			return string(data), nil
		},
	})

	r.Register(&Tool{
		Name:        "write_file",
		Description: "Write content to a file at the given path. Creates parent directories if needed.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"path": {"type": "string", "description": "File path relative to workspace"},
				"content": {"type": "string", "description": "Content to write"}
			},
			"required": ["path", "content"]
		}`),
		Kind: ToolKindActivity,
		Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
			var params struct {
				Path    string `json:"path"`
				Content string `json:"content"`
			}
			if err := json.Unmarshal(input, &params); err != nil {
				return "", err
			}
			fullPath, err := safePath(absWorkspace, params.Path)
			if err != nil {
				return "", err
			}
			if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
				return "", fmt.Errorf("write_file: mkdir: %w", err)
			}
			if err := os.WriteFile(fullPath, []byte(params.Content), 0644); err != nil {
				return "", fmt.Errorf("write_file: %w", err)
			}
			return "File written successfully.", nil
		},
	})

	r.Register(&Tool{
		Name:        "edit_file",
		Description: "Replace a string in a file. The old_string must appear exactly once in the file.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"path": {"type": "string", "description": "File path relative to workspace"},
				"old_string": {"type": "string", "description": "Exact string to replace"},
				"new_string": {"type": "string", "description": "Replacement string"}
			},
			"required": ["path", "old_string", "new_string"]
		}`),
		Kind: ToolKindActivity,
		Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
			var params struct {
				Path      string `json:"path"`
				OldString string `json:"old_string"`
				NewString string `json:"new_string"`
			}
			if err := json.Unmarshal(input, &params); err != nil {
				return "", err
			}
			fullPath, err := safePath(absWorkspace, params.Path)
			if err != nil {
				return "", err
			}
			data, err := os.ReadFile(fullPath)
			if err != nil {
				return "", fmt.Errorf("edit_file: %w", err)
			}
			content := string(data)
			count := strings.Count(content, params.OldString)
			if count == 0 {
				return "", fmt.Errorf("edit_file: old_string not found in file")
			}
			if count > 1 {
				return "", fmt.Errorf("edit_file: old_string appears %d times, must be unique", count)
			}
			newContent := strings.Replace(content, params.OldString, params.NewString, 1)
			if err := os.WriteFile(fullPath, []byte(newContent), 0644); err != nil {
				return "", fmt.Errorf("edit_file: write: %w", err)
			}
			return "File edited successfully.", nil
		},
	})

	r.Register(&Tool{
		Name:        "list_directory",
		Description: "List files and directories at the given path.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"path": {"type": "string", "description": "Directory path relative to workspace (default: root)"}
			}
		}`),
		Kind: ToolKindActivity,
		Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
			var params struct{ Path string `json:"path"` }
			if err := json.Unmarshal(input, &params); err != nil {
				return "", err
			}
			if params.Path == "" {
				params.Path = "."
			}
			fullPath, err := safePath(absWorkspace, params.Path)
			if err != nil {
				return "", err
			}
			entries, err := os.ReadDir(fullPath)
			if err != nil {
				return "", fmt.Errorf("list_directory: %w", err)
			}
			var lines []string
			for _, e := range entries {
				name := e.Name()
				if e.IsDir() {
					name += "/"
				}
				lines = append(lines, name)
			}
			return strings.Join(lines, "\n"), nil
		},
	})

}

// safePath resolves a relative path within the workspace and ensures it doesn't escape.
func safePath(workspace, rel string) (string, error) {
	full := filepath.Join(workspace, filepath.Clean(rel))
	abs, err := filepath.Abs(full)
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(abs, workspace) {
		return "", fmt.Errorf("path escapes workspace: %s", rel)
	}
	return abs, nil
}
