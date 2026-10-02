package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/victor/temporal-agent/subproc"
)

// RegisterFilesystemTools registers the tools that read and write the
// workspace. What they create belongs to owner, the user exec runs as, so
// that its commands can change it too; nil leaves it to the worker's user.
func RegisterFilesystemTools(r *Registry, workspacePath string, owner *subproc.Identity) {
	ws := newWorkspace(workspacePath, owner)

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
			var params struct {
				Path string `json:"path"`
			}
			if err := json.Unmarshal(input, &params); err != nil {
				return "", err
			}
			name, err := relPath(params.Path)
			if err != nil {
				return "", err
			}
			root, err := ws.open()
			if err != nil {
				return "", fmt.Errorf("read_file: %w", err)
			}
			defer root.Close()
			data, err := readFile(root, name)
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
		Kind:      ToolKindActivity,
		Sensitive: true,
		Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
			var params struct {
				Path    string `json:"path"`
				Content string `json:"content"`
			}
			if err := json.Unmarshal(input, &params); err != nil {
				return "", err
			}
			name, err := relPath(params.Path)
			if err != nil {
				return "", err
			}
			root, err := ws.open()
			if err != nil {
				return "", fmt.Errorf("write_file: %w", err)
			}
			defer root.Close()
			if err := ws.writeFile(root, name, []byte(params.Content)); err != nil {
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
		Kind:      ToolKindActivity,
		Sensitive: true,
		Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
			var params struct {
				Path      string `json:"path"`
				OldString string `json:"old_string"`
				NewString string `json:"new_string"`
			}
			if err := json.Unmarshal(input, &params); err != nil {
				return "", err
			}
			name, err := relPath(params.Path)
			if err != nil {
				return "", err
			}
			root, err := ws.open()
			if err != nil {
				return "", fmt.Errorf("edit_file: %w", err)
			}
			defer root.Close()
			// One descriptor to read and write back: the file edited is the
			// file read, whatever happens to its name in between.
			f, err := openRegular(root, name, os.O_RDWR)
			if err != nil {
				return "", fmt.Errorf("edit_file: %w", err)
			}
			defer f.Close()
			data, err := io.ReadAll(f)
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
			if err := f.Truncate(0); err != nil {
				return "", fmt.Errorf("edit_file: write: %w", err)
			}
			if _, err := f.WriteAt([]byte(newContent), 0); err != nil {
				return "", fmt.Errorf("edit_file: write: %w", err)
			}
			if err := f.Close(); err != nil {
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
			var params struct {
				Path string `json:"path"`
			}
			if err := json.Unmarshal(input, &params); err != nil {
				return "", err
			}
			if params.Path == "" {
				params.Path = "."
			}
			name, err := relPath(params.Path)
			if err != nil {
				return "", err
			}
			root, err := ws.open()
			if err != nil {
				return "", fmt.Errorf("list_directory: %w", err)
			}
			defer root.Close()
			entries, err := readDir(root, name)
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
