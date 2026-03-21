package tool

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func setupWorkspace(t *testing.T) (string, *Registry) {
	t.Helper()
	dir := t.TempDir()
	r := NewRegistry()
	RegisterFilesystemTools(r, dir)
	return dir, r
}

func execTool(t *testing.T, r *Registry, name string, params interface{}) (string, error) {
	t.Helper()
	input, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	return r.Execute(context.Background(), name, input)
}

func TestReadFile(t *testing.T) {
	dir, r := setupWorkspace(t)
	os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("hello world"), 0644)

	result, err := execTool(t, r, "read_file", map[string]string{"path": "hello.txt"})
	if err != nil {
		t.Fatal(err)
	}
	if result != "hello world" {
		t.Errorf("got %q, want %q", result, "hello world")
	}
}

func TestReadFile_NotFound(t *testing.T) {
	_, r := setupWorkspace(t)

	_, err := execTool(t, r, "read_file", map[string]string{"path": "nope.txt"})
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestReadFile_PathEscape(t *testing.T) {
	_, r := setupWorkspace(t)

	_, err := execTool(t, r, "read_file", map[string]string{"path": "../../etc/passwd"})
	if err == nil {
		t.Fatal("expected error for path escape")
	}
}

func TestWriteFile(t *testing.T) {
	dir, r := setupWorkspace(t)

	result, err := execTool(t, r, "write_file", map[string]string{
		"path":    "output.txt",
		"content": "written",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != "File written successfully." {
		t.Errorf("unexpected result: %s", result)
	}

	data, _ := os.ReadFile(filepath.Join(dir, "output.txt"))
	if string(data) != "written" {
		t.Errorf("file content = %q, want %q", data, "written")
	}
}

func TestWriteFile_CreatesDirs(t *testing.T) {
	dir, r := setupWorkspace(t)

	_, err := execTool(t, r, "write_file", map[string]string{
		"path":    "sub/dir/file.txt",
		"content": "deep",
	})
	if err != nil {
		t.Fatal(err)
	}

	data, _ := os.ReadFile(filepath.Join(dir, "sub", "dir", "file.txt"))
	if string(data) != "deep" {
		t.Errorf("got %q, want %q", data, "deep")
	}
}

func TestEditFile(t *testing.T) {
	dir, r := setupWorkspace(t)
	os.WriteFile(filepath.Join(dir, "code.go"), []byte("func old() {}"), 0644)

	_, err := execTool(t, r, "edit_file", map[string]string{
		"path":       "code.go",
		"old_string": "old",
		"new_string": "new",
	})
	if err != nil {
		t.Fatal(err)
	}

	data, _ := os.ReadFile(filepath.Join(dir, "code.go"))
	if string(data) != "func new() {}" {
		t.Errorf("got %q", data)
	}
}

func TestEditFile_NotFound(t *testing.T) {
	dir, r := setupWorkspace(t)
	os.WriteFile(filepath.Join(dir, "code.go"), []byte("func hello() {}"), 0644)

	_, err := execTool(t, r, "edit_file", map[string]string{
		"path":       "code.go",
		"old_string": "nonexistent",
		"new_string": "x",
	})
	if err == nil {
		t.Fatal("expected error for old_string not found")
	}
}

func TestEditFile_Ambiguous(t *testing.T) {
	dir, r := setupWorkspace(t)
	os.WriteFile(filepath.Join(dir, "dup.go"), []byte("aaa aaa"), 0644)

	_, err := execTool(t, r, "edit_file", map[string]string{
		"path":       "dup.go",
		"old_string": "aaa",
		"new_string": "bbb",
	})
	if err == nil {
		t.Fatal("expected error for ambiguous old_string")
	}
}

func TestListDirectory(t *testing.T) {
	dir, r := setupWorkspace(t)
	os.WriteFile(filepath.Join(dir, "a.txt"), []byte(""), 0644)
	os.Mkdir(filepath.Join(dir, "subdir"), 0755)

	result, err := execTool(t, r, "list_directory", map[string]string{})
	if err != nil {
		t.Fatal(err)
	}

	if !contains(result, "a.txt") {
		t.Errorf("expected a.txt in result: %s", result)
	}
	if !contains(result, "subdir/") {
		t.Errorf("expected subdir/ in result: %s", result)
	}
}

func TestListDirectory_Subdir(t *testing.T) {
	dir, r := setupWorkspace(t)
	sub := filepath.Join(dir, "mydir")
	os.Mkdir(sub, 0755)
	os.WriteFile(filepath.Join(sub, "inner.txt"), []byte(""), 0644)

	result, err := execTool(t, r, "list_directory", map[string]string{"path": "mydir"})
	if err != nil {
		t.Fatal(err)
	}

	if !contains(result, "inner.txt") {
		t.Errorf("expected inner.txt in result: %s", result)
	}
}

func TestSafePath_Escape(t *testing.T) {
	workspace := "/tmp/test-workspace"
	_, err := safePath(workspace, "../../../etc/passwd")
	if err == nil {
		t.Fatal("expected error for path traversal")
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsStr(s, sub))
}

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
