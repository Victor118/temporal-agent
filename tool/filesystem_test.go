package tool

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/victor/temporal-agent/subproc"
)

func setupWorkspace(t *testing.T) (string, *Registry) {
	t.Helper()
	dir := t.TempDir()
	r := NewRegistry()
	RegisterFilesystemTools(r, dir, nil)
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

// A sibling directory sharing the workspace's prefix is outside it.
func TestSafePath_SiblingPrefix(t *testing.T) {
	parent := t.TempDir()
	workspace := filepath.Join(parent, "workspace")
	sibling := filepath.Join(parent, "workspace2")
	os.MkdirAll(workspace, 0o755)
	os.MkdirAll(sibling, 0o755)
	os.WriteFile(filepath.Join(sibling, "secret"), []byte("s"), 0o644)

	if _, err := safePath(workspace, "../workspace2/secret"); err == nil {
		t.Error("reached a sibling directory sharing the workspace's prefix")
	}
	if p, err := safePath(workspace, "a/../b.txt"); err != nil || p != filepath.Join(workspace, "b.txt") {
		t.Errorf("a path inside: %q, %v", p, err)
	}
	if p, err := safePath(workspace, "/etc/passwd"); err != nil || p != filepath.Join(workspace, "etc/passwd") {
		t.Errorf("an absolute path stays under the workspace: %q, %v", p, err)
	}
}

// A symbolic link leading out of the workspace is refused, through a file, a
// directory, or a link whose target does not exist yet.
func TestSafePath_Symlinks(t *testing.T) {
	workspace, r := setupWorkspace(t)
	outside := t.TempDir()
	os.WriteFile(filepath.Join(outside, "secret"), []byte("secret"), 0o644)
	os.Symlink(filepath.Join(outside, "secret"), filepath.Join(workspace, "file-link"))
	os.Symlink(outside, filepath.Join(workspace, "dir-link"))
	os.Symlink(filepath.Join(outside, "new"), filepath.Join(workspace, "dangling"))
	os.WriteFile(filepath.Join(workspace, "inside.txt"), []byte("ok"), 0o644)
	os.Symlink(filepath.Join(workspace, "inside.txt"), filepath.Join(workspace, "inner-link"))

	for _, p := range []string{"file-link", "dir-link/secret", "dir-link/new.txt"} {
		if out, err := execTool(t, r, "read_file", map[string]string{"path": p}); err == nil {
			t.Errorf("read %s through a link: %q", p, out)
		}
	}
	if _, err := execTool(t, r, "write_file", map[string]string{"path": "dangling", "content": "x"}); err == nil {
		t.Error("wrote through a dangling link")
	}
	if _, err := os.Stat(filepath.Join(outside, "new")); err == nil {
		t.Error("a file was created outside the workspace")
	}
	if out, err := execTool(t, r, "read_file", map[string]string{"path": "inner-link"}); err != nil || out != "ok" {
		t.Errorf("a link inside the workspace: %q, %v", out, err)
	}
}

// What write_file creates belongs to the user exec runs as, so that its
// commands can change it; what was there keeps its owner.
func TestWriteFile_GivesWhatItCreatesToTheOwner(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("giving files away takes root")
	}
	dir := t.TempDir()
	owner := &subproc.Identity{UID: 65534, GID: 65534}
	r := NewRegistry()
	RegisterFilesystemTools(r, dir, owner)
	os.WriteFile(filepath.Join(dir, "kept.txt"), []byte("x"), 0o644)

	for _, path := range []string{"a/b/new.txt", "kept.txt"} {
		input, _ := json.Marshal(map[string]string{"path": path, "content": "y"})
		if _, err := r.Execute(context.Background(), "write_file", input); err != nil {
			t.Fatal(err)
		}
	}
	uid := func(rel string) uint32 {
		fi, err := os.Lstat(filepath.Join(dir, rel))
		if err != nil {
			t.Fatal(err)
		}
		return fi.Sys().(*syscall.Stat_t).Uid
	}
	for _, rel := range []string{"a", "a/b", "a/b/new.txt"} {
		if uid(rel) != 65534 {
			t.Errorf("%s belongs to %d, want the owner", rel, uid(rel))
		}
	}
	if uid(".") != 0 || uid("kept.txt") != 0 {
		t.Error("write_file gave away what it did not create")
	}
}
