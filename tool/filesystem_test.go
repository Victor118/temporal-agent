package tool

import (
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

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

func TestRelPath_Escape(t *testing.T) {
	for _, p := range []string{"../../../etc/passwd", "a/../../b", "/../x", ".."} {
		if _, err := relPath(p); err == nil {
			t.Errorf("relPath(%q) climbed out of the workspace", p)
		}
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

// A sibling directory sharing the workspace's prefix is outside it; an
// absolute path is taken relative to the workspace.
func TestRelPath(t *testing.T) {
	parent := t.TempDir()
	workspace := filepath.Join(parent, "workspace")
	sibling := filepath.Join(parent, "workspace2")
	os.MkdirAll(workspace, 0o755)
	os.MkdirAll(sibling, 0o755)
	os.WriteFile(filepath.Join(sibling, "secret"), []byte("s"), 0o644)
	r := NewRegistry()
	RegisterFilesystemTools(r, workspace, nil)

	if out, err := execTool(t, r, "read_file", map[string]string{"path": "../workspace2/secret"}); err == nil {
		t.Errorf("reached a sibling directory sharing the workspace's prefix: %q", out)
	}
	for p, want := range map[string]string{"a/../b.txt": "b.txt", "/etc/passwd": "etc/passwd", "": ".", "//a//b/": "a/b"} {
		if got, err := relPath(p); err != nil || got != want {
			t.Errorf("relPath(%q) = %q, %v; want %q", p, got, err, want)
		}
	}
}

// A symbolic link leading out of the workspace is refused, through a file, a
// directory, or a link whose target does not exist yet; one staying inside is
// followed, if its target is relative (os.Root refuses an absolute one).
func TestFileTools_Symlinks(t *testing.T) {
	workspace, r := setupWorkspace(t)
	outside := t.TempDir()
	os.WriteFile(filepath.Join(outside, "secret"), []byte("secret"), 0o644)
	os.Symlink(filepath.Join(outside, "secret"), filepath.Join(workspace, "file-link"))
	os.Symlink(outside, filepath.Join(workspace, "dir-link"))
	rel, _ := filepath.Rel(workspace, outside)
	os.Symlink(rel, filepath.Join(workspace, "rel-dir-link"))
	os.Symlink(filepath.Join(outside, "new"), filepath.Join(workspace, "dangling"))
	os.WriteFile(filepath.Join(workspace, "inside.txt"), []byte("ok"), 0o644)
	os.Symlink("inside.txt", filepath.Join(workspace, "inner-link"))
	os.Symlink(filepath.Join(workspace, "inside.txt"), filepath.Join(workspace, "abs-inner-link"))

	for _, p := range []string{"file-link", "dir-link/secret", "dir-link/new.txt", "rel-dir-link/secret", "abs-inner-link"} {
		if out, err := execTool(t, r, "read_file", map[string]string{"path": p}); err == nil {
			t.Errorf("read %s through a link: %q", p, out)
		}
	}
	for _, p := range []string{"dangling", "dir-link/new", "rel-dir-link/new", "rel-dir-link/sub/new"} {
		if _, err := execTool(t, r, "write_file", map[string]string{"path": p, "content": "x"}); err == nil {
			t.Errorf("wrote %s through a link", p)
		}
	}
	if _, err := execTool(t, r, "edit_file", map[string]string{"path": "file-link", "old_string": "secret", "new_string": "x"}); err == nil {
		t.Error("edited a file through a link")
	}
	if _, err := execTool(t, r, "list_directory", map[string]string{"path": "rel-dir-link"}); err == nil {
		t.Error("listed a directory through a link")
	}
	entries, _ := os.ReadDir(outside)
	if len(entries) != 1 {
		t.Errorf("something was created outside the workspace: %v", entries)
	}
	if out, err := execTool(t, r, "read_file", map[string]string{"path": "inner-link"}); err != nil || out != "ok" {
		t.Errorf("a link inside the workspace: %q, %v", out, err)
	}
}

// A path that was a file on one call and is a link out of the workspace on
// the next is refused on the next: nothing is remembered, nothing checked
// apart from the open.
func TestFileTools_ALinkSwappedInAfterACall(t *testing.T) {
	workspace, r := setupWorkspace(t)
	outside := t.TempDir()
	secret := filepath.Join(outside, "secret")
	os.WriteFile(secret, []byte("secret"), 0o644)
	os.WriteFile(filepath.Join(workspace, "a.txt"), []byte("a"), 0o644)
	os.MkdirAll(filepath.Join(workspace, "d"), 0o755)

	if _, err := execTool(t, r, "read_file", map[string]string{"path": "a.txt"}); err != nil {
		t.Fatal(err)
	}
	if _, err := execTool(t, r, "write_file", map[string]string{"path": "d/b.txt", "content": "b"}); err != nil {
		t.Fatal(err)
	}
	os.Remove(filepath.Join(workspace, "a.txt"))
	os.Symlink(secret, filepath.Join(workspace, "a.txt"))
	os.RemoveAll(filepath.Join(workspace, "d"))
	os.Symlink(outside, filepath.Join(workspace, "d"))

	if out, err := execTool(t, r, "read_file", map[string]string{"path": "a.txt"}); err == nil {
		t.Errorf("read through a link swapped in: %q", out)
	}
	if _, err := execTool(t, r, "write_file", map[string]string{"path": "a.txt", "content": "pwned"}); err == nil {
		t.Error("wrote through a link swapped in")
	}
	if _, err := execTool(t, r, "write_file", map[string]string{"path": "d/b.txt", "content": "pwned"}); err == nil {
		t.Error("wrote through a directory swapped for a link")
	}
	if data, _ := os.ReadFile(secret); string(data) != "secret" {
		t.Errorf("the file outside became %q", data)
	}
	if _, err := os.Stat(filepath.Join(outside, "b.txt")); err == nil {
		t.Error("a file was created outside the workspace")
	}
}

// A process of the workspace's owner swaps a file and a directory for links
// out of it, back and forth, while the tools work: whatever the timing, they
// never read nor write outside. Checking a path and then opening it would
// lose this race now and then; opening through os.Root does not have a
// moment to lose it in.
func TestFileTools_RaceAgainstASwappedLink(t *testing.T) {
	workspace, r := setupWorkspace(t)
	RegisterGrepTool(r, workspace)
	outside := t.TempDir()
	secret := filepath.Join(outside, "secret")
	os.WriteFile(secret, []byte("SECRET"), 0o644)
	rel, _ := filepath.Rel(workspace, outside)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		file, dir := filepath.Join(workspace, "a.txt"), filepath.Join(workspace, "d")
		tmp := filepath.Join(workspace, "tmp")
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			os.RemoveAll(tmp)
			if i%2 == 0 {
				os.WriteFile(tmp, []byte("inside"), 0o644)
				os.Rename(tmp, file)
				os.RemoveAll(dir)
				os.Mkdir(dir, 0o755)
				os.WriteFile(filepath.Join(dir, "secret"), []byte("inside"), 0o644)
			} else {
				os.Symlink(filepath.Join(rel, "secret"), tmp)
				os.Rename(tmp, file)
				os.RemoveAll(dir)
				os.Symlink(rel, dir)
			}
		}
	}()

	deadline := time.Now().Add(2 * time.Second)
	for n := 0; time.Now().Before(deadline); n++ {
		for _, p := range []string{"a.txt", "d/secret"} {
			if out, err := execTool(t, r, "read_file", map[string]string{"path": p}); err == nil && strings.Contains(out, "SECRET") {
				t.Fatalf("read %s outside the workspace after %d calls", p, n)
			}
		}
		if out, _ := execTool(t, r, "grep", map[string]string{"pattern": "SECRET"}); strings.Contains(out, "SECRET") {
			t.Fatalf("grep read outside the workspace after %d calls: %q", n, out)
		}
		execTool(t, r, "write_file", map[string]string{"path": "a.txt", "content": "pwned"})
		execTool(t, r, "write_file", map[string]string{"path": "d/secret", "content": "pwned"})
		execTool(t, r, "edit_file", map[string]string{"path": "a.txt", "old_string": "SECRET", "new_string": "pwned"})
		if data, _ := os.ReadFile(secret); string(data) != "SECRET" {
			t.Fatalf("wrote outside the workspace after %d calls: %q", n, data)
		}
	}
	close(stop)
	<-done
}

// A FIFO in the workspace is refused, not read until the activity times out.
func TestFileTools_FIFO(t *testing.T) {
	workspace, r := setupWorkspace(t)
	if err := syscall.Mkfifo(filepath.Join(workspace, "fifo"), 0o644); err != nil {
		t.Skip(err)
	}
	errs := make(chan error, 3)
	go func() {
		for _, tool := range []string{"read_file", "edit_file", "list_directory"} {
			_, err := execTool(t, r, tool, map[string]string{"path": "fifo", "old_string": "a", "new_string": "b"})
			errs <- err
		}
	}()
	for i := 0; i < 3; i++ {
		select {
		case err := <-errs:
			if err == nil {
				t.Error("a FIFO was accepted")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("a tool blocked on a FIFO")
		}
	}
}

// No file tool reaches the workspace by a path it joined itself: every one of
// them goes through os.Root (workspace.go). A call to one of these in their
// sources would bring back the moment between a check and an open.
func TestFileTools_OnlyThroughTheRoot(t *testing.T) {
	forbidden := map[string]map[string]bool{
		"os": {"ReadFile": true, "WriteFile": true, "Open": true, "OpenFile": true, "Create": true,
			"ReadDir": true, "Stat": true, "Lstat": true, "Mkdir": true, "MkdirAll": true,
			"Remove": true, "RemoveAll": true, "Rename": true, "Chown": true, "Lchown": true,
			"Chmod": true, "Truncate": true, "Readlink": true, "Symlink": true, "Link": true, "DirFS": true},
		"filepath": {"Walk": true, "WalkDir": true, "EvalSymlinks": true, "Glob": true},
	}
	fset := token.NewFileSet()
	for _, name := range []string{"filesystem.go", "grep.go", "glob.go", "workspace.go"} {
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if pkg, ok := sel.X.(*ast.Ident); ok && forbidden[pkg.Name][sel.Sel.Name] {
				t.Errorf("%s: %s.%s reaches the workspace by name", fset.Position(sel.Pos()), pkg.Name, sel.Sel.Name)
			}
			if sel.Sel.Name == "Give" {
				t.Errorf("%s: Give hands over a tree by name; GiveFile acts on what was opened", fset.Position(sel.Pos()))
			}
			return true
		})
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
