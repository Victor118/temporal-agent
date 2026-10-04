package tool

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/subproc"
	"github.com/victor/temporal-agent/subproc/subproctest"
)

// fileSaver keeps what is published in memory.
type fileSaver struct {
	files    []store.File
	contents map[string][]byte
	gone     bool // the session was deleted
}

func (s *fileSaver) SaveFile(_ context.Context, f store.File, content []byte) (store.File, error) {
	if s.gone {
		return store.File{}, store.ErrFileSessionGone
	}
	if s.contents == nil {
		s.contents = map[string][]byte{}
	}
	s.files = append(s.files, f)
	s.contents[f.ID] = content
	return f, nil
}

var turnCall = CallContext{Turn: &TurnRef{SessionID: "s1", TurnKey: "m7.jarvis"}}

// callCtx is the context of a call from a session turn of jarvis's,
// answering alice, with a collector of what it publishes.
func callCtx(cc CallContext) (context.Context, *Published) {
	ctx := WithCall(WithUserID(WithAgentID(context.Background(), "jarvis"), "u-alice"), cc)
	return WithPublished(ctx)
}

func publishFile(t *testing.T, p *Publisher, cc CallContext, name, content string) (string, error, *Published) {
	t.Helper()
	r := NewRegistry()
	RegisterPublishFileTool(r, p)
	input, _ := json.Marshal(map[string]string{"name": name, "content": content})
	ctx, pub := callCtx(cc)
	out, err := r.Execute(ctx, "publish_file", input)
	return out, err, pub
}

func TestPublishFile(t *testing.T) {
	saver := &fileSaver{}
	out, err, pub := publishFile(t, &Publisher{Store: saver}, turnCall, "  rapport.md ", "# Rapport\n")
	if err != nil {
		t.Fatal(err)
	}
	if len(saver.files) != 1 {
		t.Fatalf("saved %+v", saver.files)
	}
	f := saver.files[0]
	sum := sha256.Sum256([]byte("# Rapport\n"))
	if f.SessionID != "s1" || f.TurnKey != "m7.jarvis" || f.AgentID != "jarvis" || f.UserID != "u-alice" ||
		f.Name != "rapport.md" || f.Size != 10 || f.SHA256 != hex.EncodeToString(sum[:]) ||
		!strings.HasPrefix(f.ContentType, "text/markdown") || f.ID == "" {
		t.Errorf("saved %+v", f)
	}
	if !bytes.Equal(saver.contents[f.ID], []byte("# Rapport\n")) {
		t.Errorf("content %q", saver.contents[f.ID])
	}
	// The model reads the name, the size and the ID.
	if !strings.Contains(out, "rapport.md (10 B, id "+f.ID+")") {
		t.Errorf("result %q", out)
	}
	// The activity returns the reference, never the content.
	if refs := pub.Files(); len(refs) != 1 || refs[0].ID != f.ID || refs[0].Size != 10 || refs[0].Name != "rapport.md" {
		t.Errorf("references %+v", refs)
	}
}

func TestPublishFile_Refusals(t *testing.T) {
	for _, c := range []struct {
		name, file, content string
		cc                  CallContext
		saver               *fileSaver
		max                 int64
		want                string
	}{
		{name: "no session turn", file: "a.txt", cc: CallContext{}, want: "no session turn"},
		{name: "a path", file: "../etc/passwd", cc: turnCall, want: "is a path"},
		{name: "a backslash", file: `a\b.txt`, cc: turnCall, want: "is a path"},
		{name: "dot dot", file: "..", cc: turnCall, want: "not a file name"},
		{name: "empty", file: "  ", cc: turnCall, want: "needs a name"},
		{name: "a control character", file: "a\nb.txt", cc: turnCall, want: "control"},
		{name: "a right-to-left override", file: "rapport‮fdp.exe", cc: turnCall, want: "control"},
		{name: "too long", file: strings.Repeat("a", 252) + ".txt", cc: turnCall, want: "too long"},
		{name: "too large", file: "a.txt", content: "12345", max: 4, cc: turnCall, want: "too large"},
		{name: "session deleted", file: "a.txt", cc: turnCall, saver: &fileSaver{gone: true}, want: "session was deleted"},
	} {
		t.Run(c.name, func(t *testing.T) {
			saver := c.saver
			if saver == nil {
				saver = &fileSaver{}
			}
			_, err, pub := publishFile(t, &Publisher{Store: saver, MaxBytes: c.max}, c.cc, c.file, c.content)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("error %v, want %q", err, c.want)
			}
			if len(saver.files) != 0 || len(pub.Files()) != 0 {
				t.Errorf("published %+v", saver.files)
			}
		})
	}
}

// A file at the size limit is published; one byte more is not.
func TestPublishFile_AtTheLimit(t *testing.T) {
	saver := &fileSaver{}
	if _, err, _ := publishFile(t, &Publisher{Store: saver, MaxBytes: 4}, turnCall, "a.txt", "1234"); err != nil {
		t.Fatal(err)
	}
}

// setupPublishingExec is exec with a publisher, as a worker registers it.
func setupPublishingExec(t *testing.T, max int64) (string, *Registry, *fileSaver) {
	t.Helper()
	id := subproctest.Identity(t)
	dir := subproctest.Dir(t, id)
	r := NewRegistry()
	saver := &fileSaver{}
	RegisterExecTool(r, dir, id, subproc.NewRuns(id), &Publisher{Store: saver, MaxBytes: max})
	return dir, r, saver
}

func execPublishing(t *testing.T, r *Registry, command string, paths ...string) (string, error, *Published) {
	t.Helper()
	input, _ := json.Marshal(map[string]any{"command": command, "publish": paths})
	ctx, pub := callCtx(turnCall)
	out, err := r.Execute(ctx, "exec", input)
	return out, err, pub
}

func TestExec_Publish(t *testing.T) {
	_, r, saver := setupPublishingExec(t, 0)
	out, err, pub := execPublishing(t, r, "mkdir -p out && printf 'a,b\\n' > out/data.csv && echo done", "out/data.csv")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out, "done") || !strings.Contains(out, "Files published") || !strings.Contains(out, "data.csv (4 B, id ") {
		t.Errorf("result %q", out)
	}
	if len(saver.files) != 1 || saver.files[0].Name != "data.csv" || saver.files[0].TurnKey != "m7.jarvis" ||
		string(saver.contents[saver.files[0].ID]) != "a,b\n" {
		t.Errorf("saved %+v", saver.files)
	}
	if len(pub.Files()) != 1 {
		t.Errorf("references %+v", pub.Files())
	}
}

// What cannot be published is said, next to the command's output and what
// could: a failure does not hide the rest.
func TestExec_PublishPartly(t *testing.T) {
	dir, r, saver := setupPublishingExec(t, 8)
	outside := t.TempDir()
	os.WriteFile(filepath.Join(outside, "secret"), []byte("s"), 0o644)
	os.Symlink(filepath.Join(outside, "secret"), filepath.Join(dir, "abs-link"))
	os.Symlink("../"+filepath.Base(outside)+"/secret", filepath.Join(dir, "rel-link"))
	os.Mkdir(filepath.Join(dir, "sub"), 0o755)
	if err := syscall.Mkfifo(filepath.Join(dir, "fifo"), 0o644); err != nil {
		t.Fatal(err)
	}

	out, err, _ := execPublishing(t, r, "echo ok > good.txt && echo 123456789 > big.txt && echo ran",
		"good.txt", "missing.txt", "/etc/passwd", "../x", "abs-link", "rel-link", "sub", "fifo", "big.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out, "ran") {
		t.Errorf("the command's output is gone: %q", out)
	}
	if len(saver.files) != 1 || saver.files[0].Name != "good.txt" {
		t.Errorf("published %+v", saver.files)
	}
	published, failed, _ := strings.Cut(out, "Files not published:")
	if !strings.Contains(published, "good.txt (3 B") {
		t.Errorf("good.txt not reported published: %q", out)
	}
	for _, path := range []string{"missing.txt", "/etc/passwd: absolute path", "../x: path escapes", "abs-link", "rel-link", "sub: ", "fifo: ", "big.txt: too large"} {
		if !strings.Contains(failed, "- "+path) {
			t.Errorf("%s not reported refused: %q", path, out)
		}
	}
}

// A command that fails publishes what it was asked to all the same.
func TestExec_PublishAfterAFailure(t *testing.T) {
	_, r, saver := setupPublishingExec(t, 0)
	out, err, _ := execPublishing(t, r, "echo partial > log.txt; exit 3", "log.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out, "Command failed") || !strings.Contains(out, "log.txt (8 B") || len(saver.files) != 1 {
		t.Errorf("result %q, saved %+v", out, saver.files)
	}
}

// Outside a session turn, the command runs and says why nothing was
// published.
func TestExec_PublishWithoutATurn(t *testing.T) {
	_, r, saver := setupPublishingExec(t, 0)
	input, _ := json.Marshal(map[string]any{"command": "echo x > a.txt && echo ran", "publish": []string{"a.txt"}})
	ctx, _ := callCtx(CallContext{})
	out, err := r.Execute(ctx, "exec", input)
	if err != nil || !strings.HasPrefix(out, "ran") || !strings.Contains(out, "no session turn") || len(saver.files) != 0 {
		t.Errorf("result %q, %v, saved %+v", out, err, saver.files)
	}
}

func TestFormatSize(t *testing.T) {
	for n, want := range map[int64]string{0: "0 B", 1023: "1023 B", 1536: "1.5 KB", 3 << 20: "3.0 MB"} {
		if got := FormatSize(n); got != want {
			t.Errorf("FormatSize(%d) = %q, want %q", n, got, want)
		}
	}
}
