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
	"time"

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
	// One file per call and name, as the store keeps them.
	for _, stored := range s.files {
		if stored.TurnKey == f.TurnKey && stored.CallID == f.CallID && stored.Name == f.Name {
			if stored.SHA256 != f.SHA256 {
				return store.File{}, store.ErrFileExists
			}
			return stored, nil
		}
	}
	if s.contents == nil {
		s.contents = map[string][]byte{}
	}
	s.files = append(s.files, f)
	s.contents[f.ID] = content
	return f, nil
}

var turnCall = CallContext{Turn: &TurnRef{SessionID: "s1", TurnKey: "m7.jarvis"}, CallID: "toolu_1"}

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
	if f.SessionID != "s1" || f.TurnKey != "m7.jarvis" || f.CallID != "toolu_1" || f.AgentID != "jarvis" || f.UserID != "u-alice" ||
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
		want                string
	}{
		// A scheduled task's call: an ID, no session turn.
		{name: "no session turn", file: "a.txt", cc: CallContext{CallID: "toolu_1"}, want: "no session turn"},
		{name: "a path", file: "../etc/passwd", cc: turnCall, want: "is a path"},
		{name: "a backslash", file: `a\b.txt`, cc: turnCall, want: "is a path"},
		{name: "dot dot", file: "..", cc: turnCall, want: "not a file name"},
		{name: "empty", file: "  ", cc: turnCall, want: "needs a name"},
		{name: "a control character", file: "a\nb.txt", cc: turnCall, want: "control"},
		{name: "a right-to-left override", file: "rapport‮fdp.exe", cc: turnCall, want: "control"},
		{name: "too long", file: strings.Repeat("a", 252) + ".txt", cc: turnCall, want: "too long"},
		{name: "too large", file: "a.txt", content: strings.Repeat("x", MaxTextFileBytes+1), cc: turnCall, want: "too large"},
		{name: "session deleted", file: "a.txt", cc: turnCall, saver: &fileSaver{gone: true}, want: "session was deleted"},
	} {
		t.Run(c.name, func(t *testing.T) {
			saver := c.saver
			if saver == nil {
				saver = &fileSaver{}
			}
			_, err, pub := publishFile(t, &Publisher{Store: saver}, c.cc, c.file, c.content)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("error %v, want %q", err, c.want)
			}
			if len(saver.files) != 0 || len(pub.Files()) != 0 {
				t.Errorf("published %+v", saver.files)
			}
		})
	}
}

// publish_file has a bound of its own, low: FILES_MAX_BYTES is exec's.
func TestPublishFile_AtTheLimit(t *testing.T) {
	saver := &fileSaver{}
	if _, err, _ := publishFile(t, &Publisher{Store: saver, MaxBytes: 4}, turnCall, "a.txt", strings.Repeat("x", MaxTextFileBytes)); err != nil {
		t.Fatal(err)
	}
}

// A call the workflow gave no context (its catalog said the tool needs
// none) is told to try again, not that it belongs to no session.
func TestPublishFile_WithoutACallContext(t *testing.T) {
	r := NewRegistry()
	saver := &fileSaver{}
	RegisterPublishFileTool(r, &Publisher{Store: saver})
	_, err := r.Execute(context.Background(), "publish_file", json.RawMessage(`{"name":"a.txt","content":"x"}`))
	if err == nil || !strings.Contains(err.Error(), "could not tell which turn") || len(saver.files) != 0 {
		t.Errorf("error %v, saved %+v", err, saver.files)
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
	// The files first: a summary that clips a long output keeps their names.
	if !strings.HasPrefix(out, "Files published") || !strings.HasSuffix(out, "\n\ndone") || !strings.Contains(out, "data.csv (4 B, id ") {
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
	if !strings.HasSuffix(out, "\n\nran") {
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
	if !strings.Contains(out, "\n\nCommand failed") || !strings.Contains(out, "log.txt (8 B") || len(saver.files) != 1 {
		t.Errorf("result %q, saved %+v", out, saver.files)
	}
}

// Outside a session turn, the command runs and says why nothing was
// published.
func TestExec_PublishWithoutATurn(t *testing.T) {
	_, r, saver := setupPublishingExec(t, 0)
	input, _ := json.Marshal(map[string]any{"command": "echo x > a.txt && echo ran", "publish": []string{"a.txt"}})
	ctx, _ := callCtx(CallContext{CallID: "toolu_1"})
	out, err := r.Execute(ctx, "exec", input)
	if err != nil || !strings.HasSuffix(out, "ran") || !strings.Contains(out, "no session turn") || len(saver.files) != 0 {
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

// slowSaver stores its first file, then takes until the call's end.
type slowSaver struct{ fileSaver }

func (s *slowSaver) SaveFile(ctx context.Context, f store.File, content []byte) (store.File, error) {
	if len(s.files) == 0 {
		return s.fileSaver.SaveFile(ctx, f, content)
	}
	<-ctx.Done()
	return store.File{}, ctx.Err()
}

// Publishing has a budget of its own: what it leaves is said not published,
// and the call returns, with what was stored.
func TestExec_PublishWithinItsBudget(t *testing.T) {
	id := subproctest.Identity(t)
	dir := subproctest.Dir(t, id)
	r := NewRegistry()
	saver := &slowSaver{}
	RegisterExecTool(r, dir, id, subproc.NewRuns(id), &Publisher{Store: saver, budget: 200 * time.Millisecond})
	start := time.Now()
	out, err, pub := execPublishing(t, r, "echo a > a.txt; echo b > b.txt; echo c > c.txt; echo ran", "a.txt", "b.txt", "c.txt")
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 5*time.Second {
		t.Errorf("publishing took %s", time.Since(start))
	}
	if len(saver.files) != 1 || len(pub.Files()) != 1 || !strings.Contains(out, "a.txt (2 B") {
		t.Errorf("published %+v: %q", saver.files, out)
	}
	for _, path := range []string{"b.txt", "c.txt"} {
		if !strings.Contains(out, "- "+path+": not published: the 200ms given to publish ran out") {
			t.Errorf("%s not said out of time: %q", path, out)
		}
	}
	if !strings.HasSuffix(out, "ran") {
		t.Errorf("the command's output is gone: %q", out)
	}
}

// exec with no call context runs its command and says why nothing was
// published.
func TestExec_PublishWithoutACallContext(t *testing.T) {
	_, r, saver := setupPublishingExec(t, 0)
	input, _ := json.Marshal(map[string]any{"command": "echo x > a.txt && echo ran", "publish": []string{"a.txt"}})
	out, err := r.Execute(context.Background(), "exec", input)
	if err != nil || !strings.HasSuffix(out, "ran") || !strings.Contains(out, "could not tell which turn") || len(saver.files) != 0 {
		t.Errorf("result %q, %v, saved %+v", out, err, saver.files)
	}
}

// A call publishing a name twice: the same content is one file; other
// content is refused, never lost behind the first.
func TestPublishFile_OneNamePerCall(t *testing.T) {
	saver := &fileSaver{}
	p := &Publisher{Store: saver}
	if _, err, _ := publishFile(t, p, turnCall, "a.md", "one"); err != nil {
		t.Fatal(err)
	}
	if _, err, _ := publishFile(t, p, turnCall, "a.md", "one"); err != nil || len(saver.files) != 1 {
		t.Errorf("same content again: %v, saved %d", err, len(saver.files))
	}
	_, err, _ := publishFile(t, p, turnCall, "a.md", "two")
	if err == nil || !strings.Contains(err.Error(), "a file named a.md was already published by this call with different content: rename one of them") {
		t.Errorf("other content: %v", err)
	}
	// A call with no ID is refused, as one with no context.
	noID := turnCall
	noID.CallID = ""
	if _, err, _ := publishFile(t, p, noID, "b.md", "x"); err == nil || !strings.Contains(err.Error(), "could not tell which turn") {
		t.Errorf("no call ID: %v", err)
	}
}

// Two paths with one base name: the first is published, the second refused
// before it is read.
func TestExec_PublishTwoPathsOneName(t *testing.T) {
	_, r, saver := setupPublishingExec(t, 0)
	out, err, _ := execPublishing(t, r, "mkdir -p out tmp && echo 1 > out/a.csv && echo 22 > tmp/a.csv && echo ran", "out/a.csv", "tmp/a.csv")
	if err != nil {
		t.Fatal(err)
	}
	if len(saver.files) != 1 || string(saver.contents[saver.files[0].ID]) != "1\n" {
		t.Errorf("saved %+v", saver.files)
	}
	if !strings.Contains(out, "- tmp/a.csv: a file named a.csv was already published by this call with different content: rename one of them") {
		t.Errorf("the second path is not refused: %q", out)
	}
}
