//go:build unix

package outputs

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/victor/temporal-agent/machine"
)

type published struct {
	mu    sync.Mutex
	files map[string]string
}

func (p *published) upload(_ context.Context, name string, content []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.files == nil {
		p.files = map[string]string{}
	}
	p.files[name] = string(content)
	return nil
}

// What a run leaves in its outputs is published within bounds: regular
// files only, never through a link nor a file another path shares, at most
// so many, so deep, so large, one per name.
func TestPublish(t *testing.T) {
	dir := t.TempDir()
	write := func(p, content string) {
		os.MkdirAll(filepath.Dir(filepath.Join(dir, p)), 0o700)
		if err := os.WriteFile(filepath.Join(dir, p), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	secret := filepath.Join(t.TempDir(), "id_ed25519")
	os.WriteFile(secret, []byte("PRIVATE KEY"), 0o600)
	write("report.md", "# Report")
	write("sub/data.csv", "a,b")
	write("sub/report.md", "another")
	write("big.bin", strings.Repeat("x", 33))
	write("a/b/c/d/deep.txt", "too deep")
	os.Symlink(secret, filepath.Join(dir, "key"))
	os.Symlink("/etc", filepath.Join(dir, "etc"))
	os.Link(secret, filepath.Join(dir, "linked"))
	exec.Command("mkfifo", filepath.Join(dir, "fifo")).Run()

	pub := &published{}
	refused := Publish(context.Background(), dir, 32, pub.upload)
	if len(pub.files) != 2 || pub.files["report.md"] != "# Report" || pub.files["data.csv"] != "a,b" {
		t.Errorf("published %v", pub.files)
	}
	said := strings.Join(refused, "\n")
	for _, want := range []string{"big.bin: too large", "a/b/c/d/: more than", "key: a link", "etc: a link", "linked: another path shares",
		"sub/report.md: another file is published as report.md"} {
		if !strings.Contains(said, want) {
			t.Errorf("refusals lack %q:\n%s", want, said)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "fifo")); err == nil && !strings.Contains(said, "fifo: not a regular file") {
		t.Errorf("a FIFO:\n%s", said)
	}
	if strings.Contains(fmt.Sprint(pub.files), "PRIVATE KEY") {
		t.Error("a secret went out")
	}

	// So many files: the first ones only.
	many := t.TempDir()
	for i := range machine.MaxOutputFiles + 3 {
		os.WriteFile(filepath.Join(many, fmt.Sprintf("f%02d.txt", i)), []byte("x"), 0o600)
	}
	pub = &published{}
	refused = Publish(context.Background(), many, 32, pub.upload)
	if len(pub.files) != machine.MaxOutputFiles || len(refused) != 3 || !strings.Contains(refused[0], "at most") {
		t.Errorf("%d published, refused %v", len(pub.files), refused)
	}

	// A run stopped, or out of time, publishes nothing, and says so.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pub = &published{}
	if refused := Publish(ctx, dir, 32, pub.upload); len(pub.files) != 0 || len(refused) != 1 || !strings.Contains(refused[0], "stopped") {
		t.Errorf("cancelled: %v %v", pub.files, refused)
	}
	ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if refused := Publish(ctx, dir, 32, pub.upload); len(pub.files) != 0 || len(refused) != 1 || !strings.Contains(refused[0], "time ran out") {
		t.Errorf("out of time: %v %v", pub.files, refused)
	}
}

// No outputs directory: nothing to say.
func TestPublish_NoDirectory(t *testing.T) {
	if refused := Publish(context.Background(), filepath.Join(t.TempDir(), "none"), 32, nil); refused != nil {
		t.Errorf("refused %v", refused)
	}
}

// A reserved name is not the run's: refused, wherever the file is, and said.
func TestPublish_Reserved(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "sub"), 0o700)
	os.WriteFile(filepath.Join(dir, "sub", "agent-x-1.bundle"), []byte("fake"), 0o600)
	os.WriteFile(filepath.Join(dir, "notes.md"), []byte("ok"), 0o600)
	pub := &published{}
	refused := Publish(context.Background(), dir, 32, pub.upload, "agent-x-1.bundle")
	if len(pub.files) != 1 || pub.files["notes.md"] != "ok" || len(refused) != 1 ||
		refused[0] != "sub/agent-x-1.bundle: the name agent-x-1.bundle is reserved for the branch's bundle" {
		t.Errorf("published %v, refused %v", pub.files, refused)
	}
}
