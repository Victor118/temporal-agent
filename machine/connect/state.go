// Package connect is `agent connect`: a machine of a user's, outside the
// private network, enrolled once, then connected to its server's gateway
// over a WebSocket, running the directives it is sent
// (docs/design/machines.md). It reaches neither the database nor Temporal:
// it imports neither, and knows no task token.
package connect

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"syscall"

	"github.com/victor/temporal-agent/machine"
)

// Config is what a machine keeps of its enrollment: its server, and its
// token, which it alone holds (the server keeps its hash).
type Config struct {
	Server    string `json:"server"`
	MachineID string `json:"machine_id"`
	Name      string `json:"name"`
	Token     string `json:"token"`
}

// State is a machine's directory: its config, the results it keeps until
// the gateway's ack, and a mark per directive it runs, so that a crash is
// told apart from a directive never received. 0700, files 0600, like an SSH
// key: whoever reads the token is the machine.
type State struct {
	Dir string
}

// DefaultDir is $XDG_CONFIG_HOME/agent/machine, or ~/.config/agent/machine.
// (The system's keychain will come later.)
func DefaultDir() (string, error) {
	base := os.Getenv("XDG_CONFIG_HOME")
	if base == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		base = filepath.Join(home, ".config")
	}
	return filepath.Join(base, "agent", "machine"), nil
}

const configFile = "machine.json"

// idPattern is a directive ID the machine accepts: it names files.
var idPattern = regexp.MustCompile(`^[A-Za-z0-9-]{1,64}$`)

func (s State) path(parts ...string) string {
	return filepath.Join(append([]string{s.Dir}, parts...)...)
}

// ErrNotEnrolled is a machine with no config yet: `agent connect --join`.
var ErrNotEnrolled = errors.New("this machine is not enrolled: run agent connect --join <url>")

// Load reads the config.
func (s State) Load() (Config, error) {
	var c Config
	b, err := os.ReadFile(s.path(configFile))
	if errors.Is(err, os.ErrNotExist) {
		return c, ErrNotEnrolled
	}
	if err != nil {
		return c, err
	}
	if err := json.Unmarshal(b, &c); err != nil {
		return c, fmt.Errorf("%s: %w", s.path(configFile), err)
	}
	if c.Server == "" || c.Token == "" {
		return c, ErrNotEnrolled
	}
	return c, nil
}

// Save writes the config, atomically: a crash leaves the old token or the
// new one, never half of either. The rotation relies on it.
func (s State) Save(c Config) error {
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return s.writeFile(s.path(configFile), b)
}

func (s State) writeFile(name string, b []byte) error {
	if err := os.MkdirAll(filepath.Dir(name), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(name), ".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), name); err != nil {
		return err
	}
	if d, err := os.Open(filepath.Dir(name)); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}

// Init makes the directory, private to its user.
func (s State) Init() error {
	for _, d := range []string{s.Dir, s.path("results"), s.path("running")} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return err
		}
		if err := os.Chmod(d, 0o700); err != nil {
			return err
		}
	}
	return nil
}

// SaveResult keeps a directive's result until the gateway acks it.
func (s State) SaveResult(m machine.Message) error {
	if !idPattern.MatchString(m.ID) {
		return fmt.Errorf("directive id %q", m.ID)
	}
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return s.writeFile(s.path("results", m.ID+".json"), b)
}

// DropResult forgets a result: acked, or of a directive the gateway closed.
func (s State) DropResult(id string) error {
	if !idPattern.MatchString(id) {
		return nil
	}
	err := os.Remove(s.path("results", id+".json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// Results are the results kept, by directive.
func (s State) Results() ([]machine.Message, error) {
	ids, err := s.list("results")
	if err != nil {
		return nil, err
	}
	var out []machine.Message
	for _, id := range ids {
		b, err := os.ReadFile(s.path("results", id+".json"))
		if err != nil {
			return nil, err
		}
		var m machine.Message
		if err := json.Unmarshal(b, &m); err != nil {
			return nil, fmt.Errorf("result %s: %w", id, err)
		}
		out = append(out, m)
	}
	return out, nil
}

// HasResult reports a result kept for id.
func (s State) HasResult(id string) bool {
	_, err := os.Stat(s.path("results", id+".json"))
	return idPattern.MatchString(id) && err == nil
}

// MarkRunning records a directive started: written before it runs.
func (s State) MarkRunning(id string) error {
	if !idPattern.MatchString(id) {
		return fmt.Errorf("directive id %q", id)
	}
	return s.writeFile(s.path("running", id), nil)
}

// Unmark records a directive ended (its result kept first).
func (s State) Unmark(id string) error {
	err := os.Remove(s.path("running", id))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// Marked are the directives marked running.
func (s State) Marked() ([]string, error) { return s.list("running") }

func (s State) list(dir string) ([]string, error) {
	entries, err := os.ReadDir(s.path(dir))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, e := range entries {
		id := strings.TrimSuffix(e.Name(), ".json")
		if e.Type().IsRegular() && idPattern.MatchString(id) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids, nil
}

// ErrLocked is a machine directory another agent connect runs on.
var ErrLocked = errors.New("another agent connect runs on this machine's directory")

// Lock takes the directory for this process (flock): two agent connect on
// one directory would share one token, cut each other's connections and
// race on its rotation. It returns the release.
func (s State) Lock() (func(), error) {
	if err := os.MkdirAll(s.Dir, 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(s.path(".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w (%s)", ErrLocked, s.Dir)
		}
		return nil, err
	}
	return func() {
		syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}, nil
}
