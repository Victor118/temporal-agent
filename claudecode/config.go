package claudecode

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"

	"github.com/victor/temporal-agent/subproc"
)

// The CLI's configuration directory (CLAUDE_CONFIG_DIR) is read at every
// start: settings.json (hooks it runs, permissions, environment,
// apiKeyHelper), CLAUDE.md, skills, the MCP servers of .claude.json. And the
// CLI writes it, as the user the run is made as, whose shell can then write
// it too. Shared by the runs of a worker, it would carry what one run left
// there into the next: a hook, an instruction, a server to start.
//
// So each run gets a configuration of its own (Params.ConfigDir), seeded from
// the operator's and thrown away with the run. Only renewed login tokens are
// kept (KeepCredentials).

// seedFiles are what a run's configuration starts with, from the operator's:
// the login, the settings, the instructions, the CLI's own state. Anything
// else (directories of skills or plugins, transcripts, caches) is not copied.
var seedFiles = []string{".credentials.json", ".claude.json", "settings.json", "CLAUDE.md"}

// credentialsFile is the OAuth login, which the CLI renews during a run.
const credentialsFile = ".credentials.json"

// maxCredentialsBytes bounds what KeepCredentials takes back from a run.
const maxCredentialsBytes = 64 * 1024

// SeedConfigDir creates dir afresh as a run's configuration: base's seedFiles
// copied in, the whole given to owner. An empty base seeds nothing. dir's
// parent must be the worker's: dir is removed and created again by name.
func SeedConfigDir(dir, base string, owner *subproc.Identity) error {
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	if err := os.Mkdir(dir, 0o700); err != nil {
		return err
	}
	for _, name := range seedFiles {
		if base == "" {
			break
		}
		data, err := os.ReadFile(filepath.Join(base, name))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			return err
		}
	}
	return owner.Give(dir)
}

// KeepCredentials copies the login a run renewed back into base: an OAuth
// refresh can retire the refresh token base holds, and the next run would
// find itself logged out. Nothing else of the run's configuration is kept.
//
// Only when base has a login of its own (a run cannot add one), and only a
// regular file of valid JSON, opened without following a link: the run's user
// wrote dir. base gets a new file, never written through a path.
func KeepCredentials(dir, base string) error {
	if base == "" {
		return nil
	}
	target := filepath.Join(base, credentialsFile)
	current, err := os.ReadFile(target)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	renewed, err := readRunFile(filepath.Join(dir, credentialsFile))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if string(renewed) == string(current) {
		return nil
	}
	if !json.Valid(renewed) {
		return fmt.Errorf("the run's %s is not JSON: not kept", credentialsFile)
	}
	tmp, err := os.CreateTemp(base, credentialsFile+".*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(renewed); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), target)
}

// readRunFile reads a regular file the run's user may have replaced: never
// through a link, never blocking on a FIFO, and no more than
// maxCredentialsBytes.
func readRunFile(path string) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", path)
	}
	data, err := io.ReadAll(io.LimitReader(f, maxCredentialsBytes+1))
	if err == nil && len(data) > maxCredentialsBytes {
		err = fmt.Errorf("%s is larger than %d bytes", path, maxCredentialsBytes)
	}
	return data, err
}
