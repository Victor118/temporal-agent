package claudecode

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

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
// kept, and only without an API key (KeepCredentials).

// seedFiles are what a run's configuration starts with, from the operator's:
// the login, the settings, the instructions, the CLI's own state. Anything
// else (directories of skills or plugins, transcripts, caches) is not copied.
var seedFiles = []string{".credentials.json", ".claude.json", "settings.json", "CLAUDE.md"}

// credentialsFile is the OAuth login, which the CLI renews during a run.
const credentialsFile = ".credentials.json"

// maxCredentialsBytes bounds what KeepCredentials takes back from a run.
const maxCredentialsBytes = 64 * 1024

// OwnConfig tells whether a run gets a configuration of its own
// (SeedConfigDir): when there is an operator's configuration to copy (base),
// or when the CLI runs as a user of its own (runAs). With neither, the CLI
// runs as the caller's user, with the configuration that user's CLI uses
// anyway (~/.claude, ~/.claude.json): a copy would keep nothing out of the
// run's reach, and an empty one would only leave the CLI logged out.
func OwnConfig(base string, runAs *subproc.Identity) bool {
	return base != "" || runAs != nil
}

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
// It is the one thing a run hands back to the operator's configuration, so
// only what a renewal could have produced comes back, and each time it does,
// the log says so:
//   - not when the CLI had an API key (ANTHROPIC_API_KEY in environ, the
//     CLI's environment): it authenticates with the key, and has no login to
//     renew;
//   - only when base has a login of its own (a run cannot add one);
//   - only a regular file, opened without following a link (the run's user
//     wrote dir), of JSON holding everything the current login holds
//     (sameShape): a renewal changes tokens and dates, never which there
//     are, and a run that emptied the login would log every next one out.
//
// Whose login it is cannot be checked here: a run could put another
// account's in its place. An API key leaves no such channel (README).
// base gets a new file, never written through a path.
func KeepCredentials(dir, base string, environ []string) error {
	if base == "" || hasAPIKey(environ) {
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
	renewed, written, err := readRunFile(filepath.Join(dir, credentialsFile))
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
	if !sameShape(current, renewed) {
		return fmt.Errorf("the run's %s lacks what the current login holds: not kept", credentialsFile)
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
	if err := os.Rename(tmp.Name(), target); err != nil {
		return err
	}
	log.Printf("claude code: kept the login a run renewed in %s (%d bytes, written %s)",
		target, len(renewed), written.UTC().Format(time.RFC3339))
	return nil
}

// hasAPIKey tells whether environ gives the CLI an API key: a non-empty
// ANTHROPIC_API_KEY, the last one winning as os/exec has it.
func hasAPIKey(environ []string) bool {
	key := ""
	for _, kv := range environ {
		if name, value, _ := strings.Cut(kv, "="); name == "ANTHROPIC_API_KEY" {
			key = value
		}
	}
	return key != ""
}

// sameShape tells whether renewed, a JSON document, holds everything current
// does: each key of current's objects, at every depth, with a value that is
// not null, and not empty where current's is a non-empty string. Current not
// being a JSON object, there is nothing to compare: any JSON will do.
func sameShape(current, renewed []byte) bool {
	var was, is any
	if json.Unmarshal(current, &was) != nil {
		return true
	}
	if _, ok := was.(map[string]any); !ok {
		return true
	}
	if json.Unmarshal(renewed, &is) != nil {
		return false
	}
	return holds(was, is)
}

func holds(was, is any) bool {
	switch w := was.(type) {
	case map[string]any:
		i, ok := is.(map[string]any)
		if !ok {
			return false
		}
		for k, v := range w {
			if iv, ok := i[k]; !ok || iv == nil || !holds(v, iv) {
				return false
			}
		}
	case string:
		if s, ok := is.(string); w != "" && (!ok || s == "") {
			return false
		}
	}
	return true
}

// readRunFile reads a regular file the run's user may have replaced: never
// through a link, never blocking on a FIFO, and no more than
// maxCredentialsBytes.
func readRunFile(path string) (data []byte, written time.Time, err error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, time.Time{}, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, time.Time{}, err
	}
	if !fi.Mode().IsRegular() {
		return nil, time.Time{}, fmt.Errorf("%s is not a regular file", path)
	}
	data, err = io.ReadAll(io.LimitReader(f, maxCredentialsBytes+1))
	if err == nil && len(data) > maxCredentialsBytes {
		err = fmt.Errorf("%s is larger than %d bytes", path, maxCredentialsBytes)
	}
	return data, fi.ModTime(), err
}
