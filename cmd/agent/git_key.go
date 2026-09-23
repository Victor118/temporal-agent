package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// checkGitKey refuses to start a coding worker whose git identity it could
// not use.
//
// Without this, a misconfigured key only shows up at the very end of a coding
// run: Claude Code has written and committed, the push fails on "permission
// denied (publickey)", and the workspace is deleted with the work still in it.
// Twenty minutes and a few dollars to learn that a file was mounted 0644.
//
// The key itself is never read here. ssh-keygen -y derives the public key,
// which is what ssh would do at push time, and fails the same way ssh would:
// on a missing file, an unparseable one, or one that wants a passphrase.
func checkGitKey(path string) error {
	if path == "" {
		return nil
	}

	fi, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("git identity %s: %w", path, err)
	}
	if fi.IsDir() {
		return fmt.Errorf("git identity %s is a directory", path)
	}
	// ssh refuses a private key others can read, and says so only at push time.
	if perm := fi.Mode().Perm(); perm&0o077 != 0 {
		return fmt.Errorf("git identity %s is mode %#o: ssh refuses a key readable by group or others, it must be 0600", path, perm)
	}

	// -P "" supplies an empty passphrase: an encrypted key fails here rather
	// than at the end of a run. A worker pushing unattended cannot be asked
	// for one, so the key must be a dedicated, unencrypted identity — scoped
	// tightly enough (a deploy key on one repository) that the missing
	// passphrase is not what protects it.
	cmd := exec.Command("ssh-keygen", "-y", "-P", "", "-f", path)
	if out, err := cmd.CombinedOutput(); err != nil {
		detail := strings.TrimSpace(string(out))
		if strings.Contains(detail, "incorrect passphrase") || strings.Contains(detail, "load failed") {
			return fmt.Errorf("git identity %s cannot be used unattended: it is passphrase-protected", path)
		}
		return fmt.Errorf("git identity %s is unusable: %s", path, detail)
	}
	return nil
}
