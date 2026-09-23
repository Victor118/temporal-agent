package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func keygen(t *testing.T, path, passphrase string) {
	t.Helper()
	cmd := exec.Command("ssh-keygen", "-q", "-t", "ed25519", "-N", passphrase, "-C", "test", "-f", path)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("ssh-keygen: %v: %s", err, out)
	}
}

func TestCheckPushKey(t *testing.T) {
	dir := t.TempDir()

	plain := filepath.Join(dir, "plain")
	keygen(t, plain, "")
	if err := checkPushKey(plain); err != nil {
		t.Errorf("a usable key was rejected: %v", err)
	}

	// No key configured is the read-only worker: nothing to check.
	if err := checkPushKey(""); err != nil {
		t.Errorf("empty path should be accepted: %v", err)
	}

	// A worker pushing unattended has nobody to ask for a passphrase. Saying
	// so at startup beats failing after a twenty-minute run.
	encrypted := filepath.Join(dir, "encrypted")
	keygen(t, encrypted, "hunter2")
	err := checkPushKey(encrypted)
	if err == nil {
		t.Fatal("a passphrase-protected key should be refused")
	}
	if !strings.Contains(err.Error(), "passphrase") {
		t.Errorf("error should name the passphrase, got: %v", err)
	}

	// ssh refuses a key others can read, and only says so at push time.
	loose := filepath.Join(dir, "loose")
	keygen(t, loose, "")
	os.Chmod(loose, 0o644)
	if err := checkPushKey(loose); err == nil || !strings.Contains(err.Error(), "0600") {
		t.Errorf("a world-readable key should be refused with what to fix, got: %v", err)
	}

	if err := checkPushKey(filepath.Join(dir, "absent")); err == nil {
		t.Error("a missing key should be refused")
	}

	notAKey := filepath.Join(dir, "notakey")
	os.WriteFile(notAKey, []byte("hello\n"), 0o600)
	if err := checkPushKey(notAKey); err == nil {
		t.Error("a file that is not a key should be refused")
	}

	if err := checkPushKey(dir); err == nil {
		t.Error("a directory should be refused")
	}
}
