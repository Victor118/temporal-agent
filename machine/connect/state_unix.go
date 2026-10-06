//go:build unix

package connect

import (
	"errors"
	"os"
	"syscall"
)

// lockFile takes f exclusively, without waiting: errLockHeld when another
// process holds it. The lock goes with the process.
func lockFile(f *os.File) error {
	err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) {
		return errLockHeld
	}
	return err
}

func unlockFile(f *os.File) {
	syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
