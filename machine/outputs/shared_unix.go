//go:build unix

package outputs

import (
	"os"
	"syscall"
)

// shared reports a file another path links to.
func shared(fi os.FileInfo) bool {
	st, ok := fi.Sys().(*syscall.Stat_t)
	return ok && st.Nlink > 1
}

// openFlags open a file of the outputs: never through a link, never
// waiting on a FIFO.
const openFlags = os.O_RDONLY | syscall.O_NOFOLLOW | syscall.O_NONBLOCK
