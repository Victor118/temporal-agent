//go:build !unix

package outputs

import "os"

// shared: unknown off Unix, where agent connect runs no coding directive.
func shared(os.FileInfo) bool { return false }

const openFlags = os.O_RDONLY
