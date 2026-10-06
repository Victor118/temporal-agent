//go:build unix

package connect

import (
	"context"

	"github.com/victor/temporal-agent/machine/outputs"
)

// DefaultMaxFileBytes bounds a file of a run's outputs on the machine
// (Coder.MaxFileBytes): the server's own default, FILES_MAX_BYTES.
const DefaultMaxFileBytes = 20 << 20

func (a *Coder) maxFileBytes() int64 {
	if a.MaxFileBytes > 0 {
		return a.MaxFileBytes
	}
	return DefaultMaxFileBytes
}

// publishOutputs publishes what the run left in dir (its outputs) for the
// directive ctx runs, through the gateway (Upload), and returns what it did
// not publish, and why (outputs.Publish). The CLI is gone by now, its
// session ended with it. A run stopped or cancelled publishes nothing.
func (a *Coder) publishOutputs(ctx context.Context, dir string) []string {
	var upload outputs.Upload
	if a.Upload != nil {
		upload = func(ctx context.Context, name string, content []byte) error {
			_, err := a.Upload(ctx, name, content)
			return err
		}
	}
	return outputs.Publish(ctx, dir, a.maxFileBytes(), upload)
}
