package connect

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/victor/temporal-agent/machine"
)

// Echo runs an echo directive: its text back after its duration, a progress
// every ProgressEvery on the way. It stops when ctx ends.
func Echo(ctx context.Context, input json.RawMessage, progress func(string)) (json.RawMessage, error) {
	var in machine.EchoInput
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, fmt.Errorf("echo input: %w", err)
	}
	if err := in.Check(); err != nil {
		return nil, err
	}
	end := time.NewTimer(in.Duration())
	defer end.Stop()
	var tick <-chan time.Time
	if every := in.ProgressEvery(); every > 0 {
		t := time.NewTicker(every)
		defer t.Stop()
		tick = t.C
	}
	start := time.Now()
	n := 0
	for {
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case <-tick:
			n++
			progress(fmt.Sprintf("echo: %s of %s", time.Since(start).Round(100*time.Millisecond), in.Duration()))
		case <-end.C:
			return json.Marshal(machine.EchoOutput{Text: in.Text, Progresses: n})
		}
	}
}
