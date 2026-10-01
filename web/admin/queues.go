package admin

import (
	"context"
	"sync"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/victor/temporal-agent/taskqueue"
)

const (
	probeTimeout = 2 * time.Second
	probeTTL     = 10 * time.Second
)

// queueProber asks Temporal for the pollers of each queue, in parallel, and
// caches the answers briefly: every page lists every queue, and the queues page
// refreshes itself.
type queueProber struct {
	tc client.Client

	mu    sync.Mutex
	cache map[string]probed
}

type probed struct {
	status taskqueue.Status
	at     time.Time
}

func newQueueProber(tc client.Client) *queueProber {
	return &queueProber{tc: tc, cache: make(map[string]probed)}
}

func (p *queueProber) statuses(ctx context.Context, queues []string) map[string]taskqueue.Status {
	out := make(map[string]taskqueue.Status, len(queues))
	var stale []string

	p.mu.Lock()
	for _, q := range queues {
		if c, ok := p.cache[q]; ok && time.Since(c.at) < probeTTL {
			out[q] = c.status
		} else {
			stale = append(stale, q)
		}
	}
	p.mu.Unlock()

	if len(stale) == 0 || p.tc == nil {
		return out
	}

	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	results := make([]taskqueue.Status, len(stale))
	var wg sync.WaitGroup
	for i, q := range stale {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = taskqueue.Describe(ctx, p.tc, q)
		}()
	}
	wg.Wait()

	now := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, st := range results {
		out[st.Queue] = st
		p.cache[st.Queue] = probed{status: st, at: now}
	}
	return out
}
