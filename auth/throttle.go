package auth

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Throttle counts failures per key and refuses a key once it reaches Limit
// failures within Window. The window starts at the first failure; it closes,
// and the count with it, Window later.
type Throttle struct {
	Limit  int
	Window time.Duration

	mu       sync.Mutex
	failures map[string]*failureCount
	now      func() time.Time
}

type failureCount struct {
	n     int
	since time.Time
}

// maxTrackedKeys bounds the memory a flood of distinct keys can take: past
// it, closed windows are swept on the next failure.
const maxTrackedKeys = 10_000

func NewThrottle(limit int, window time.Duration) *Throttle {
	return &Throttle{Limit: limit, Window: window, failures: map[string]*failureCount{}, now: time.Now}
}

// Blocked reports whether key has used up its failures for now.
func (t *Throttle) Blocked(key string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	f := t.current(key)
	return f != nil && f.n >= t.Limit
}

// Fail records a failure for key.
func (t *Throttle) Fail(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.failures) >= maxTrackedKeys {
		for k := range t.failures {
			t.current(k)
		}
	}
	f := t.current(key)
	if f == nil {
		f = &failureCount{since: t.now()}
		t.failures[key] = f
	}
	f.n++
}

// Reset forgets key's failures.
func (t *Throttle) Reset(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.failures, key)
}

// current returns key's open window, dropping a closed one. Called with mu held.
func (t *Throttle) current(key string) *failureCount {
	f := t.failures[key]
	if f != nil && t.now().Sub(f.since) >= t.Window {
		delete(t.failures, key)
		return nil
	}
	return f
}

// LoginLimits bound password guessing. Per client address, so one machine
// cannot try many accounts; per account, so many machines cannot try one.
// The account limit locks the account out for its window, its owner too:
// the price of stopping a distributed guess.
type LoginLimits struct {
	PerClient  *Throttle
	PerAccount *Throttle
}

// DefaultLoginLimits allows 20 failed logins per client address and 10 per
// account in 15 minutes. Behind a reverse proxy every client shares the
// proxy's address, hence the higher client limit.
func DefaultLoginLimits() *LoginLimits {
	return &LoginLimits{
		PerClient:  NewThrottle(20, 15*time.Minute),
		PerAccount: NewThrottle(10, 15*time.Minute),
	}
}

func accountKey(email string) string { return strings.ToLower(strings.TrimSpace(email)) }

// ClientAddr is the address a request comes from, for the login limits. It
// is the connection's peer: a forwarded header would let a client pick its
// own key.
func ClientAddr(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
