package auth

import (
	"fmt"
	"net"
	"net/http"
	"net/netip"
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

// A nil *Throttle limits nothing: its methods do nothing, and Blocked is
// always false.

// Blocked reports whether key has used up its failures for now.
func (t *Throttle) Blocked(key string) bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	f := t.current(key)
	return f != nil && f.n >= t.Limit
}

// Fail records a failure for key.
func (t *Throttle) Fail(key string) {
	if t == nil {
		return
	}
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
	if t == nil {
		return
	}
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
// the price of stopping a distributed guess. A nil limit is no limit.
type LoginLimits struct {
	PerClient  *Throttle
	PerAccount *Throttle
}

// DefaultLoginLimits allows 10 failed logins per account in 15 minutes, and
// 20 per client address when clients knows the addresses. When it does not —
// behind a reverse proxy it is not told about, every client has the proxy's
// address — a limit per address would let anyone lock everyone out with
// twenty wrong passwords: there is none.
func DefaultLoginLimits(clients ClientAddrs) *LoginLimits {
	limits := &LoginLimits{PerAccount: NewThrottle(10, 15*time.Minute)}
	if clients.Known() {
		limits.PerClient = NewThrottle(20, 15*time.Minute)
	}
	return limits
}

func accountKey(email string) string { return strings.ToLower(strings.TrimSpace(email)) }

// ClientAddrs finds the address a request comes from, for the login limits
// and the logs. The zero value knows no address for sure: it returns the
// connection's peer, which may be a proxy.
type ClientAddrs struct {
	// Direct: the server faces its clients, the peer is the client.
	Direct bool
	// TrustedProxies are the peers whose X-Forwarded-For is believed. Anyone
	// else could put any address there and pick its own key.
	TrustedProxies []netip.Prefix
}

// ParseClientAddrs reads TRUSTED_PROXIES: comma-separated addresses or CIDR
// ranges of the reverse proxies in front of the server, "none" when there is
// none, or empty when it is not known.
func ParseClientAddrs(spec string) (ClientAddrs, error) {
	spec = strings.TrimSpace(spec)
	switch spec {
	case "":
		return ClientAddrs{}, nil
	case "none":
		return ClientAddrs{Direct: true}, nil
	}
	var c ClientAddrs
	for _, item := range strings.Split(spec, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		prefix, err := netip.ParsePrefix(item)
		if err != nil {
			addr, aerr := netip.ParseAddr(item)
			if aerr != nil {
				return ClientAddrs{}, fmt.Errorf("TRUSTED_PROXIES: %q is neither an address nor a CIDR range", item)
			}
			prefix = netip.PrefixFrom(addr.Unmap(), addr.Unmap().BitLen())
		}
		c.TrustedProxies = append(c.TrustedProxies, prefix.Masked())
	}
	return c, nil
}

// Known reports whether Of returns the clients' own addresses, which is what
// a limit per address needs.
func (c ClientAddrs) Known() bool { return c.Direct || len(c.TrustedProxies) > 0 }

// Of returns the address r comes from. From a trusted proxy, it is the
// rightmost X-Forwarded-For entry that is not a trusted proxy: what the first
// of them saw. Entries further left were written by the client itself.
func (c ClientAddrs) Of(r *http.Request) string {
	peer := peerAddr(r)
	if !c.trusted(peer) {
		return peer
	}
	var hops []string
	for _, h := range r.Header.Values("X-Forwarded-For") {
		hops = append(hops, strings.Split(h, ",")...)
	}
	for i := len(hops) - 1; i >= 0; i-- {
		hop := strings.TrimSpace(hops[i])
		if hop == "" {
			continue
		}
		if !c.trusted(hop) {
			return hop
		}
		peer = hop // every hop so far is a proxy: the leftmost is the client
	}
	return peer
}

// trusted reports whether addr is one of the trusted proxies.
func (c ClientAddrs) trusted(addr string) bool {
	a, err := netip.ParseAddr(addr)
	if err != nil {
		return false
	}
	a = a.Unmap()
	for _, p := range c.TrustedProxies {
		if p.Contains(a) {
			return true
		}
	}
	return false
}

// peerAddr is the connection's peer, without its port.
func peerAddr(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
