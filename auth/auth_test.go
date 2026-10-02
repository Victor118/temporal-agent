package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/victor/temporal-agent/store"
)

func TestHashAndVerify(t *testing.T) {
	h1, err := HashPassword("correct horse battery")
	if err != nil {
		t.Fatal(err)
	}
	h2, _ := HashPassword("correct horse battery")
	if h1 == h2 {
		t.Error("two hashes of the same password are equal: no salt")
	}
	if !strings.HasPrefix(h1, "$argon2id$v=19$m=65536,t=2,p=2$") {
		t.Errorf("hash %q", h1)
	}
	if ok, err := VerifyPassword("correct horse battery", h1); !ok || err != nil {
		t.Errorf("right password: %v %v", ok, err)
	}
	if ok, _ := VerifyPassword("Correct horse battery", h1); ok {
		t.Error("wrong password accepted")
	}
	if _, err := VerifyPassword("x", "$2a$10$bcrypt"); err == nil {
		t.Error("a foreign hash format must be an error")
	}
}

func TestPasswordPolicy(t *testing.T) {
	if CheckPasswordPolicy("short") == nil {
		t.Error("short password accepted")
	}
	// Counted in characters, not bytes.
	if CheckPasswordPolicy("ééééééééé") == nil {
		t.Error("9 characters accepted")
	}
	if err := CheckPasswordPolicy("ten chars!"); err != nil {
		t.Error(err)
	}
}

type fakeStore struct {
	users  map[string]*store.User // by lowercase email
	logins map[string]string      // token hash → user ID
}

func (f *fakeStore) GetUserByEmail(_ context.Context, email string) (*store.User, error) {
	return f.users[strings.ToLower(email)], nil
}

func (f *fakeStore) CreateLoginSession(_ context.Context, tokenHash, userID string, _ time.Time) error {
	f.logins[tokenHash] = userID
	return nil
}

func (f *fakeStore) GetLoginSessionUser(_ context.Context, tokenHash string) (*store.User, error) {
	for _, u := range f.users {
		if u.ID == f.logins[tokenHash] && u.DisabledAt == nil {
			return u, nil
		}
	}
	return nil, nil
}

func (f *fakeStore) DeleteLoginSession(_ context.Context, tokenHash string) error {
	delete(f.logins, tokenHash)
	return nil
}

func newService(t *testing.T) (*Service, *fakeStore) {
	t.Helper()
	LoginFailDelay = 0
	hash, _ := HashPassword("correct horse battery")
	st := &fakeStore{
		users: map[string]*store.User{
			"alice@example.com": {ID: "u-alice", Email: "Alice@example.com", PasswordHash: hash, Role: store.UserRoleStandard},
		},
		logins: map[string]string{},
	}
	return &Service{Store: st}, st
}

func TestLogin(t *testing.T) {
	s, st := newService(t)
	ctx := context.Background()

	token, u, err := s.Login(ctx, "192.0.2.1", "ALICE@example.com", "correct horse battery")
	if err != nil || u.ID != "u-alice" {
		t.Fatalf("login: %v %v", u, err)
	}
	// Only the hash of the token is stored.
	if _, stored := st.logins[token]; stored || st.logins[hashToken(token)] != "u-alice" {
		t.Errorf("logins %v", st.logins)
	}

	for _, creds := range [][2]string{{"alice@example.com", "wrong"}, {"nobody@example.com", "correct horse battery"}} {
		if _, _, err := s.Login(ctx, "192.0.2.1", creds[0], creds[1]); !errors.Is(err, ErrInvalidCredentials) {
			t.Errorf("%v: %v", creds, err)
		}
	}

	now := time.Now()
	st.users["alice@example.com"].DisabledAt = &now
	if _, _, err := s.Login(ctx, "192.0.2.1", "alice@example.com", "correct horse battery"); !errors.Is(err, ErrInvalidCredentials) {
		t.Errorf("disabled user logged in: %v", err)
	}
}

func TestRequireUser(t *testing.T) {
	s, _ := newService(t)
	token, _, _ := s.Login(context.Background(), "192.0.2.1", "alice@example.com", "correct horse battery")

	var seen *store.User
	h := s.RequireUser(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { seen = UserFrom(r.Context()) }))

	for name, cookie := range map[string]string{"none": "", "forged": "not-a-token"} {
		req := httptest.NewRequest(http.MethodGet, "/me", nil)
		if cookie != "" {
			req.AddCookie(&http.Cookie{Name: CookieName, Value: cookie})
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != http.StatusUnauthorized {
			t.Errorf("%s: %d", name, w.Code)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/me", nil)
	req.AddCookie(&http.Cookie{Name: CookieName, Value: token})
	h.ServeHTTP(httptest.NewRecorder(), req)
	if seen == nil || seen.ID != "u-alice" {
		t.Errorf("user in context: %+v", seen)
	}

	// After logout the token is worth nothing.
	s.Logout(context.Background(), req)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("after logout: %d", w.Code)
	}
}

func TestSameOrigin(t *testing.T) {
	h := SameOrigin(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	for _, c := range []struct {
		method, origin string
		want           int
	}{
		{http.MethodGet, "", 200},
		{http.MethodPost, "http://example.com", 200},
		{http.MethodPost, "", 403},
		{http.MethodPost, "http://evil.example", 403},
		{http.MethodDelete, "http://evil.example", 403},
	} {
		req := httptest.NewRequest(c.method, "/x", nil)
		if c.origin != "" {
			req.Header.Set("Origin", c.origin)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != c.want {
			t.Errorf("%s from %q: %d, want %d", c.method, c.origin, w.Code, c.want)
		}
	}
}

// Parallel guesses get past a delay: the limits stop them, per client and
// per account, and say so without checking the password.
func TestLogin_Limits(t *testing.T) {
	s, _ := newService(t)
	s.Limits = &LoginLimits{PerClient: NewThrottle(3, time.Hour), PerAccount: NewThrottle(5, time.Hour)}
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if _, _, err := s.Login(ctx, "198.51.100.7", "alice@example.com", "guess"); !errors.Is(err, ErrInvalidCredentials) {
			t.Fatalf("guess %d: %v", i, err)
		}
	}
	// The client is out of tries, even with the right password.
	if _, _, err := s.Login(ctx, "198.51.100.7", "alice@example.com", "correct horse battery"); !errors.Is(err, ErrTooManyAttempts) {
		t.Errorf("a blocked client: %v", err)
	}
	// Another client still logs in, which clears the account's count.
	if _, _, err := s.Login(ctx, "203.0.113.9", "alice@example.com", "correct horse battery"); err != nil {
		t.Errorf("another client: %v", err)
	}

	// Guesses on one account from many clients lock the account.
	for i := 0; i < 5; i++ {
		s.Login(ctx, "10.0.0."+string(rune('1'+i)), "ALICE@example.com ", "guess")
	}
	if _, _, err := s.Login(ctx, "203.0.113.9", "alice@example.com", "correct horse battery"); !errors.Is(err, ErrTooManyAttempts) {
		t.Errorf("a locked account: %v", err)
	}
}

func TestThrottle_WindowCloses(t *testing.T) {
	now := time.Now()
	th := NewThrottle(2, time.Minute)
	th.now = func() time.Time { return now }
	th.Fail("k")
	th.Fail("k")
	if !th.Blocked("k") || th.Blocked("other") {
		t.Fatal("blocked the wrong keys")
	}
	now = now.Add(time.Minute)
	if th.Blocked("k") {
		t.Error("still blocked once the window closed")
	}
}

// Behind a proxy nobody declared, every client has the proxy's address: a
// limit per address would let twenty wrong passwords lock every account out.
// The account limit alone stays.
func TestDefaultLoginLimits_PerAddressOnlyWhenAddressesAreKnown(t *testing.T) {
	s, st := newService(t)
	hash, _ := HashPassword("bob's password")
	st.users["bob@example.com"] = &store.User{ID: "u-bob", Email: "bob@example.com", PasswordHash: hash}
	s.Limits = DefaultLoginLimits(ClientAddrs{})
	ctx := context.Background()

	proxy := "172.18.0.5"
	for i := 0; i < 30; i++ {
		s.Login(ctx, proxy, "mallory-"+string(rune('a'+i))+"@example.com", "guess")
	}
	if _, _, err := s.Login(ctx, proxy, "bob@example.com", "bob's password"); err != nil {
		t.Errorf("an unknown client address locked another account out: %v", err)
	}

	known := DefaultLoginLimits(ClientAddrs{Direct: true})
	if known.PerClient == nil || known.PerAccount == nil {
		t.Errorf("limits with known addresses = %+v, want both", known)
	}
}

func TestParseClientAddrs(t *testing.T) {
	if c, err := ParseClientAddrs(""); err != nil || c.Known() {
		t.Errorf("empty: %+v, %v", c, err)
	}
	if c, err := ParseClientAddrs("none"); err != nil || !c.Direct || !c.Known() {
		t.Errorf("none: %+v, %v", c, err)
	}
	c, err := ParseClientAddrs("10.0.0.0/8, 172.18.0.5 ,fd00::/8")
	if err != nil || len(c.TrustedProxies) != 3 {
		t.Fatalf("list: %+v, %v", c, err)
	}
	if _, err := ParseClientAddrs("10.0.0.0/8,proxy.local"); err == nil {
		t.Error("a host name was accepted")
	}
}

func TestClientAddrs_Of(t *testing.T) {
	c, _ := ParseClientAddrs("10.0.0.0/8")
	req := func(peer string, xff ...string) *http.Request {
		r := httptest.NewRequest(http.MethodPost, "/login", nil)
		r.RemoteAddr = peer
		for _, h := range xff {
			r.Header.Add("X-Forwarded-For", h)
		}
		return r
	}
	for name, tc := range map[string]struct {
		r    *http.Request
		want string
	}{
		"direct client, header ignored":   {req("198.51.100.7:4242", "1.2.3.4"), "198.51.100.7"},
		"through the proxy":               {req("10.0.0.2:4242", "203.0.113.9"), "203.0.113.9"},
		"client-written entries are left": {req("10.0.0.2:4242", "1.2.3.4, 203.0.113.9"), "203.0.113.9"},
		"chained proxies":                 {req("10.0.0.2:4242", "203.0.113.9, 10.0.0.3"), "203.0.113.9"},
		"several headers":                 {req("10.0.0.2:4242", "1.2.3.4", "203.0.113.9"), "203.0.113.9"},
		"proxy without a header":          {req("10.0.0.2:4242"), ""},
		"proxies only":                    {req("10.0.0.2:4242", "10.0.0.3, 10.0.0.4"), ""},
		"IPv4-mapped peer":                {req("[::ffff:10.0.0.2]:4242", "203.0.113.9"), "203.0.113.9"},
	} {
		if got := c.Of(tc.r); got != tc.want {
			t.Errorf("%s: %q, want %q", name, got, tc.want)
		}
	}
	// Without trusted proxies, the header is nobody's word.
	if got := (ClientAddrs{Direct: true}).Of(req("10.0.0.2:4242", "203.0.113.9")); got != "10.0.0.2" {
		t.Errorf("direct: %q", got)
	}
}

// A trusted proxy that names no client gives every client its own address:
// no limit per address applies to such logins, or twenty wrong passwords
// through it would lock everyone out. The account limit still does.
func TestLogin_NoLimitPerAddressForAProxyNamingNoClient(t *testing.T) {
	s, st := newService(t)
	hash, _ := HashPassword("bob's password")
	st.users["bob@example.com"] = &store.User{ID: "u-bob", Email: "bob@example.com", PasswordHash: hash}
	clients, _ := ParseClientAddrs("172.18.0.5")
	s.Clients = clients
	s.Limits = DefaultLoginLimits(clients)
	ctx := context.Background()
	r := httptest.NewRequest(http.MethodPost, "/login", nil)
	r.RemoteAddr = "172.18.0.5:4242"

	for i := 0; i < 25; i++ {
		s.Login(ctx, s.ClientAddr(r), "mallory-"+string(rune('a'+i))+"@example.com", "guess")
	}
	if _, _, err := s.Login(ctx, s.ClientAddr(r), "bob@example.com", "bob's password"); err != nil {
		t.Errorf("failures through a proxy naming no client locked another account out: %v", err)
	}
	for i := 0; i < 10; i++ {
		s.Login(ctx, s.ClientAddr(r), "bob@example.com", "guess")
	}
	if _, _, err := s.Login(ctx, s.ClientAddr(r), "bob@example.com", "bob's password"); !errors.Is(err, ErrTooManyAttempts) {
		t.Errorf("the account limit no longer applies: %v", err)
	}
}
