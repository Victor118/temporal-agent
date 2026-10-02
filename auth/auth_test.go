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
	store.Store
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

	token, u, err := s.Login(ctx, "ALICE@example.com", "correct horse battery")
	if err != nil || u.ID != "u-alice" {
		t.Fatalf("login: %v %v", u, err)
	}
	// Only the hash of the token is stored.
	if _, stored := st.logins[token]; stored || st.logins[hashToken(token)] != "u-alice" {
		t.Errorf("logins %v", st.logins)
	}

	for _, creds := range [][2]string{{"alice@example.com", "wrong"}, {"nobody@example.com", "correct horse battery"}} {
		if _, _, err := s.Login(ctx, creds[0], creds[1]); !errors.Is(err, ErrInvalidCredentials) {
			t.Errorf("%v: %v", creds, err)
		}
	}

	now := time.Now()
	st.users["alice@example.com"].DisabledAt = &now
	if _, _, err := s.Login(ctx, "alice@example.com", "correct horse battery"); !errors.Is(err, ErrInvalidCredentials) {
		t.Errorf("disabled user logged in: %v", err)
	}
}

func TestRequireUser(t *testing.T) {
	s, _ := newService(t)
	token, _, _ := s.Login(context.Background(), "alice@example.com", "correct horse battery")

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
