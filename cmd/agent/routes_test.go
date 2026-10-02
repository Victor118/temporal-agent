package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/victor/temporal-agent/auth"
	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/sse"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/web/admin"
)

// routeStore holds users, logins and one session in memory. Methods the
// routes under test do not reach are left to the nil embedded Store.
type routeStore struct {
	store.Store
	users   []store.User
	logins  map[string]string
	session store.Session
	members []string
}

func (f *routeStore) user(match func(store.User) bool) *store.User {
	for _, u := range f.users {
		if match(u) {
			return &u
		}
	}
	return nil
}

func (f *routeStore) GetUserByEmail(_ context.Context, email string) (*store.User, error) {
	return f.user(func(u store.User) bool { return strings.EqualFold(u.Email, email) }), nil
}

func (f *routeStore) CreateLoginSession(_ context.Context, tokenHash, userID string, _ time.Time) error {
	f.logins[tokenHash] = userID
	return nil
}

func (f *routeStore) GetLoginSessionUser(_ context.Context, tokenHash string) (*store.User, error) {
	id := f.logins[tokenHash]
	return f.user(func(u store.User) bool { return u.ID == id }), nil
}

func (f *routeStore) GetSession(_ context.Context, id string) (*store.Session, error) {
	if id != f.session.SessionID {
		return nil, nil
	}
	return &f.session, nil
}

func (f *routeStore) IsSessionMember(_ context.Context, sessionID, userID string) (bool, error) {
	if sessionID != f.session.SessionID {
		return false, nil
	}
	for _, m := range f.members {
		if m == userID {
			return true, nil
		}
	}
	return false, nil
}

func (f *routeStore) ListSessionMembers(context.Context, string) ([]store.SessionMember, error) {
	var out []store.SessionMember
	for _, id := range f.members {
		u := f.user(func(u store.User) bool { return u.ID == id })
		out = append(out, store.SessionMember{UserID: id, Email: u.Email})
	}
	return out, nil
}

func (f *routeStore) AddSessionMember(_ context.Context, _, userID, _ string) error {
	f.members = append(f.members, userID)
	return nil
}

func (f *routeStore) RemoveSessionMember(_ context.Context, _, userID string) error {
	for i, m := range f.members {
		if m == userID {
			f.members = append(f.members[:i], f.members[i+1:]...)
		}
	}
	return nil
}

const pw = "correct horse battery"

func newRouteTest(t *testing.T) (http.Handler, *routeStore) {
	t.Helper()
	auth.LoginFailDelay = 0
	hash, _ := auth.HashPassword(pw)
	st := &routeStore{
		users: []store.User{
			{ID: "u-alice", Email: "alice@example.com", PasswordHash: hash, Role: store.UserRoleStandard},
			{ID: "u-bob", Email: "bob@example.com", PasswordHash: hash, Role: store.UserRoleStandard},
			{ID: "u-carol", Email: "carol@example.com", PasswordHash: hash, Role: store.UserRoleStandard},
		},
		logins:  map[string]string{},
		session: store.Session{SessionID: "s1", CreatedBy: "u-alice"},
		members: []string{"u-alice", "u-bob"},
	}
	svc := &auth.Service{Store: st}
	h := &handler{auth: svc, store: st, hub: sse.NewHub(), cfg: &config.Config{}}
	return publicRouter(h, admin.New(admin.Config{Store: st, Auth: svc})), st
}

func call(t *testing.T, h http.Handler, method, path, body string, cookie *http.Cookie) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "http://example.com")
	if cookie != nil {
		req.AddCookie(cookie)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func logIn(t *testing.T, h http.Handler, email string) *http.Cookie {
	t.Helper()
	w := call(t, h, http.MethodPost, "/auth/login", `{"email":"`+email+`","password":"`+pw+`"}`, nil)
	for _, c := range w.Result().Cookies() {
		if c.Name == auth.CookieName {
			return c
		}
	}
	t.Fatalf("login %s: %d %s", email, w.Code, w.Body)
	return nil
}

func TestRoutes_RequireLogin(t *testing.T) {
	h, _ := newRouteTest(t)
	for _, path := range []string{"/auth/me", "/me/sessions", "/sessions/s1/history", "/api/admin/queues"} {
		if w := call(t, h, http.MethodGet, path, "", nil); w.Code != http.StatusUnauthorized {
			t.Errorf("%s without login: %d", path, w.Code)
		}
	}
	if w := call(t, h, http.MethodPost, "/auth/login", `{"email":"alice@example.com","password":"wrong"}`, nil); w.Code != http.StatusUnauthorized {
		t.Errorf("wrong password: %d", w.Code)
	}
	c := logIn(t, h, "ALICE@example.com")
	w := call(t, h, http.MethodGet, "/auth/me", "", c)
	var me store.User
	json.Unmarshal(w.Body.Bytes(), &me)
	if me.ID != "u-alice" || strings.Contains(w.Body.String(), "password") {
		t.Errorf("/auth/me = %s", w.Body)
	}
	// Not an admin: the admin JSON API is closed.
	if w := call(t, h, http.MethodGet, "/api/admin/queues", "", c); w.Code != http.StatusForbidden {
		t.Errorf("admin API as a user: %d", w.Code)
	}
}

func TestRoutes_OnlyMembersReachASession(t *testing.T) {
	h, _ := newRouteTest(t)
	carol := logIn(t, h, "carol@example.com")
	for _, r := range []struct{ method, path string }{
		{http.MethodGet, "/sessions/s1/members"},
		{http.MethodPost, "/sessions/s1/members"},
		{http.MethodDelete, "/sessions/s1"},
		{http.MethodPost, "/sessions/s1/answer"},
	} {
		// 404, not 403: a non-member does not learn the session exists.
		if w := call(t, h, r.method, r.path, `{}`, carol); w.Code != http.StatusNotFound {
			t.Errorf("%s %s as a non-member: %d", r.method, r.path, w.Code)
		}
	}
}

func TestRoutes_Members(t *testing.T) {
	h, st := newRouteTest(t)
	bob := logIn(t, h, "bob@example.com")

	// Any member may add another user, by email.
	if w := call(t, h, http.MethodPost, "/sessions/s1/members", `{"email":"Carol@example.com"}`, bob); w.Code != 200 || len(st.members) != 3 {
		t.Fatalf("add carol: %d %s", w.Code, w.Body)
	}
	if w := call(t, h, http.MethodPost, "/sessions/s1/members", `{"email":"nobody@example.com"}`, bob); w.Code != http.StatusNotFound {
		t.Errorf("add an unknown email: %d", w.Code)
	}
	// A member leaves; they cannot remove someone else.
	if w := call(t, h, http.MethodDelete, "/sessions/s1/members/u-alice", "", bob); w.Code != http.StatusForbidden {
		t.Errorf("bob removing alice: %d", w.Code)
	}
	if w := call(t, h, http.MethodDelete, "/sessions/s1/members/u-bob", "", bob); w.Code != http.StatusNoContent {
		t.Errorf("bob leaving: %d", w.Code)
	}
	if w := call(t, h, http.MethodGet, "/sessions/s1/members", "", bob); w.Code != http.StatusNotFound {
		t.Errorf("bob after leaving: %d", w.Code)
	}
}

func TestRoutes_OnlyTheCreatorDeletes(t *testing.T) {
	h, _ := newRouteTest(t)
	bob := logIn(t, h, "bob@example.com")
	if w := call(t, h, http.MethodDelete, "/sessions/s1", "", bob); w.Code != http.StatusForbidden {
		t.Errorf("a member deleting the session: %d", w.Code)
	}
}

func TestRoutes_AnswerBelongsToTheSession(t *testing.T) {
	h, _ := newRouteTest(t)
	bob := logIn(t, h, "bob@example.com")
	// Bob is a member of s1, not of s2: an s2 question cannot be answered
	// through s1.
	w := call(t, h, http.MethodPost, "/sessions/s1/answer", `{"workflow_id":"s2-tool-ask_user-1-0","answer":"yes"}`, bob)
	if w.Code != http.StatusForbidden {
		t.Errorf("answering another session's question: %d", w.Code)
	}
}

func TestRoutes_RefuseCrossSiteWrites(t *testing.T) {
	h, st := newRouteTest(t)
	bob := logIn(t, h, "bob@example.com")
	req := httptest.NewRequest(http.MethodPost, "/sessions/s1/members", strings.NewReader(`{"email":"carol@example.com"}`))
	req.Header.Set("Origin", "http://evil.example")
	req.AddCookie(bob)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden || len(st.members) != 2 {
		t.Errorf("cross-site add: %d, members %v", w.Code, st.members)
	}
}
