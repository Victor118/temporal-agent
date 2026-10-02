package admin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/victor/temporal-agent/auth"
	"github.com/victor/temporal-agent/store"
)

// fakeStore keeps agents in memory with the revision rules of PostgresStore.
// Methods the back-office does not call are left to the nil embedded Store.
type fakeStore struct {
	store.Store
	agents []store.Agent
	tools  []store.ToolRecord
	users  []store.User
	logins map[string]string // token hash → user ID
}

func (f *fakeStore) ListAgents(context.Context) ([]store.Agent, error) {
	return append([]store.Agent(nil), f.agents...), nil
}
func (f *fakeStore) ListTools(context.Context) ([]store.ToolRecord, error) { return f.tools, nil }
func (f *fakeStore) CountSessionsByAgent(context.Context) (map[string]int, error) {
	return map[string]int{}, nil
}
func (f *fakeStore) ListActivityQueues(context.Context) ([]store.ActivityQueueEntry, error) {
	return nil, nil
}

func (f *fakeStore) GetAgent(_ context.Context, id string) (*store.Agent, error) {
	for _, a := range f.agents {
		if a.ID == id {
			return &a, nil
		}
	}
	return nil, nil
}

func (f *fakeStore) CreateAgent(_ context.Context, a store.Agent) error {
	for _, e := range f.agents {
		if e.ID == a.ID {
			return store.ErrAgentExists
		}
	}
	a.Revision = 1
	f.agents = append(f.agents, a)
	return nil
}

func (f *fakeStore) UpdateAgent(_ context.Context, a store.Agent, rev int64) (int64, error) {
	for i, e := range f.agents {
		if e.ID != a.ID {
			continue
		}
		if e.Revision != rev {
			return 0, store.ErrAgentConflict
		}
		a.Revision = rev + 1
		f.agents[i] = a
		return a.Revision, nil
	}
	return 0, store.ErrAgentNotFound
}

func (f *fakeStore) DeleteAgent(_ context.Context, id string) error {
	for i, e := range f.agents {
		if e.ID == id {
			f.agents = append(f.agents[:i], f.agents[i+1:]...)
			return nil
		}
	}
	return store.ErrAgentNotFound
}

// Users and login sessions, in memory.

func (f *fakeStore) find(match func(store.User) bool) *store.User {
	for i := range f.users {
		if match(f.users[i]) {
			u := f.users[i]
			return &u
		}
	}
	return nil
}

func (f *fakeStore) GetUser(_ context.Context, id string) (*store.User, error) {
	return f.find(func(u store.User) bool { return u.ID == id }), nil
}

func (f *fakeStore) GetUserByEmail(_ context.Context, email string) (*store.User, error) {
	return f.find(func(u store.User) bool { return strings.EqualFold(u.Email, email) }), nil
}

func (f *fakeStore) ListUsers(context.Context) ([]store.User, error) { return f.users, nil }

func (f *fakeStore) CreateUser(_ context.Context, u store.User) error {
	if f.find(func(e store.User) bool { return strings.EqualFold(e.Email, u.Email) }) != nil {
		return store.ErrUserExists
	}
	f.users = append(f.users, u)
	return nil
}

func (f *fakeStore) update(id string, fn func(*store.User)) error {
	for i := range f.users {
		if f.users[i].ID == id {
			fn(&f.users[i])
			return nil
		}
	}
	return store.ErrUserNotFound
}

func (f *fakeStore) UpdateUser(_ context.Context, u store.User) error {
	return f.update(u.ID, func(e *store.User) {
		e.Email, e.DisplayName, e.Role, e.TelegramID = u.Email, u.DisplayName, u.Role, u.TelegramID
	})
}

func (f *fakeStore) revoke(userID string) {
	for tok, id := range f.logins {
		if id == userID {
			delete(f.logins, tok)
		}
	}
}

func (f *fakeStore) SetUserPassword(_ context.Context, id, hash string) error {
	err := f.update(id, func(e *store.User) { e.PasswordHash = hash })
	if err == nil {
		f.revoke(id)
	}
	return err
}

func (f *fakeStore) SetUserDisabled(_ context.Context, id string, disabled bool) error {
	return f.update(id, func(e *store.User) {
		e.DisabledAt = nil
		if disabled {
			now := time.Now()
			e.DisabledAt = &now
			f.revoke(id)
		}
	})
}

func (f *fakeStore) CreateLoginSession(_ context.Context, tokenHash, userID string, _ time.Time) error {
	f.logins[tokenHash] = userID
	return nil
}

func (f *fakeStore) GetLoginSessionUser(_ context.Context, tokenHash string) (*store.User, error) {
	id, ok := f.logins[tokenHash]
	if !ok {
		return nil, nil
	}
	return f.find(func(u store.User) bool { return u.ID == id && u.DisabledAt == nil }), nil
}

func (f *fakeStore) DeleteLoginSession(_ context.Context, tokenHash string) error {
	delete(f.logins, tokenHash)
	return nil
}

const testPassword = "correct horse battery"

func newTestAdmin(t *testing.T) (*Admin, *fakeStore) {
	t.Helper()
	auth.LoginFailDelay = 0
	hash, err := auth.HashPassword(testPassword)
	if err != nil {
		t.Fatal(err)
	}
	st := &fakeStore{
		agents: []store.Agent{
			{ID: "default", Name: "Default", Tools: []string{"read_file"}, Revision: 1},
			{ID: "coder", Name: "Coder", Tools: []string{"exec"}, Revision: 1},
		},
		tools: []store.ToolRecord{
			{Name: "exec", Kind: "activity", TaskQueue: "tools"},
			{Name: "read_file", Kind: "activity", TaskQueue: "tools"},
		},
		users: []store.User{
			{ID: "u-admin", Email: "admin@example.com", Role: store.UserRoleAdmin, PasswordHash: hash},
			{ID: "u-bob", Email: "bob@example.com", Role: store.UserRoleStandard, PasswordHash: hash},
		},
		logins: map[string]string{},
	}
	return New(Config{Store: st, DefaultAgentID: "default", WorkflowQueue: "agent", Auth: &auth.Service{Store: st}}), st
}

// do sends a request as a browser on the same origin would.
func do(h http.Handler, method, target string, form url.Values, cookie *http.Cookie) *httptest.ResponseRecorder {
	var req *http.Request
	if method == http.MethodGet {
		if form != nil {
			target += "?" + form.Encode()
		}
		req = httptest.NewRequest(method, target, nil)
	} else {
		req = httptest.NewRequest(method, target, strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Origin", "http://example.com")
	}
	if cookie != nil {
		req.AddCookie(cookie)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

// handler mounts the routes the way the server does.
func handler(a *Admin) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/admin/", http.StripPrefix("/admin", a.Routes()))
	return mux
}

func sessionCookieOf(w *httptest.ResponseRecorder) *http.Cookie {
	for _, c := range w.Result().Cookies() {
		if c.Name == auth.CookieName && c.Value != "" {
			return c
		}
	}
	return nil
}

func loginAs(t *testing.T, h http.Handler, email string) *http.Cookie {
	t.Helper()
	w := do(h, http.MethodPost, "/admin/login", url.Values{"email": {email}, "password": {testPassword}}, nil)
	c := sessionCookieOf(w)
	if c == nil {
		t.Fatalf("login as %s gave no session cookie: %d %s", email, w.Code, w.Body)
	}
	return c
}

func login(t *testing.T, h http.Handler) *http.Cookie { return loginAs(t, h, "admin@example.com") }

func TestAuth_NonAdminRefused(t *testing.T) {
	a, st := newTestAdmin(t)
	h := handler(a)

	// A user with the right password but no admin role gets no session from
	// the back-office.
	w := do(h, http.MethodPost, "/admin/login", url.Values{"email": {"bob@example.com"}, "password": {testPassword}}, nil)
	if sessionCookieOf(w) != nil || !strings.Contains(w.Body.String(), "pas administrateur") || len(st.logins) != 0 {
		t.Errorf("non-admin login: %d, logins %v", w.Code, st.logins)
	}

	// Logged in through the chat, a non-admin is still sent to the login page.
	token, _, err := a.cfg.Auth.Login(context.Background(), "bob@example.com", testPassword)
	if err != nil {
		t.Fatal(err)
	}
	c := &http.Cookie{Name: auth.CookieName, Value: token}
	if w := do(h, http.MethodGet, "/admin/agents", nil, c); w.Code != http.StatusSeeOther {
		t.Errorf("non-admin GET: %d", w.Code)
	}
}

func TestAuth_Login(t *testing.T) {
	a, _ := newTestAdmin(t)
	h := handler(a)

	if w := do(h, http.MethodGet, "/admin/agents", nil, nil); w.Code != http.StatusSeeOther || !strings.HasPrefix(w.Header().Get("Location"), "/admin/login") {
		t.Errorf("GET without session: %d %s", w.Code, w.Header().Get("Location"))
	}
	for _, creds := range [][2]string{{"admin@example.com", "wrong"}, {"nobody@example.com", testPassword}} {
		w := do(h, http.MethodPost, "/admin/login", url.Values{"email": {creds[0]}, "password": {creds[1]}}, nil)
		if sessionCookieOf(w) != nil || !strings.Contains(w.Body.String(), "incorrect") {
			t.Errorf("login %v: %d", creds, w.Code)
		}
	}

	w := do(h, http.MethodPost, "/admin/login", url.Values{"email": {"ADMIN@example.com"}, "password": {testPassword}, "next": {"/admin/tools"}}, nil)
	if w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/admin/tools" {
		t.Errorf("login redirect: %d %q", w.Code, w.Header().Get("Location"))
	}
	// The redirect after login stays inside the back-office.
	for _, next := range []string{"//evil.example", "https://evil.example", "/admin\\..\\x", "/"} {
		w := do(h, http.MethodPost, "/admin/login", url.Values{"email": {"admin@example.com"}, "password": {testPassword}, "next": {next}}, nil)
		if loc := w.Header().Get("Location"); loc != "/admin/" {
			t.Errorf("next=%q redirected to %q", next, loc)
		}
	}

	c := login(t, h)
	if w := do(h, http.MethodGet, "/admin/agents", nil, c); w.Code != 200 {
		t.Errorf("GET with session: %d", w.Code)
	}
	do(h, http.MethodPost, "/admin/logout", url.Values{}, c)
	if w := do(h, http.MethodGet, "/admin/agents", nil, c); w.Code != http.StatusSeeOther {
		t.Errorf("GET after logout: %d", w.Code)
	}
}

func TestAuth_HtmxGetsRedirectHeader(t *testing.T) {
	a, _ := newTestAdmin(t)
	req := httptest.NewRequest(http.MethodGet, "/admin/queues/status", nil)
	req.Header.Set("HX-Request", "true")
	w := httptest.NewRecorder()
	handler(a).ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized || !strings.HasPrefix(w.Header().Get("HX-Redirect"), "/admin/login") {
		t.Errorf("htmx without session: %d %q", w.Code, w.Header().Get("HX-Redirect"))
	}
}

func TestSameOrigin(t *testing.T) {
	a, st := newTestAdmin(t)
	h := handler(a)
	c := login(t, h)

	for _, origin := range []string{"", "http://evil.example"} {
		req := httptest.NewRequest(http.MethodPost, "/admin/agents/coder/delete", nil)
		req.AddCookie(c)
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != http.StatusForbidden {
			t.Errorf("origin %q: %d", origin, w.Code)
		}
	}
	if len(st.agents) != 2 {
		t.Error("a cross-site request deleted an agent")
	}
}

func TestCreateAgent(t *testing.T) {
	a, st := newTestAdmin(t)
	h := handler(a)
	c := login(t, h)

	w := do(h, http.MethodPost, "/admin/agents", url.Values{
		"id": {"writer"}, "name": {"Writer"}, "skills": {"a\n\nb\na"}, "tool": {"read_file", "exec"}, "globs": {" web_* \nread_file\n"},
	}, c)
	if w.Code != http.StatusSeeOther || !strings.HasPrefix(w.Header().Get("Location"), "/admin/agents/writer?saved=") {
		t.Fatalf("create: %d %q %s", w.Code, w.Header().Get("Location"), w.Body)
	}
	got, _ := st.GetAgent(context.Background(), "writer")
	// Checked tools first, then the patterns, without the duplicate.
	if !reflect.DeepEqual(got.Tools, []string{"read_file", "exec", "web_*"}) || !reflect.DeepEqual(got.Skills, []string{"a", "b"}) {
		t.Errorf("stored %+v", got)
	}

	// An empty allowlist is stored as [], never nil: nil would read as "no
	// allowlist" to older code.
	do(h, http.MethodPost, "/admin/agents", url.Values{"id": {"quiet"}, "name": {"Quiet"}, "globs": {"\n  \n"}}, c)
	if q, _ := st.GetAgent(context.Background(), "quiet"); q == nil || q.Tools == nil || len(q.Tools) != 0 {
		t.Errorf("quiet = %+v", q)
	}

	for name, form := range map[string]url.Values{
		"duplicate":    {"id": {"coder"}, "name": {"Again"}},
		"invalid id":   {"id": {"Bad_ID"}, "name": {"Bad"}},
		"no name":      {"id": {"nameless"}},
		"invalid glob": {"id": {"globby"}, "name": {"Globby"}, "globs": {"read_[file"}},
	} {
		before := len(st.agents)
		w := do(h, http.MethodPost, "/admin/agents", form, c)
		if w.Code != 200 || !strings.Contains(w.Body.String(), `class="error"`) || len(st.agents) != before {
			t.Errorf("%s: %d, agents %d → %d", name, w.Code, before, len(st.agents))
		}
	}
}

func TestUpdateAgent_Revision(t *testing.T) {
	a, st := newTestAdmin(t)
	h := handler(a)
	c := login(t, h)

	form := url.Values{"name": {"Coder 2"}, "tool": {"exec", "read_file"}, "revision": {"1"}}
	if w := do(h, http.MethodPost, "/admin/agents/coder", form, c); w.Code != http.StatusSeeOther {
		t.Fatalf("update: %d %s", w.Code, w.Body)
	}
	got, _ := st.GetAgent(context.Background(), "coder")
	if got.Name != "Coder 2" || got.Revision != 2 || len(got.Tools) != 2 {
		t.Errorf("after update: %+v", got)
	}

	// The same form again carries revision 1: it was read before the update
	// above and must not overwrite it.
	form.Set("name", "Stale")
	w := do(h, http.MethodPost, "/admin/agents/coder", form, c)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "modifié ailleurs") {
		t.Errorf("stale update: %d", w.Code)
	}
	if got, _ := st.GetAgent(context.Background(), "coder"); got.Name != "Coder 2" {
		t.Errorf("stale update overwrote: %+v", got)
	}

	// The ID comes from the URL, never from the form.
	form = url.Values{"id": {"hijack"}, "name": {"Coder 3"}, "revision": {"2"}}
	do(h, http.MethodPost, "/admin/agents/coder", form, c)
	if h, _ := st.GetAgent(context.Background(), "hijack"); h != nil {
		t.Error("an update created another agent")
	}
}

func TestDeleteAgent(t *testing.T) {
	a, st := newTestAdmin(t)
	h := handler(a)
	c := login(t, h)

	if w := do(h, http.MethodPost, "/admin/agents/default/delete", url.Values{}, c); w.Code != http.StatusConflict {
		t.Errorf("deleting the default agent: %d", w.Code)
	}
	if w := do(h, http.MethodPost, "/admin/agents/coder/delete", url.Values{}, c); w.Code != http.StatusSeeOther {
		t.Errorf("delete: %d", w.Code)
	}
	if got, _ := st.GetAgent(context.Background(), "coder"); got != nil {
		t.Error("coder still there")
	}
}

func TestAllowlistPreview(t *testing.T) {
	a, _ := newTestAdmin(t)
	h := handler(a)
	c := login(t, h)

	w := do(h, http.MethodGet, "/admin/allowlist/preview", url.Values{"id": {"default"}, "tool": {"exec"}, "globs": {"nope_*\nread_*"}}, c)
	body := w.Body.String()
	if w.Code != 200 || !strings.Contains(body, `chip sens">exec`) || !strings.Contains(body, "ne correspond à aucun tool") {
		t.Errorf("preview: %d %s", w.Code, body)
	}
	// The picker's "via" badges are refreshed out of band, not re-rendered.
	if !strings.Contains(body, `id="via-read_file" class="via" hx-swap-oob="true">via <code>read_*</code>`) {
		t.Errorf("preview misses the via badge of read_file: %s", body)
	}
	// A new agent whose ID is not typed yet still gets a preview.
	if w := do(h, http.MethodGet, "/admin/allowlist/preview", url.Values{"globs": {"*"}}, c); !strings.Contains(w.Body.String(), "4</b> tools") {
		t.Errorf("preview without id: %s", w.Body)
	}
}

func TestFormFromAgent_SplitsAllowlist(t *testing.T) {
	tools := []store.ToolRecord{{Name: "exec"}, {Name: "read_file"}}
	agents := []store.Agent{{ID: "a"}, {ID: "b"}}
	f := formFromAgent(store.Agent{ID: "a", Tools: []string{"read_file", "agent_b", "github_*", "implement_feature", "agent_gone"}}, tools, agents)

	// A published name, or another agent's tool, is a checkbox. A pattern, or
	// a tool that is not there right now, stays text: saving the form must
	// give the same allowlist back.
	if !reflect.DeepEqual(f.Picked, []string{"read_file", "agent_b"}) || f.Globs != "github_*\nimplement_feature\nagent_gone" {
		t.Errorf("picked %v, globs %q", f.Picked, f.Globs)
	}
	if got := f.allowlist(); !reflect.DeepEqual(got, []string{"read_file", "agent_b", "github_*", "implement_feature", "agent_gone"}) {
		t.Errorf("round trip = %v", got)
	}
}

func TestUsers_Create(t *testing.T) {
	a, st := newTestAdmin(t)
	h := handler(a)
	c := login(t, h)

	w := do(h, http.MethodPost, "/admin/users", url.Values{
		"email": {"carol@example.com"}, "display_name": {"Carol"}, "role": {"user"},
		"telegram_id": {"12345"}, "password": {"a long enough secret"},
	}, c)
	if w.Code != http.StatusSeeOther {
		t.Fatalf("create: %d %s", w.Code, w.Body)
	}
	carol, _ := st.GetUserByEmail(context.Background(), "carol@example.com")
	if carol == nil || carol.DisplayName != "Carol" || carol.TelegramID == nil || *carol.TelegramID != 12345 || carol.ID == "" {
		t.Fatalf("stored %+v", carol)
	}
	// The password is stored hashed, and works.
	if ok, _ := auth.VerifyPassword("a long enough secret", carol.PasswordHash); !ok || strings.Contains(carol.PasswordHash, "secret") {
		t.Errorf("password hash %q", carol.PasswordHash)
	}

	for name, form := range map[string]url.Values{
		"duplicate email": {"email": {"BOB@example.com"}, "role": {"user"}, "password": {"a long enough secret"}},
		"bad email":       {"email": {"carol"}, "role": {"user"}, "password": {"a long enough secret"}},
		"short password":  {"email": {"dan@example.com"}, "role": {"user"}, "password": {"short"}},
		"unknown role":    {"email": {"dan@example.com"}, "role": {"root"}, "password": {"a long enough secret"}},
		"bad telegram id": {"email": {"dan@example.com"}, "role": {"user"}, "telegram_id": {"abc"}, "password": {"a long enough secret"}},
	} {
		before := len(st.users)
		w := do(h, http.MethodPost, "/admin/users", form, c)
		body := w.Body.String()
		if w.Code != 200 || !strings.Contains(body, `class="error"`) || len(st.users) != before {
			t.Errorf("%s: %d, users %d → %d", name, w.Code, before, len(st.users))
		}
		if strings.Contains(body, "a long enough secret") {
			t.Errorf("%s: the password was sent back to the browser", name)
		}
	}
}

func TestUsers_AdminCannotLockThemselvesOut(t *testing.T) {
	a, st := newTestAdmin(t)
	h := handler(a)
	c := login(t, h)

	w := do(h, http.MethodPost, "/admin/users/u-admin", url.Values{"email": {"admin@example.com"}, "role": {"user"}}, c)
	if !strings.Contains(w.Body.String(), "retirer le rôle admin") {
		t.Errorf("self-demotion: %d", w.Code)
	}
	if w := do(h, http.MethodPost, "/admin/users/u-admin/disable", url.Values{}, c); w.Code != http.StatusConflict {
		t.Errorf("self-disable: %d", w.Code)
	}
	if u, _ := st.GetUser(context.Background(), "u-admin"); !u.IsAdmin() || u.DisabledAt != nil {
		t.Errorf("admin changed: %+v", u)
	}
}

func TestUsers_DisableAndPasswordEndSessions(t *testing.T) {
	a, st := newTestAdmin(t)
	h := handler(a)
	c := login(t, h)
	ctx := context.Background()

	bobToken, _, _ := a.cfg.Auth.Login(ctx, "bob@example.com", testPassword)
	do(h, http.MethodPost, "/admin/users/u-bob/disable", url.Values{}, c)
	if u, _ := st.GetLoginSessionUser(ctx, hashOf(bobToken)); u != nil {
		t.Error("a disabled user is still logged in")
	}
	if _, _, err := a.cfg.Auth.Login(ctx, "bob@example.com", testPassword); err == nil {
		t.Error("a disabled user can log in")
	}
	do(h, http.MethodPost, "/admin/users/u-bob/enable", url.Values{}, c)

	bobToken, _, _ = a.cfg.Auth.Login(ctx, "bob@example.com", testPassword)
	w := do(h, http.MethodPost, "/admin/users/u-bob/password", url.Values{"password": {"another long secret"}}, c)
	if w.Code != http.StatusSeeOther {
		t.Fatalf("reset: %d %s", w.Code, w.Body)
	}
	if u, _ := st.GetLoginSessionUser(ctx, hashOf(bobToken)); u != nil {
		t.Error("the old session survived a password reset")
	}
	if _, _, err := a.cfg.Auth.Login(ctx, "bob@example.com", "another long secret"); err != nil {
		t.Errorf("new password refused: %v", err)
	}
}

// hashOf mirrors how the auth service keys login sessions.
func hashOf(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
