package admin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/victor/temporal-agent/store"
)

// fakeStore keeps agents in memory with the revision rules of PostgresStore.
// Methods the back-office does not call are left to the nil embedded Store.
type fakeStore struct {
	store.Store
	agents []store.Agent
	tools  []store.ToolRecord
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

const testKey = "s3cret"

func newTestAdmin(t *testing.T, key string) (*Admin, *fakeStore) {
	t.Helper()
	loginFailDelay = 0
	st := &fakeStore{
		agents: []store.Agent{
			{ID: "default", Name: "Default", Tools: []string{"read_file"}, Revision: 1},
			{ID: "coder", Name: "Coder", Tools: []string{"exec"}, Revision: 1},
		},
		tools: []store.ToolRecord{
			{Name: "exec", Kind: "activity", TaskQueue: "tools"},
			{Name: "read_file", Kind: "activity", TaskQueue: "tools"},
		},
	}
	return New(Config{Store: st, DefaultAgentID: "default", WorkflowQueue: "agent", AdminKey: key}), st
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

func login(t *testing.T, h http.Handler) *http.Cookie {
	t.Helper()
	w := do(h, http.MethodPost, "/admin/login", url.Values{"password": {testKey}}, nil)
	for _, c := range w.Result().Cookies() {
		if c.Name == sessionCookie && c.Value != "" {
			return c
		}
	}
	t.Fatalf("login gave no session cookie: %d %s", w.Code, w.Body)
	return nil
}

func TestAuth_ClosedWithoutKey(t *testing.T) {
	a, _ := newTestAdmin(t, "")
	h := handler(a)

	if w := do(h, http.MethodGet, "/admin/agents", nil, nil); w.Code != http.StatusSeeOther || !strings.HasPrefix(w.Header().Get("Location"), "/admin/login") {
		t.Errorf("GET without session: %d %s", w.Code, w.Header().Get("Location"))
	}
	// Even an empty password must not open a back-office with no key.
	w := do(h, http.MethodPost, "/admin/login", url.Values{"password": {""}}, nil)
	if len(w.Result().Cookies()) != 0 {
		t.Error("login succeeded with no ADMIN_API_KEY")
	}
	if !strings.Contains(do(h, http.MethodGet, "/admin/login", nil, nil).Body.String(), "fermé") {
		t.Error("login page should say the back-office is closed")
	}
}

func TestAuth_Login(t *testing.T) {
	a, _ := newTestAdmin(t, testKey)
	h := handler(a)

	w := do(h, http.MethodPost, "/admin/login", url.Values{"password": {"wrong"}}, nil)
	if len(w.Result().Cookies()) != 0 || !strings.Contains(w.Body.String(), "incorrect") {
		t.Errorf("wrong password: cookies %v", w.Result().Cookies())
	}

	w = do(h, http.MethodPost, "/admin/login", url.Values{"password": {testKey}, "next": {"/admin/tools"}}, nil)
	if w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/admin/tools" {
		t.Errorf("login redirect: %d %q", w.Code, w.Header().Get("Location"))
	}
	// The redirect after login stays inside the back-office.
	for _, next := range []string{"//evil.example", "https://evil.example", "/admin\\..\\x", "/"} {
		w := do(h, http.MethodPost, "/admin/login", url.Values{"password": {testKey}, "next": {next}}, nil)
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
	a, _ := newTestAdmin(t, testKey)
	req := httptest.NewRequest(http.MethodGet, "/admin/queues/status", nil)
	req.Header.Set("HX-Request", "true")
	w := httptest.NewRecorder()
	handler(a).ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized || !strings.HasPrefix(w.Header().Get("HX-Redirect"), "/admin/login") {
		t.Errorf("htmx without session: %d %q", w.Code, w.Header().Get("HX-Redirect"))
	}
}

func TestSameOrigin(t *testing.T) {
	a, st := newTestAdmin(t, testKey)
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
	a, st := newTestAdmin(t, testKey)
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
	a, st := newTestAdmin(t, testKey)
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
	a, st := newTestAdmin(t, testKey)
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
	a, _ := newTestAdmin(t, testKey)
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
