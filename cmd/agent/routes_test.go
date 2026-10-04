package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/auth"
	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/session"
	"github.com/victor/temporal-agent/sse"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/web/admin"
	"github.com/victor/temporal-agent/web/chat"
	"github.com/victor/temporal-agent/workflow"
)

// routeStore holds users, logins and one session in memory. What the routes
// under test never reach answers empty (see fakes_test.go).
type routeStore struct {
	users    []store.User
	logins   map[string]string
	session  store.Session
	members  []string
	messages map[string][]store.MessageWithID
	created  []store.Session
	appended []store.Message
	tools    []store.ToolRecord
	// Other sessions than s1, with their members: a fork's parent.
	others       map[string]store.Session
	otherMembers map[string][]string
	// loads counts the loads of each session's conversation.
	loads map[string]int
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
		if s, ok := f.others[id]; ok {
			return &s, nil
		}
		return nil, nil
	}
	return &f.session, nil
}

func (f *routeStore) IsSessionMember(_ context.Context, sessionID, userID string) (bool, error) {
	if sessionID != f.session.SessionID {
		return slices.Contains(f.otherMembers[sessionID], userID), nil
	}
	for _, m := range f.members {
		if m == userID {
			return true, nil
		}
	}
	return false, nil
}

func (f *routeStore) SessionMembership(ctx context.Context, sessionID, userID string) (int, bool, error) {
	members, _ := f.ListSessionMembers(ctx, sessionID)
	ok, _ := f.IsSessionMember(ctx, sessionID, userID)
	return len(members), ok, nil
}

func (f *routeStore) ListSessionMembers(_ context.Context, sessionID string) ([]store.SessionMember, error) {
	var out []store.SessionMember
	members := f.members
	if sessionID != f.session.SessionID {
		members = f.otherMembers[sessionID]
	}
	for _, id := range members {
		u := f.user(func(u store.User) bool { return u.ID == id })
		out = append(out, store.SessionMember{UserID: id, Email: u.Email})
	}
	return out, nil
}

func (f *routeStore) AddSessionMember(_ context.Context, _, userID, _ string) error {
	f.members = append(f.members, userID)
	return nil
}

// DeleteSession deletes s1's members along: the rest stays readable.
func (f *routeStore) DeleteSession(_ context.Context, id string) error {
	if id == f.session.SessionID {
		f.members = nil
	}
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

// liveSID is a session ID the server learns from: the hub's observers take
// only a canonical UUID for a session (session.IsSessionTopic). The tests
// that need it give it to the store's session in place of "s1".
const liveSID = "6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b"

func newRouteTest(t *testing.T) (http.Handler, *routeStore) {
	t.Helper()
	return newRouteTestWith(t, nil)
}

// newRouteTestWith builds the router over tc, a stand-in for Temporal: the
// routes that start workflows need one.
func newRouteTestWith(t *testing.T, tc session.Temporal) (http.Handler, *routeStore) {
	t.Helper()
	h, st, _ := newRouteTestHub(t, tc)
	return h, st
}

// newRouteTestHub is newRouteTestWith, with the server's hub: what the
// workers publish goes there.
func newRouteTestHub(t *testing.T, tc session.Temporal) (http.Handler, *routeStore, *sse.Hub) {
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
	hub := sse.NewHub()
	srv := newServer(&config.Config{WorkflowQueue: "agent", DefaultAgentID: "default"}, st, tc, hub, svc, admin.New(admin.Config{Auth: svc}).Routes())
	return srv.routes(), st, hub
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
	// The pages' streams check membership themselves: logging in comes first.
	for _, path := range []string{"/s/" + liveSID + "/stream", "/tree/stream"} {
		if w := call(t, h, http.MethodGet, path, "", nil); w.Code != http.StatusSeeOther || !strings.HasPrefix(w.Header().Get("Location"), "/login") {
			t.Errorf("%s without login: %d %s", path, w.Code, w.Header().Get("Location"))
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
	w := call(t, h, http.MethodPost, "/sessions/s1/answer", `{"workflow_id":"s2:p:default:m3:tool:ask_user:1-0","answer":"yes"}`, bob)
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

// --- Forks ---

// fakeTemporal records the workflows started and the signals sent. Nothing
// runs: a description says not running, a query and a termination fail.
type fakeTemporal struct {
	started []string // workflow IDs
	signals []interface{}
}

func (f *fakeTemporal) QueryWorkflow(context.Context, string, string, string, ...interface{}) (converter.EncodedValue, error) {
	return nil, errors.New("not running")
}

func (f *fakeTemporal) TerminateWorkflow(context.Context, string, string, string, ...interface{}) error {
	return errors.New("not running")
}

func (f *fakeTemporal) ExecuteWorkflow(_ context.Context, opts client.StartWorkflowOptions, _ interface{}, _ ...interface{}) (client.WorkflowRun, error) {
	f.started = append(f.started, opts.ID)
	return nil, nil
}

func (f *fakeTemporal) DescribeWorkflowExecution(context.Context, string, string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	return nil, errors.New("not running")
}

func (f *routeStore) LoadMessagesUpTo(_ context.Context, sessionID string, lastID int64) ([]store.MessageWithID, error) {
	f.countLoad(sessionID)
	var out []store.MessageWithID
	for _, m := range f.messages[sessionID] {
		if lastID == 0 || m.ID <= lastID {
			out = append(out, m)
		}
	}
	return out, nil
}

func (f *routeStore) countLoad(sessionID string) {
	if f.loads == nil {
		f.loads = map[string]int{}
	}
	f.loads[sessionID]++
}

func (f *routeStore) CreateSession(_ context.Context, s store.Session) error {
	f.created = append(f.created, s)
	return nil
}

func (f *routeStore) ListForks(context.Context, string, string) ([]store.Session, error) {
	return nil, nil
}

func newForkTest(t *testing.T) (http.Handler, *routeStore, *fakeTemporal) {
	t.Helper()
	tc := &fakeTemporal{}
	h, st := newRouteTestWith(t, tc)
	st.session.Title = "Plan"
	st.messages = map[string][]store.MessageWithID{"s1": {
		{ID: 1, Message: store.Message{Role: store.RoleUser, Content: `"question"`}},
		{ID: 2, Message: store.Message{Role: store.RoleAssistant, Content: `"answer"`}},
		{ID: 3, Message: store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{Content: "raw"}}},
	}}
	return h, st, tc
}

func TestRoutes_Fork(t *testing.T) {
	h, st, tc := newForkTest(t)
	bob := logIn(t, h, "bob@example.com")

	w := call(t, h, http.MethodPost, "/sessions/s1/fork", `{"message_id":2}`, bob)
	if w.Code != http.StatusCreated {
		t.Fatalf("fork: %d %s", w.Code, w.Body)
	}
	var resp createSessionResponse
	json.Unmarshal(w.Body.Bytes(), &resp)
	if len(st.created) != 1 {
		t.Fatalf("created %v", st.created)
	}
	f := st.created[0]
	// The forking user alone, the parent's agent, and where it started from.
	if f.SessionID != resp.SessionID || f.CreatedBy != "u-bob" || f.ForkedBy != "u-bob" ||
		f.ParentSessionID != "s1" || f.ForkedAtMessageID != 2 || f.Title != "Fork : Plan" {
		t.Errorf("fork %+v", f)
	}
	if len(tc.started) != 1 || tc.started[0] != "fork-"+f.SessionID {
		t.Errorf("workflows started %v", tc.started)
	}

	// A purpose titles the fork; one too long is refused.
	if w := call(t, h, http.MethodPost, "/sessions/s1/fork", `{"message_id":2,"purpose":"  Export CSV  "}`, bob); w.Code != http.StatusCreated ||
		len(st.created) != 2 || st.created[1].ForkPurpose != "Export CSV" || st.created[1].Title != "Export CSV" {
		t.Errorf("fork with a purpose: %d, %+v", w.Code, st.created)
	}
	if w := call(t, h, http.MethodPost, "/sessions/s1/fork", `{"message_id":2,"purpose":"`+strings.Repeat("x", session.MaxPurposeRunes+1)+`"}`, bob); w.Code != http.StatusBadRequest {
		t.Errorf("a purpose too long: %d", w.Code)
	}

	for body, why := range map[string]string{
		`{"message_id":3}`:  "a tool result",
		`{"message_id":99}`: "a message of no session",
		`{}`:                "no message",
	} {
		if w := call(t, h, http.MethodPost, "/sessions/s1/fork", body, bob); w.Code != http.StatusBadRequest {
			t.Errorf("forking from %s: %d", why, w.Code)
		}
	}

	carol := logIn(t, h, "carol@example.com")
	if w := call(t, h, http.MethodPost, "/sessions/s1/fork", `{"message_id":2}`, carol); w.Code != http.StatusNotFound {
		t.Errorf("a non-member forking: %d", w.Code)
	}
}

func TestRoutes_ForkInfoHidesAnInaccessibleParent(t *testing.T) {
	h, st, _ := newForkTest(t)
	// s1 is now a fork of "secret", which bob is no member of.
	st.session.ParentSessionID, st.session.ForkedAtMessageID = "secret", 1
	st.messages["s1"][0].Kind, st.session.SummaryMessageID = store.KindForkSummary, 1
	bob := logIn(t, h, "bob@example.com")

	w := call(t, h, http.MethodGet, "/sessions/s1", "", bob)
	if w.Code != 200 {
		t.Fatalf("info: %d %s", w.Code, w.Body)
	}
	var info struct {
		Summary string `json:"summary"`
		Parent  map[string]any
	}
	json.Unmarshal(w.Body.Bytes(), &info)
	if info.Summary != "ready" || info.Parent["accessible"] != false || info.Parent["session_id"] != nil || info.Parent["title"] != nil {
		t.Errorf("info %s", w.Body)
	}
}

func (f *routeStore) ListAgents(context.Context) ([]store.Agent, error) {
	return []store.Agent{{ID: "default", Name: "Default"}}, nil
}

// --- Messages and @agent ---

func (f *routeStore) AppendMessage(_ context.Context, sessionID, key string, m store.Message) (int64, error) {
	f.appended = append(f.appended, m)
	return int64(len(f.appended)), nil
}

func (f *routeStore) SetSessionAgentMode(_ context.Context, _, mode string) error {
	f.session.AgentMode = mode
	return nil
}

func (f *routeStore) UpdateSessionTitle(context.Context, string, string) error { return nil }

func (f *fakeTemporal) ListWorkflow(context.Context, *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	return &workflowservice.ListWorkflowExecutionsResponse{}, nil
}

func (f *fakeTemporal) SignalWorkflow(_ context.Context, workflowID, _, signal string, arg interface{}) error {
	f.signals = append(f.signals, arg)
	return nil
}

func (f *fakeTemporal) SignalWithStartWorkflow(_ context.Context, workflowID, signal string, arg interface{}, _ client.StartWorkflowOptions, _ interface{}, _ ...interface{}) (client.WorkflowRun, error) {
	f.signals = append(f.signals, arg)
	return nil, nil
}

func TestRoutes_MessagesCallTheAgentOnlyWhenMeant(t *testing.T) {
	tc := &fakeTemporal{}
	h, st := newRouteTestWith(t, tc)
	bob := logIn(t, h, "bob@example.com")
	send := func(text string) bool {
		t.Helper()
		w := call(t, h, http.MethodPost, "/sessions/s1/messages", `{"content":"`+text+`"}`, bob)
		if w.Code != http.StatusAccepted {
			t.Fatalf("send %q: %d %s", text, w.Code, w.Body)
		}
		var resp struct {
			AgentCalled bool `json:"agent_called"`
		}
		json.Unmarshal(w.Body.Bytes(), &resp)
		return resp.AgentCalled
	}

	// s1 has two members: in auto mode, they talk to each other.
	if send("on se voit demain ?") {
		t.Error("a plain message between two members called the agent")
	}
	if len(st.appended) != 1 || st.appended[0].Author != "bob@example.com" || st.appended[0].UserID != "u-bob" {
		t.Errorf("not stored as bob's: %+v", st.appended)
	}
	if len(tc.signals) != 0 {
		t.Errorf("signals %v", tc.signals)
	}

	// The default agent has no mention set: its ID calls it.
	if send("@agent résume la discussion") {
		t.Error("@agent called an agent whose mention is @default")
	}
	if !send("@default résume la discussion") {
		t.Error("@default did not call the agent")
	}
	if len(tc.signals) != 1 {
		t.Fatalf("signals %v", tc.signals)
	}
	msg := tc.signals[0].(workflow.ParticipantMessage)
	// The turn loads the message from the store: the signal names it.
	if msg.MessageID == 0 || msg.UserID != "u-bob" {
		t.Errorf("signal %+v", msg)
	}
	if len(st.appended) != 3 {
		t.Errorf("%d messages stored, want all three", len(st.appended))
	}

	// The session can call the agent on every message.
	if w := call(t, h, http.MethodPut, "/sessions/s1/agent-mode", `{"mode":"always"}`, bob); w.Code != http.StatusNoContent {
		t.Fatalf("set mode: %d", w.Code)
	}
	if !send("et maintenant ?") {
		t.Error("mode always did not call the agent")
	}
	if w := call(t, h, http.MethodPut, "/sessions/s1/agent-mode", `{"mode":"sometimes"}`, bob); w.Code != http.StatusBadRequest {
		t.Errorf("unknown mode: %d", w.Code)
	}
}

// The history hides a private input, as the tool's worker published it.
func TestRoutes_HistoryHidesPrivateInputs(t *testing.T) {
	h, st := newRouteTest(t)
	st.tools = []store.ToolRecord{{Name: "save_user_memory", PrivateInput: true}, {Name: "web_fetch"}}
	st.messages = map[string][]store.MessageWithID{"s1": {
		{ID: 1, Message: store.Message{Role: store.RoleAssistant, ToolCalls: []store.ToolCall{
			{Name: "save_user_memory", Input: json.RawMessage(`{"content":"Alice's secret"}`)},
			{Name: "web_fetch", Input: json.RawMessage(`{"url":"https://example.com"}`)},
		}}},
	}}
	bob := logIn(t, h, "bob@example.com")
	w := call(t, h, http.MethodGet, "/sessions/s1/history", "", bob)
	if w.Code != 200 || strings.Contains(w.Body.String(), "Alice's secret") || !strings.Contains(w.Body.String(), "example.com") {
		t.Errorf("history %d %s", w.Code, w.Body)
	}
}

// Why a turn failed is listed as such, not as an answer of the agent; the
// end of a turn that did not fail is not listed.
func TestRoutes_HistoryListsATurnErrorApart(t *testing.T) {
	h, st := newRouteTest(t)
	st.messages = map[string][]store.MessageWithID{"s1": {
		{ID: 1, Message: store.TurnEnd("default", "call LLM: credit balance is too low")},
		{ID: 2, Message: store.TurnEnd("default", "")},
	}}
	bob := logIn(t, h, "bob@example.com")
	w := call(t, h, http.MethodGet, "/sessions/s1/history", "", bob)
	var history []map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &history); err != nil || len(history) != 1 {
		t.Fatalf("history %d %s (%v)", w.Code, w.Body, err)
	}
	if e := history[0]; e["type"] != "turn_error" || e["role"] != nil || e["content"] != "call LLM: credit balance is too low" {
		t.Errorf("entry %v, want a turn_error with no role", e)
	}
}

// form posts an htmx form to the interface.
func form(t *testing.T, h http.Handler, path string, values url.Values, cookie *http.Cookie) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(values.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Origin", "http://example.com")
	req.Header.Set("HX-Request", "true")
	req.AddCookie(cookie)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

// The interface's fork form carries the purpose; htmx then opens the fork.
func TestUI_ForkFormTakesAPurpose(t *testing.T) {
	h, st, _ := newForkTest(t)
	bob := logIn(t, h, "bob@example.com")
	w := form(t, h, "/s/s1/fork", url.Values{"message_id": {"2"}, "purpose": {"Écrire l'export CSV"}}, bob)
	if w.Code != http.StatusOK || len(st.created) != 1 {
		t.Fatalf("fork form: %d %s", w.Code, w.Body)
	}
	f := st.created[0]
	if f.ForkPurpose != "Écrire l'export CSV" || f.Title != "Écrire l'export CSV" || w.Header().Get("HX-Location") != "/s/"+f.SessionID {
		t.Errorf("fork %+v, location %q", f, w.Header().Get("HX-Location"))
	}
}

// A fork's report is listed as such, with its sender and its fork.
func TestRoutes_HistoryListsAForkReport(t *testing.T) {
	h, st := newRouteTest(t)
	st.messages = map[string][]store.MessageWithID{"s1": {
		{ID: 1, Message: store.Message{Role: store.RoleUser, Kind: store.KindForkReport, Content: `"done"`, UserID: "u-bob", Author: "Bob",
			Fork: &store.ForkRef{SessionID: "f1", Title: "Export", UpToMessageID: 7}}},
	}}
	bob := logIn(t, h, "bob@example.com")
	w := call(t, h, http.MethodGet, "/sessions/s1/history", "", bob)
	var history []map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &history); err != nil || len(history) != 1 {
		t.Fatalf("history %d %s (%v)", w.Code, w.Body, err)
	}
	fork, _ := history[0]["fork"].(map[string]any)
	if e := history[0]; e["type"] != "fork_report" || e["author"] != "Bob" || e["content"] != "done" || fork["session_id"] != "f1" || fork["up_to_message_id"] != 7.0 {
		t.Errorf("entry %v", e)
	}
}

// newReportTest makes s1 a fork of p1, its brief written. Alice and Bob are
// members of s1; Bob alone is a member of p1.
func newReportTest(t *testing.T) (http.Handler, *routeStore, *fakeTemporal) {
	t.Helper()
	h, st, tc := newForkTest(t)
	st.session.ParentSessionID, st.session.ForkedAtMessageID = "p1", 1
	st.messages["s1"][0].Kind, st.session.SummaryMessageID = store.KindForkSummary, 1
	st.others = map[string]store.Session{"p1": {SessionID: "p1", CreatedBy: "u-bob", Title: "Plan"}}
	st.otherMembers = map[string][]string{"p1": {"u-bob"}}
	return h, st, tc
}

// A member of the fork and of its parent reports; a member of the fork alone
// cannot, nor can a stranger to the fork.
func TestRoutes_ReportToParent(t *testing.T) {
	h, _, tc := newReportTest(t)
	bob, alice, carol := logIn(t, h, "bob@example.com"), logIn(t, h, "alice@example.com"), logIn(t, h, "carol@example.com")

	if w := call(t, h, http.MethodPost, "/sessions/s1/report", "", alice); w.Code != http.StatusForbidden {
		t.Errorf("a member of the fork alone: %d %s", w.Code, w.Body)
	}
	if w := call(t, h, http.MethodPost, "/sessions/s1/report", "", carol); w.Code != http.StatusNotFound {
		t.Errorf("a stranger to the fork: %d", w.Code)
	}
	if len(tc.started) != 0 {
		t.Fatalf("started %v", tc.started)
	}
	if w := call(t, h, http.MethodPost, "/sessions/s1/report", "", bob); w.Code != http.StatusAccepted {
		t.Fatalf("bob: %d %s", w.Code, w.Body)
	}
	if len(tc.started) != 1 || tc.started[0] != workflow.ReportWorkflowID("s1", 0) {
		t.Errorf("started %v", tc.started)
	}

	// The session's info says who can report, and why not.
	var info struct {
		Report struct {
			CanReport bool   `json:"can_report"`
			Refused   string `json:"refused"`
		} `json:"report"`
	}
	json.Unmarshal(call(t, h, http.MethodGet, "/sessions/s1", "", bob).Body.Bytes(), &info)
	if !info.Report.CanReport || info.Report.Refused != "" {
		t.Errorf("bob's report state %+v", info.Report)
	}
	json.Unmarshal(call(t, h, http.MethodGet, "/sessions/s1", "", alice).Body.Bytes(), &info)
	if info.Report.CanReport || info.Report.Refused != session.ErrNotParentMember.Error() {
		t.Errorf("alice's report state %+v", info.Report)
	}
}

// A session that is not a fork, or with nothing to report: a conflict.
func TestRoutes_ReportConflicts(t *testing.T) {
	h, st, _ := newReportTest(t)
	bob := logIn(t, h, "bob@example.com")
	st.session.LastReportedMessageID = 3 // everything reported
	if w := call(t, h, http.MethodPost, "/sessions/s1/report", "", bob); w.Code != http.StatusConflict {
		t.Errorf("nothing new: %d", w.Code)
	}
	st.session.ParentSessionID, st.session.ForkedAtMessageID = "", 0
	if w := call(t, h, http.MethodPost, "/sessions/s1/report", "", bob); w.Code != http.StatusConflict {
		t.Errorf("not a fork: %d", w.Code)
	}
}

// The interface's button answers with the report section: started, or why
// not; the rail shows it in a fork.
func TestUI_ReportToParent(t *testing.T) {
	h, st, tc := newReportTest(t)
	bob, alice := logIn(t, h, "bob@example.com"), logIn(t, h, "alice@example.com")

	w := form(t, h, "/s/s1/report", url.Values{}, bob)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `Rapport au parent`) || len(tc.started) != 1 {
		t.Errorf("bob: %d %s, started %v", w.Code, w.Body, tc.started)
	}
	// The section says why alice cannot, once: the refusal is its reason.
	w = form(t, h, "/s/s1/report", url.Values{}, alice)
	if body := w.Body.String(); strings.Count(body, "tu n&#39;en es pas membre") != 1 || strings.Contains(body, "Rapport impossible") ||
		strings.Contains(body, "<button") || len(tc.started) != 1 {
		t.Errorf("alice: %d %s", w.Code, w.Body)
	}

	// The button asks first, naming where the report goes and to how many
	// readers; what it does is written under it, not in a tooltip.
	req := httptest.NewRequest(http.MethodGet, "/s/s1", nil)
	req.AddCookie(bob)
	page := httptest.NewRecorder()
	st.loads = nil
	h.ServeHTTP(page, req)
	body := page.Body.String()
	if st.loads["s1"] != 1 {
		t.Errorf("the fork page loaded its conversation %d times, want once", st.loads["s1"])
	}
	if page.Code != http.StatusOK || !strings.Contains(body, "⑂ Rapporter au parent") ||
		!strings.Contains(body, `hx-confirm="Poster dans « Plan » (1 membre) un résumé de ce fork, signé de ton nom ?"`) ||
		!strings.Contains(body, "signé de ton nom : ses membres le liront, même ceux qui ne sont pas dans ce fork.") {
		t.Errorf("fork page: %d %s", page.Code, body)
	}

	// The report section, reloaded on the fork's events, loads the
	// conversation once at most; a message to the fork
	// loads none (its summary's state is on its row).
	req = httptest.NewRequest(http.MethodGet, "/s/s1/report", nil)
	req.AddCookie(bob)
	section := httptest.NewRecorder()
	st.loads = nil
	h.ServeHTTP(section, req)
	if section.Code != http.StatusOK || !strings.Contains(section.Body.String(), `Rapport au parent`) || st.loads["s1"] > 1 {
		t.Errorf("report section: %d, %d loads %s", section.Code, st.loads["s1"], section.Body)
	}
	st.loads = nil
	if w := call(t, h, http.MethodPost, "/sessions/s1/messages", `{"content":"done here"}`, bob); w.Code != http.StatusAccepted || st.loads["s1"] != 0 {
		t.Errorf("a message to the fork: %d, %d loads %s", w.Code, st.loads["s1"], w.Body)
	}
}

// A session that is no fork has no report section; a post there gets one
// saying why, rather than an empty answer.
func TestUI_ReportFromANonFork(t *testing.T) {
	h, _, tc := newForkTest(t)
	w := form(t, h, "/s/s1/report", url.Values{}, logIn(t, h, "bob@example.com"))
	if body := w.Body.String(); w.Code != http.StatusOK || !strings.Contains(body, `Rapport au parent`) ||
		!strings.Contains(body, "pas un fork") || strings.Contains(body, "<button") || len(tc.started) != 0 {
		t.Errorf("%d %s, started %v", w.Code, body, tc.started)
	}
}

// get loads a page or a fragment as htmx does, holding version when it is
// not empty.
func get(t *testing.T, h http.Handler, path, version string, cookie *http.Cookie) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("HX-Request", "true")
	if version != "" {
		req.Header.Set(chat.VersionHeader, version)
	}
	req.AddCookie(cookie)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

var versionRe = regexp.MustCompile(`data-version="([0-9a-f]+)"`)

// versionIn is the version a fragment carries, and the page holds.
func versionIn(t *testing.T, body string) string {
	t.Helper()
	m := versionRe.FindStringSubmatch(body)
	if m == nil {
		t.Fatalf("no version in %s", body)
	}
	return m[1]
}

// A reload of an unchanged fragment is a 204: htmx swaps nothing. The page
// holds the versions its reloads get, so its first reload is one too.
func TestUI_UnchangedFragmentsAreNoContent(t *testing.T) {
	h, st, _ := newReportTest(t)
	bob := logIn(t, h, "bob@example.com")
	page := get(t, h, "/s/s1", "", bob).Body.String()
	for _, path := range []string{"/s/s1/thread", "/tree?current=s1", "/s/s1/report"} {
		t.Run(path, func(t *testing.T) {
			w := get(t, h, path, "", bob)
			if w.Code != http.StatusOK {
				t.Fatalf("first load: %d %s", w.Code, w.Body)
			}
			v := versionIn(t, w.Body.String())
			if !strings.Contains(page, `data-version="`+v+`"`) {
				t.Errorf("the page does not hold version %s", v)
			}
			if w := get(t, h, path, v, bob); w.Code != http.StatusNoContent || w.Body.Len() != 0 {
				t.Errorf("unchanged: %d %s", w.Code, w.Body)
			}
			if w := get(t, h, path, "stale", bob); w.Code != http.StatusOK || versionIn(t, w.Body.String()) != v {
				t.Errorf("another version held: %d", w.Code)
			}
		})
	}

	// A change shows: a new message, a new title.
	thread := versionIn(t, get(t, h, "/s/s1/thread", "", bob).Body.String())
	tree := versionIn(t, get(t, h, "/tree?current=s1", "", bob).Body.String())
	report := versionIn(t, get(t, h, "/s/s1/report", "", bob).Body.String())
	st.messages["s1"] = append(st.messages["s1"], store.MessageWithID{ID: 4, Message: store.Message{Role: store.RoleUser, Content: `"more"`, UserID: "u-bob", Author: "Bob"}})
	st.session.Title = "Plan B"
	st.session.LastReportedMessageID = 4 // nothing new: the button goes grey
	if w := get(t, h, "/s/s1/thread", thread, bob); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "more") || versionIn(t, w.Body.String()) == thread {
		t.Errorf("thread after a message: %d", w.Code)
	}
	if w := get(t, h, "/tree?current=s1", tree, bob); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "Plan B") {
		t.Errorf("tree after a new title: %d", w.Code)
	}
	if w := get(t, h, "/s/s1/report", report, bob); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "Rien à rapporter") {
		t.Errorf("report after everything was reported: %d %s", w.Code, w.Body)
	}
}

// The thread shows each answer under the message it answers, and "Forker
// un fil" forks from the latest message, which is no longer the last one
// shown: Jarvis answered Alice (3) after Smith answered Bob (2, then 4).
func TestUI_TheThreadInTheOrderOfTheAnchors(t *testing.T) {
	h, st := newRouteTestWith(t, &fakeTemporal{})
	st.messages = map[string][]store.MessageWithID{"s1": {
		{ID: 1, Key: "msg:a", Message: store.Message{Role: store.RoleUser, Content: `"Question d'Alice"`, UserID: "u-alice", Author: "Alice"}},
		{ID: 2, Key: "msg:b", Message: store.Message{Role: store.RoleUser, Content: `"Question de Bob"`, UserID: "u-bob", Author: "Bob"}},
		{ID: 3, Key: "m2.smith:0", Message: store.Message{Role: store.RoleAssistant, Content: `"Réponse à Bob"`, AgentID: "smith"}},
		{ID: 4, Key: "m1.default:0", Message: store.Message{Role: store.RoleAssistant, Content: `"Réponse à Alice"`, AgentID: "default"}},
	}}
	bob := logIn(t, h, "bob@example.com")
	body := get(t, h, "/s/s1/thread", "", bob).Body.String()
	order := []int{}
	for _, s := range []string{"Question d&#39;Alice", "Réponse à Alice", "Question de Bob", "Réponse à Bob"} {
		order = append(order, strings.Index(body, s))
	}
	if !slices.IsSorted(order) || order[0] < 0 {
		t.Errorf("positions %v, want each answer under its question: %s", order, body)
	}
	if !strings.Contains(body, `name="message_id" value="4"`) {
		t.Errorf("forks from another message than the latest: %s", body)
	}
}

// A page's stream starts where the page was rendered: what is published
// while it loads is sent again when the stream connects.
func TestUI_TheStreamStartsWhereThePageStands(t *testing.T) {
	h, _ := newRouteTestWith(t, &fakeTemporal{})
	bob := logIn(t, h, "bob@example.com")
	body := get(t, h, "/s/s1", "", bob).Body.String()
	m := regexp.MustCompile(`sse-connect="/s/s1/stream\?last_event_id=([0-9a-z]+-[0-9]+)"`).FindStringSubmatch(body)
	if m == nil {
		t.Fatalf("no stream position on the page: %s", body)
	}
}

// The thread says who works as soon as a turn starts, and stops as soon as
// it ends: from the turn events, as a worker apart posts them to the
// internal API, or as a dev process publishes them on the hub.
func TestUI_TheThreadNamesWhoWorks(t *testing.T) {
	h, st, hub := newRouteTestHub(t, &fakeTemporal{})
	st.session.SessionID = liveSID
	bob := logIn(t, h, "bob@example.com")
	notify := handleInternalNotify(hub, "k3y")
	turn := func(typ, agentID, name string) {
		t.Helper()
		data, _ := json.Marshal(workflow.TurnEvent{AgentID: agentID, AgentName: name, Turn: "k"})
		body, _ := json.Marshal(activity.NotifyInput{SessionID: liveSID, Event: activity.SSEEvent{Type: typ, Data: data}})
		if code := post(notify, "/internal/notify", string(body), map[string]string{"Authorization": "Bearer k3y"}); code != http.StatusNoContent {
			t.Fatalf("notify: %d", code)
		}
	}
	thread := func() string { return get(t, h, "/s/"+liveSID+"/thread", "", bob).Body.String() }

	if out := thread(); strings.Contains(out, "travaille") {
		t.Fatalf("idle: %s", out)
	}
	turn(workflow.EventTurnStarted, "default", "")
	if out := thread(); !strings.Contains(out, "Default travaille…") {
		t.Errorf("the session's agent, named from the agents: %s", out)
	}
	turn(workflow.EventTurnDone, "default", "")
	turn(workflow.EventTurnStarted, "gone", "Ancien agent")
	if out := thread(); !strings.Contains(out, "Ancien agent travaille…") {
		t.Errorf("an agent the server does not know, by the event's name: %s", out)
	}
	turn(workflow.EventTurnDone, "gone", "Ancien agent")
	if out := thread(); strings.Contains(out, "travaille") {
		t.Errorf("after the turn: %s", out)
	}

	// In a dev process, the worker publishes on the hub itself.
	data, _ := json.Marshal(workflow.TurnEvent{AgentID: "default", Turn: "k2"})
	activity.HubNotifier{Hub: hub}.Notify(context.Background(), activity.Notification{SessionID: liveSID, Event: activity.SSEEvent{Type: workflow.EventTurnStarted, Data: data}})
	if out := thread(); !strings.Contains(out, "Default travaille…") {
		t.Errorf("from the hub: %s", out)
	}
}

// A member's tree stream rings when one of their sessions changes, here a
// turn a worker reports; another user's does not.
func TestUI_TheTreeStreamRings(t *testing.T) {
	h, st, hub := newRouteTestHub(t, &fakeTemporal{})
	st.session.SessionID = liveSID
	srv := httptest.NewServer(h)
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	open := func(email string) *bufio.Scanner {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/tree/stream?last_event_id="+hub.Position(), nil)
		req.AddCookie(logIn(t, h, email))
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("stream: %v %v", err, resp)
		}
		t.Cleanup(func() { resp.Body.Close() })
		return bufio.NewScanner(resp.Body)
	}
	from := hub.Position()
	bob, carol := open("bob@example.com"), open("carol@example.com")

	data, _ := json.Marshal(workflow.TurnEvent{AgentID: "default", Turn: "k"})
	hub.Publish(liveSID, activity.SSEEvent{Type: workflow.EventTurnStarted, Data: data})
	if got := nextEvent(t, bob); !strings.HasSuffix(got, " "+session.EventTreeChanged) {
		t.Errorf("bob's tree: %q", got)
	}
	// A session page has one stream: the session's events, and the tree's.
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/s/"+liveSID+"/stream?last_event_id="+from, nil)
	req.AddCookie(logIn(t, h, "bob@example.com"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("session stream: %v %v", err, resp)
	}
	defer resp.Body.Close()
	page := bufio.NewScanner(resp.Body)
	// The tree rings in the background: before the session's event or after.
	got := []string{nextEvent(t, page), nextEvent(t, page)}
	for i, e := range got {
		got[i] = e[strings.LastIndex(e, " ")+1:]
	}
	slices.Sort(got)
	if want := []string{session.EventTreeChanged, workflow.EventTurnStarted}; !slices.Equal(got, want) {
		t.Errorf("session page stream: %q, want %q", got, want)
	}

	// Carol is no member of the session: the next thing her stream says is
	// what came to her tree after, not the session's change.
	hub.Publish(session.TreeTopic("u-carol"), activity.SSEEvent{Type: "marker", Data: []byte(`{}`)})
	if got := nextEvent(t, carol); !strings.HasSuffix(got, " marker") {
		t.Errorf("carol's tree: %q", got)
	}
}

// A member who leaves hears no more of the session: their streams of it end
// at once, the page's and the JSON one, with a last session_gone; the other
// members' go on, and are told. A page that reconnects after is told it is
// gone, where the JSON API answers 404. Deleting the session ends its
// creator's stream too.
func TestStreams_EndForAMemberWhoLeaves(t *testing.T) {
	h, st, _ := newRouteTestHub(t, &fakeTemporal{})
	st.session.SessionID = liveSID
	srv := httptest.NewServer(h)
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	open := func(path string, cookie *http.Cookie) *bufio.Scanner {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+path, nil)
		req.AddCookie(cookie)
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("stream %s: %v %v", path, err, resp)
		}
		t.Cleanup(func() { resp.Body.Close() })
		return bufio.NewScanner(resp.Body)
	}
	bob, alice := logIn(t, h, "bob@example.com"), logIn(t, h, "alice@example.com")
	bobPage, bobJSON := open("/s/"+liveSID+"/stream", bob), open("/sessions/"+liveSID+"/stream", bob)
	alicePage := open("/s/"+liveSID+"/stream", alice)

	if w := call(t, h, http.MethodDelete, "/sessions/"+liveSID+"/members/u-bob", "", bob); w.Code != http.StatusNoContent {
		t.Fatalf("bob leaving: %d", w.Code)
	}
	for name, lines := range map[string]*bufio.Scanner{"page": bobPage, "JSON": bobJSON} {
		if got := restOf(t, lines); !slices.Equal(got, goneLines) {
			t.Errorf("bob's %s stream after he left: %q, want %q", name, got, goneLines)
		}
	}
	if got := nextEvent(t, alicePage); !strings.HasSuffix(got, " "+session.EventMemberLeft) {
		t.Errorf("alice's stream: %q", got)
	}

	// A session that does not exist reads like one bob is not in.
	if got := restOf(t, open("/s/"+uuid.NewString()+"/stream", bob)); !slices.Equal(got, goneLines) {
		t.Errorf("unknown session: %q, want %q", got, goneLines)
	}

	// Bob's page reconnects: it is told, and EventSource stops retrying.
	if got := restOf(t, open("/s/"+liveSID+"/stream", bob)); !slices.Equal(got, goneLines) {
		t.Errorf("bob's page reconnecting: %q, want %q", got, goneLines)
	}
	if w := call(t, h, http.MethodGet, "/sessions/"+liveSID+"/stream", "", bob); w.Code != http.StatusNotFound {
		t.Errorf("bob's JSON stream reconnecting: %d", w.Code)
	}

	if w := call(t, h, http.MethodDelete, "/sessions/"+liveSID, "", alice); w.Code != http.StatusNoContent {
		t.Fatalf("alice deleting: %d %s", w.Code, w.Body)
	}
	// Her tree rang when bob left: that event, then the end.
	got := restOf(t, alicePage)
	if len(got) < 2 || !slices.Equal(got[len(got)-2:], goneLines) || slices.Contains(got, "event: "+session.EventMemberLeft) {
		t.Errorf("alice's stream after the deletion: %q, want it to end with %q", got, goneLines)
	}
}

// A participant with MaxQueued messages waiting refuses one more: 429, and
// the message is not stored, so the member who sends it again sends it once.
func TestRoutes_QueueFull(t *testing.T) {
	h, st := newRouteTestWith(t, &fakeTemporal{})
	bob := logIn(t, h, "bob@example.com")
	for i := range session.MaxQueued {
		if w := call(t, h, http.MethodPost, "/sessions/s1/messages", fmt.Sprintf(`{"content":"@default %d"}`, i), bob); w.Code != http.StatusAccepted {
			t.Fatalf("message %d: %d %s", i, w.Code, w.Body)
		}
	}
	w := call(t, h, http.MethodPost, "/sessions/s1/messages", `{"content":"@default one more"}`, bob)
	if w.Code != http.StatusTooManyRequests || len(st.appended) != session.MaxQueued {
		t.Errorf("one more: %d %s, %d stored", w.Code, w.Body, len(st.appended))
	}
}

// telegramStore is a store whose Telegram chat 42 is Bob's, in session s1.
type telegramStore struct {
	*routeStore
}

func (s telegramStore) GetUserByTelegramID(context.Context, int64) (*store.User, error) {
	return &store.User{ID: "u-bob", Email: "bob@example.com"}, nil
}
func (s telegramStore) GetActiveSessionByChannel(context.Context, string, string, string) (*store.Session, error) {
	return &s.session, nil
}

// On Telegram, a message refused for a full queue is answered in text, in
// the webhook's response, and not stored.
func TestTelegram_QueueFullIsAnsweredInText(t *testing.T) {
	_, rs := newRouteTest(t)
	st := telegramStore{rs}
	ch := &telegramChannel{
		sessions: session.New(st, &fakeTemporal{}, sse.NewHub(), session.Config{WorkflowQueue: "agent", DefaultAgentID: "default"}),
		users:    st, secret: "s3cret",
	}
	send := func(text string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/webhooks/telegram", strings.NewReader(`{"update_id":1,"message":{"message_id":1,"chat":{"id":42},"text":"`+text+`"}}`))
		req.Header.Set(telegramSecretHeader, "s3cret")
		w := httptest.NewRecorder()
		ch.ServeHTTP(w, req)
		return w
	}
	for i := range session.MaxQueued {
		if w := send(fmt.Sprint("@default ", i)); w.Code != http.StatusOK || w.Body.Len() != 0 {
			t.Fatalf("message %d: %d %s", i, w.Code, w.Body)
		}
	}
	w := send("@default one more")
	var reply struct {
		Method string `json:"method"`
		ChatID int64  `json:"chat_id"`
		Text   string `json:"text"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &reply); err != nil || w.Code != http.StatusOK || reply.Method != "sendMessage" || reply.ChatID != 42 || reply.Text != queueFullText {
		t.Errorf("reply %d %s (%v)", w.Code, w.Body, err)
	}
	if len(rs.appended) != session.MaxQueued {
		t.Errorf("%d stored, want the refused one out", len(rs.appended))
	}
}

// The session list says nothing of a workflow of its own, which no session
// has any more.
func TestRoutes_ListHasNoActive(t *testing.T) {
	h, _ := newRouteTestWith(t, &fakeTemporal{})
	bob := logIn(t, h, "bob@example.com")
	if w := call(t, h, http.MethodGet, "/me/sessions", "", bob); w.Code != http.StatusOK || strings.Contains(w.Body.String(), `"active"`) || !strings.Contains(w.Body.String(), `"s1"`) {
		t.Errorf("sessions %d %s", w.Code, w.Body)
	}
}
