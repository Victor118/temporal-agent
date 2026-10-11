package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/victor/temporal-agent/access"
	"github.com/victor/temporal-agent/session"
	"github.com/victor/temporal-agent/sse"
	"github.com/victor/temporal-agent/store"
)

// agentTeam is the default agent and the commercial one, closed to everyone
// but Alice.
func agentTeam(st *routeStore) {
	st.session.AgentID = "default"
	st.agents = []store.Agent{{ID: "default", Name: "Jarvis", Mention: "jarvis"}, {ID: "commercial", Name: "Commercial", Mention: "commercial"}}
	st.rules = access.Snapshot{Rules: []access.Rule{{Kind: access.KindPattern, Target: "agent_commercial", Users: []string{"u-alice"}}}}
}

// The JSON API: a session or a fork with an agent closed to the member is
// refused (403); a message says which agents it addressed will not answer.
func TestRoutes_AgentsClosedToTheMember(t *testing.T) {
	h, st, _ := newForkTest(t)
	agentTeam(st)
	bob := logIn(t, h, "bob@example.com")

	if w := call(t, h, http.MethodPost, "/sessions", `{"agent_id":"commercial"}`, bob); w.Code != http.StatusForbidden || len(st.created) != 0 {
		t.Errorf("a session with a closed agent: %d %s", w.Code, w.Body)
	}
	if w := call(t, h, http.MethodPost, "/sessions", `{"agent_id":"commercial"}`, logIn(t, h, "alice@example.com")); w.Code != http.StatusCreated {
		t.Errorf("its holder's session: %d %s", w.Code, w.Body)
	}

	w := call(t, h, http.MethodPost, "/sessions/s1/messages", `{"content":"@commercial @jarvis ?"}`, bob)
	var resp struct {
		Called  bool     `json:"agent_called"`
		Refused []string `json:"refused_agents"`
	}
	if json.Unmarshal(w.Body.Bytes(), &resp); w.Code != http.StatusAccepted || !resp.Called || len(resp.Refused) != 1 || resp.Refused[0] != "commercial" {
		t.Errorf("message: %d %s", w.Code, w.Body)
	}

	st.session.AgentID = "commercial"
	if w := call(t, h, http.MethodPost, "/sessions/s1/fork", `{"message_id":2}`, bob); w.Code != http.StatusForbidden {
		t.Errorf("a fork of a closed agent: %d %s", w.Code, w.Body)
	}
}

// The interface lists the agents open to the member, says before a message
// that the session's agent is closed to them, and after one which agents
// will not answer it.
func TestUI_AgentsClosedToTheMember(t *testing.T) {
	h, st := newRouteTestWith(t, &fakeTemporal{})
	agentTeam(st)
	bob := logIn(t, h, "bob@example.com")
	alice := logIn(t, h, "alice@example.com")

	page := get(t, h, "/s/s1", "", bob).Body.String()
	if strings.Contains(page, "@commercial") || strings.Contains(page, "L'agent de cette session ne t'est pas ouvert") {
		t.Errorf("bob's page lists the closed agent, or says the session's agent is closed")
	}
	if page := get(t, h, "/s/s1", "", alice).Body.String(); !strings.Contains(page, "@commercial") {
		t.Errorf("alice's page does not list the agent open to her")
	}

	w := form(t, h, "/s/s1/messages", url.Values{"content": {"@commercial un devis ?"}}, bob)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `id="thread-notice">@commercial ne t&#39;est pas ouvert`) {
		t.Errorf("send: %d %s", w.Code, w.Body)
	}

	st.session.AgentID = "commercial"
	if page := get(t, h, "/s/s1", "", bob).Body.String(); !strings.Contains(page, "L'agent de cette session ne t'est pas ouvert") {
		t.Errorf("the composer does not say the session's agent is closed: %s", page)
	}
}

// On Telegram, the agents a message addressed that will not answer are
// said in text, in the webhook's response.
func TestTelegram_ClosedAgentsAnsweredInText(t *testing.T) {
	_, rs := newRouteTest(t)
	agentTeam(rs)
	st := telegramStore{rs}
	ch := &telegramChannel{
		sessions: session.New(st, &fakeTemporal{}, sse.NewHub(), session.Config{WorkflowQueue: "agent", DefaultAgentID: "default"}),
		users:    st, secret: "s3cret",
	}
	req := httptest.NewRequest(http.MethodPost, "/webhooks/telegram", strings.NewReader(`{"update_id":1,"message":{"message_id":1,"chat":{"id":42},"text":"@commercial ?"}}`))
	req.Header.Set(telegramSecretHeader, "s3cret")
	w := httptest.NewRecorder()
	ch.ServeHTTP(w, req)
	var reply struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &reply); err != nil || !strings.HasPrefix(reply.Text, "@commercial ne t'est pas ouvert") {
		t.Errorf("reply %d %s", w.Code, w.Body)
	}
	if len(rs.appended) != 1 {
		t.Errorf("%d stored, want the message kept", len(rs.appended))
	}
}
