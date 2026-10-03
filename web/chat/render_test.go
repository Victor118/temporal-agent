package chat

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/victor/temporal-agent/store"
)

var smith = AgentInfo{ID: "smith", Name: "Agent Smith", Mention: "agentSmith"}

func testPage(view string) *Page {
	roots := BuildTree([]store.Session{
		sess("root", "", 0, t0), sess("fork", "root", 3, t0),
	}, map[string]store.SessionStats{"fork": {Members: 2, Messages: 5}}, map[string]Status{"fork": StatusWaiting}, "fork")
	node := Find(roots, "fork")
	p := &Page{
		Me: NewPerson("u1", "Victor F"), IsAdmin: true, Roots: roots, Node: node, Crumbs: Path(node), View: view,
		Members:        []Member{{Person: NewPerson("u1", "Victor F"), Email: "v@x.fr"}, {Person: NewPerson("u2", "Bob"), Email: "b@x.fr"}},
		Agent:          AgentInfo{ID: "default", Name: "Default Agent", Mention: "jarvis", Description: "General."},
		Agents:         []AgentInfo{{ID: "default", Name: "Default Agent", Mention: "jarvis"}, smith},
		AgentMode:      "auto",
		AgentOnMention: true,
		Parent:         &ParentInfo{SessionID: "root", Title: "root", Accessible: true, MessageID: 3},
		IsCreator:      true,
		Notifications:  2,
		Working:        true,
		LastMessageID:  9,
		Error:          "boom",
	}
	p.Thread = BuildThread([]store.MessageWithID{
		{ID: 1, Message: store.Message{Role: store.RoleUser, Kind: store.KindForkSummary, Content: j("brief")}},
		{ID: 2, Message: store.Message{Role: store.RoleUser, Content: j("@agent <b>hi</b>"), UserID: "u2", Author: "Bob"}},
		{ID: 9, Message: store.Message{Role: store.RoleAssistant, Content: j("**ok**"), ToolCalls: []store.ToolCall{{Name: "grep"}}}},
		{ID: 10, Message: store.Message{Role: store.RoleAssistant, Content: j("useful"), AgentID: "smith", Author: "Smith"}},
	}, "u1", map[int64][]ForkLink{9: {{SessionID: "f2", Title: "F2"}}}, []Question{{WorkflowID: "fork-tool-ask_user-1", Text: "Which?", AgentChain: []string{"default"}}},
		AgentDirectory{ByID: map[string]AgentInfo{"smith": smith}, Session: p.Agent})
	if view == "map" {
		m := BuildMap(Root(node))
		p.Map = &m
	}
	return p
}

func render(t *testing.T, name string, data any) string {
	t.Helper()
	w := httptest.NewRecorder()
	Render(w, name, data)
	if w.Code != 200 {
		t.Fatalf("%s: %d %s", name, w.Code, w.Body)
	}
	return w.Body.String()
}

func TestRender_Pages(t *testing.T) {
	page := render(t, "page", testPage("thread"))
	for _, want := range []string{
		`sse-connect="/sessions/fork/stream"`, // live updates wired
		`hx-post="/s/fork/messages"`,          // composer
		`<span class="mention">@agent</span>`, // mention highlighted
		`&lt;b&gt;hi&lt;/b&gt;`,               // a member's HTML escaped
		`<strong>ok</strong>`,                 // the agent's Markdown rendered
		`<span class="tool">grep</span>`,
		`name="message_id" value="9"`, // fork from a message
		`href="/s/f2"`,                // a fork of a message
		`name="workflow_id" value="fork-tool-ask_user-1"`,
		`href="/s/root#m3"`, // back to the message forked from
		"L'agent travaille",
		"@jarvis pour le solliciter, la mention d'un autre agent pour l'appeler", // other agents can be called
		"les autres agents à leur mention",
		"Plusieurs dans un message répondent l'un après l'autre",
		`class="dot waiting"`,
		"Configuration",
		`<span class="who">Default Agent</span><span class="agent-meta">@jarvis</span>`, // each answer signed by its agent
		`<span class="who">Agent Smith</span><span class="agent-meta">@agentSmith</span>`,
		`<code title="Agent Smith">@agentSmith</code>`, // the agents to call, in the rail
	} {
		if !strings.Contains(page, want) {
			t.Errorf("page lacks %q", want)
		}
	}
	if strings.Contains(page, "<b>hi</b>") {
		t.Error("a member's HTML reached the page")
	}

	// One agent to call: nothing about the others.
	for _, onMention := range []bool{true, false} {
		alone := testPage("thread")
		alone.Agents, alone.AgentOnMention = alone.Agents[:1], onMention
		out := render(t, "page", alone)
		for _, unwanted := range []string{"autre agent", "autres agents", "Plusieurs dans un message"} {
			if strings.Contains(out, unwanted) {
				t.Errorf("with one agent, the page says %q", unwanted)
			}
		}
	}

	mp := render(t, "page", testPage("map"))
	if !strings.Contains(mp, `class="mnode waiting current"`) || !strings.Contains(mp, "<path d=\"M") {
		t.Errorf("map page: %s", mp)
	}

	welcome := testPage("thread")
	welcome.Node, welcome.Crumbs = nil, nil
	if !strings.Contains(render(t, "page", welcome), "Nouvelle session") {
		t.Error("welcome page")
	}

	frag := testPage("thread")
	frag.Fragment = true
	if out := render(t, "thread", frag); !strings.Contains(out, `id="composer" hx-swap-oob="true"`) {
		t.Error("the thread fragment must bring the composer along, out of band")
	}
	if strings.Contains(page, `hx-swap-oob`) {
		t.Error("the full page must not mark its composer out of band")
	}

	for _, frag := range []string{"thread", "rail", "tree-items"} {
		if out := render(t, frag, testPage("thread")); strings.Contains(out, "<html") {
			t.Errorf("%s fragment carries a whole page", frag)
		}
	}
	if !strings.Contains(render(t, "login", LoginPage{Email: "a@b.c", Error: "nope"}), `name="password"`) {
		t.Error("login page")
	}
	render(t, "notifications", NotificationsPage{Items: []Notification{{ID: 1, HTML: Markdown("done")}}})
}
