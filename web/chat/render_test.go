package chat

import (
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/victor/temporal-agent/session"
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
		Working:        []WorkingAgent{{Name: "Jarvis"}},
		Participants: AgentsPanel{CanStop: true, Rows: []AgentRow{
			{Participant: "default", Agent: AgentInfo{ID: "default", Name: "Default Agent", Mention: "jarvis"}, Working: true, Turn: "m2.default", Author: "Bob", CanStop: true, CanClear: true, Queued: 1},
			{Participant: "smith", Agent: smith},
		}},
		LastMessageID: 9,
		Error:         "boom",
	}
	p.Thread = BuildThread([]store.MessageWithID{
		{ID: 1, Message: store.Message{Role: store.RoleUser, Kind: store.KindForkSummary, Content: j("brief")}},
		{ID: 2, Message: store.Message{Role: store.RoleUser, Content: j("@agent <b>hi</b>"), UserID: "u2", Author: "Bob"}},
		{ID: 9, Message: store.Message{Role: store.RoleAssistant, Content: j("**ok**"), ToolCalls: []store.ToolCall{{Name: "grep"}}}},
		{ID: 10, Message: store.Message{Role: store.RoleAssistant, Content: j("useful"), AgentID: "smith", Author: "Smith"}},
	}, "u1", map[int64][]ForkLink{9: {{SessionID: "f2", Title: "F2"}}}, []Question{{WorkflowID: "f1:p:default:m3:tool:ask_user:c1", Text: "Which?", AgentChain: []string{"default"}}},
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
	p := testPage("thread")
	p.StreamFrom = "e-7"
	page := render(t, "page", p)
	for _, want := range []string{
		`hx-post="/s/fork/messages"`,          // composer
		`<span class="mention">@agent</span>`, // mention highlighted
		`&lt;b&gt;hi&lt;/b&gt;`,               // a member's HTML escaped
		`<strong>ok</strong>`,                 // the agent's Markdown rendered
		`<span class="tool">grep</span>`,
		`name="message_id" value="9"`, // fork from a message
		`href="/s/f2"`,                // a fork of a message
		`name="workflow_id" value="f1:p:default:m3:tool:ask_user:c1"`,
		`href="/s/root#m3"`, // back to the message forked from
		"Jarvis travaille…",
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
	// Reloads morph the panes in place; nothing is swapped whole.
	for _, want := range []string{
		`<script src="/static/idiomorph-ext-0.8.0.min.js"></script>`,
		`<body hx-boost="true" hx-ext="morph, sse">`,
		`<div id="thread" hx-get="/s/fork/thread" hx-swap="morph:innerHTML"`,
		`hx-post="/s/fork/messages" hx-target="#thread" hx-swap="morph:innerHTML"`,
		`hx-swap="morph:innerHTML">`, // the tree
		`hx-target="#rail" hx-swap="morph"`,
		`id="composer-text"`, // focus comes back to it after a morph
		`id="q-f1:p:default:m3:tool:ask_user:c1"`,
	} {
		if !strings.Contains(page, want) {
			t.Errorf("page lacks %q", want)
		}
	}
	if strings.Contains(page, "outerHTML") || strings.Contains(page, "hx-preserve") {
		t.Error("a pane is still swapped whole")
	}
	// The streams ring the bell; the polls are a slow fallback.
	for _, want := range []string{
		`<div class="columns" sse-connect="/s/fork/stream?last_event_id=e-7" sse-close="session_gone">`, // one stream, from where the page stands
		`<aside class="col-tree">`,
		`hx-trigger="sse:changed, sse:reload, every 60s"`, // the tree
	} {
		if !strings.Contains(page, want) {
			t.Errorf("page lacks %q", want)
		}
	}
	for _, poll := range []string{"every 3s", "every 4s", "every 8s"} {
		if strings.Contains(page, poll) {
			t.Errorf("page still polls %s", poll)
		}
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
	if out := render(t, "page", welcome); !strings.Contains(out, "Nouvelle session") || !strings.Contains(out, `sse-connect="/tree/stream?last_event_id=`) {
		t.Error("welcome page, its tree's stream")
	}

	frag := testPage("thread")
	frag.Fragment = true
	fw := httptest.NewRecorder()
	RenderFragment(fw, "thread", frag, "")
	if out := fw.Body.String(); !strings.Contains(out, `id="thread-inner"`) || !strings.Contains(out, `id="composer" hx-swap-oob="morph"`) {
		t.Error("the thread fragment must bring the composer along, out of band, morphed")
	}
	if strings.Contains(page, `hx-swap-oob`) {
		t.Error("the full page must not mark its composer out of band")
	}

	for _, frag := range []string{"thread-inner", "rail", "tree-items"} {
		if out := render(t, frag, testPage("thread")); strings.Contains(out, "<html") {
			t.Errorf("%s fragment carries a whole page", frag)
		}
	}
	if !strings.Contains(render(t, "login", LoginPage{Email: "a@b.c", Error: "nope"}), `name="password"`) {
		t.Error("login page")
	}
	render(t, "notifications", NotificationsPage{Items: []Notification{{ID: 1, HTML: Markdown("done")}}})
}

// A session page leaves the session when its stream says it is no member
// of it: the stream closes, the page goes home, with no script of its own.
// Another member out, it reloads the members it shows. A page without a
// session has neither.
func TestRender_SessionPageReactsToMembership(t *testing.T) {
	for _, view := range []string{"thread", "map"} {
		page := render(t, "page", testPage(view))
		for _, want := range []string{
			`sse-connect="/s/fork/stream?last_event_id=" sse-close="session_gone">`,
			`<div hidden hx-get="/" hx-trigger="sse:session_gone" hx-target="body" hx-push-url="true"></div>`,
			`<div hidden hx-get="/s/fork" hx-trigger="sse:member_left" hx-select="#avatars" hx-target="#avatars" hx-swap="morph" hx-select-oob="#people:morph"></div>`,
			`<div class="avatars" id="avatars">`,
			`<div class="people" id="people">`,
		} {
			if !strings.Contains(page, want) {
				t.Errorf("%s page lacks %q", view, want)
			}
		}
	}
	welcome := testPage("thread")
	welcome.Node, welcome.Crumbs = nil, nil
	if out := render(t, "page", welcome); strings.Contains(out, "sse-close") || strings.Contains(out, "sse:session_gone") || strings.Contains(out, "sse:member_left") {
		t.Error("the welcome page reacts to a session's membership")
	}
	if session.EventSessionGone != "session_gone" || session.EventMemberLeft != "member_left" || slices.Contains(session.StateEvents, session.EventSessionGone) {
		t.Error("the page's events and the session's differ")
	}
}

// Forking asks what for, in a form rather than a confirmation; the fork shows
// its purpose, escaped, in its thread and its rail.
func TestRender_ForkPurpose(t *testing.T) {
	p := testPage("thread")
	page := render(t, "page", p)
	for _, want := range []string{
		`<summary class="btn" title="Nouvelle session à partir de ce message">⑂ Forker ici</summary>`,
		`id="fork-purpose-m9" name="purpose" maxlength="500"`,
		`Pour quoi faire ?`,
		// The composer's: its fields belong to a form outside the composer's.
		`name="purpose" form="fork-last"`,
		`<form id="fork-last" hx-post="/s/fork/fork" hidden><input type="hidden" name="message_id" value="9"></form>`,
	} {
		if !strings.Contains(page, want) {
			t.Errorf("page lacks %q", want)
		}
	}
	if strings.Contains(page, "hx-confirm=\"Démarrer") {
		t.Error("forking still asks a bare confirmation")
	}
	if strings.Contains(page, "But du fork") {
		t.Error("a fork without a purpose shows one")
	}

	p.Node.Session.ForkPurpose = "Écrire <l'export> CSV"
	page = render(t, "page", p)
	if strings.Count(page, "Écrire &lt;l&#39;export&gt; CSV") != 2 || strings.Contains(page, "<l'export>") {
		t.Errorf("the purpose, in the thread and the rail, escaped: %d", strings.Count(page, "Écrire &lt;l&#39;export&gt; CSV"))
	}

	// While the brief is written, nothing to fork from.
	p.SummaryPending = true
	if out := render(t, "page", p); strings.Contains(out, `name="purpose"`) {
		t.Error("fork forms while the summary is pending")
	}
}

// In the parent, a report reads as one: who sent it, from which fork, linked.
func TestRender_ForkReport(t *testing.T) {
	p := testPage("thread")
	p.Thread = BuildThread([]store.MessageWithID{
		{ID: 12, Message: store.Message{Role: store.RoleUser, Kind: store.KindForkReport, Content: j("**fait** <b>x</b>"), UserID: "u2", Author: "Bob",
			Fork: &store.ForkRef{SessionID: "f2", Title: "Export <CSV>", UpToMessageID: 30}}},
	}, "u1", map[int64][]ForkLink{9: {{SessionID: "f2", Title: "Export"}}}, nil, AgentDirectory{Session: p.Agent})
	out := render(t, "thread-inner", p)
	for _, want := range []string{
		`id="m12"`,
		`⑂ Rapport du fork « Export &lt;CSV&gt; »</span><span class="agent-meta">— par Bob</span>`,
		`<strong>fait</strong>`,
		`<a href="/s/f2">Ouvrir le fork</a>`,
		`name="message_id" value="12"`, // one can fork from a report
	} {
		if !strings.Contains(out, want) {
			t.Errorf("thread lacks %q", want)
		}
	}
	if strings.Contains(out, "<b>x</b>") {
		t.Error("the report's HTML reached the page")
	}

	// A fork the viewer cannot open: deleted, or not theirs, which the
	// thread does not tell apart.
	p.Thread = BuildThread([]store.MessageWithID{
		{ID: 12, Message: store.Message{Role: store.RoleUser, Kind: store.KindForkReport, Content: j("fait"), UserID: "u2", Author: "Bob",
			Fork: &store.ForkRef{SessionID: "f2", Title: "Export", UpToMessageID: 30}}},
	}, "u1", nil, nil, AgentDirectory{Session: p.Agent})
	if out := render(t, "thread-inner", p); !strings.Contains(out, `<span class="note">Fork inaccessible</span>`) || strings.Contains(out, `href="/s/f2"`) {
		t.Error("a report from a fork the viewer cannot open")
	}
}

// The rail's report section, by state: the button, why it is disabled, the
// polling while a report is written, a failure, the latest report.
func TestRender_ReportButton(t *testing.T) {
	at := time.Date(2026, 10, 1, 14, 2, 0, 0, time.Local)
	for name, c := range map[string]struct {
		state  session.ReportState
		want   []string
		unwant []string
	}{
		"ready": {session.ReportState{ParentSessionID: "root"},
			[]string{`hx-post="/s/fork/report" hx-target="#report"`, `⑂ Rapporter au parent</button>`,
				`hx-confirm="Poster dans « root » (3 membres) un résumé de ce fork, signé de ton nom ?"`,
				`<div class="note" style="margin-top:6px">Résume ce qui s'est fait ici depuis le dernier rapport`},
			[]string{"disabled", "every 3s", "title=", "L&#39;agent travaille"}},
		// The agent on a turn: a report may go, and says it stops there.
		"agent working": {session.ReportState{ParentSessionID: "root", AgentWorking: true},
			[]string{`⑂ Rapporter au parent</button>`,
				`hx-confirm="L&#39;agent est en plein tour (il travaille ou attend une réponse) : le rapport couvrira ce qui est écrit jusqu&#39;ici, la suite ira dans le rapport suivant. Poster dans « root » (3 membres)`,
				`<div class="note" style="margin-top:6px">L&#39;agent est en plein tour (il travaille ou attend une réponse) : le rapport couvrira ce qui est écrit jusqu&#39;ici, la suite ira dans le rapport suivant.</div>`},
			[]string{"disabled", "en entier", "attend la fin"}},
		// Nothing to report while the agent works: the reason, not the
		// partial report's note.
		"agent working, nothing new": {session.ReportState{ParentSessionID: "root", AgentWorking: true, NothingNew: true},
			[]string{"disabled", "Rien à rapporter pour l&#39;instant."}, []string{"la suite ira dans le rapport suivant.</div>"}},
		"not a fork": {session.ReportState{Refused: session.ErrNotAFork},
			[]string{"Cette session n&#39;est pas un fork"}, []string{"<button"}},
		"pending": {session.ReportState{ParentSessionID: "root", Pending: true},
			[]string{"disabled", "Rapport en cours…", "Le rapport s&#39;écrit"}, []string{"every 3s"}},
		"failed": {session.ReportState{ParentSessionID: "root", Failed: true},
			[]string{"Le dernier rapport n'a pas pu être écrit. Tu peux réessayer.", "⑂ Rapporter au parent"}, []string{"disabled"}},
		"brief pending": {session.ReportState{ParentSessionID: "root", SummaryPending: true},
			[]string{"disabled", "Le brief du fork est en cours d&#39;écriture."}, nil},
		"nothing new": {session.ReportState{ParentSessionID: "root", NothingNew: true, LastReportID: 31, LastReportedAt: &at},
			[]string{"disabled", "Rien de nouveau depuis le dernier rapport.", `Dernier rapport : <a href="/s/root#m31">` + clock(at) + `</a>`}, nil},
		"not a member of the parent": {session.ReportState{ParentSessionID: "root", Refused: session.ErrNotParentMember, LastReportID: 31, LastReportedAt: &at},
			[]string{"Seul un membre de la session parente peut y rapporter"}, []string{"<button", "Dernier rapport"}},
		"parent deleted": {session.ReportState{Refused: session.ErrNoParent},
			[]string{"La session parente a été supprimée"}, []string{"<button"}},
	} {
		t.Run(name, func(t *testing.T) {
			p := testPage("thread")
			p.Parent.Members = 3
			p.Report = &ReportView{ReportState: c.state}
			out := render(t, "report", p)
			for _, want := range c.want {
				if !strings.Contains(out, want) {
					t.Errorf("section lacks %q:\n%s", want, out)
				}
			}
			for _, unwant := range c.unwant {
				if strings.Contains(out, unwant) {
					t.Errorf("section has %q:\n%s", unwant, out)
				}
			}
			if rail := render(t, "rail", p); !strings.Contains(rail, `<section id="report" hx-get="/s/fork/report" hx-swap="morph:innerHTML"`) ||
				!strings.Contains(rail, `hx-trigger="`+mustReloadOn(t, "report")+`"`) {
				t.Errorf("the rail lacks the report section, reloaded on the fork's events: %s", rail)
			}
		})
	}

	// Not a fork: no section.
	p := testPage("thread")
	if out := render(t, "rail", p); strings.Contains(out, `id="report"`) {
		t.Error("a report section outside a fork")
	}

	// The question names the parent as the viewer knows it, and counts.
	p.Parent = &ParentInfo{Accessible: true, Members: 1}
	if got := p.ReportConfirm(); got != "Poster dans « Session sans titre » (1 membre) un résumé de ce fork, signé de ton nom ?" {
		t.Errorf("confirm %q", got)
	}
}

// In the fork, where the latest report stopped, linked to it in the parent;
// and a failed report, said in the thread.
func TestRender_ReportedMarkAndFailure(t *testing.T) {
	p := testPage("thread")
	at := time.Date(2026, 10, 1, 14, 2, 0, 0, time.Local)
	fork := store.Session{SessionID: "fork", ParentSessionID: "root", LastReportedMessageID: 2, LastReportID: 31, LastReportedAt: &at}
	p.Thread = MarkReported(p.Thread, fork, true)
	p.Report = &ReportView{ReportState: session.ReportState{ParentSessionID: "root", Failed: true}}
	out := render(t, "thread-inner", p)
	page := render(t, "page", p)
	mark := `⑂ Rapport envoyé à la session parente · ` + clock(at) + ` · <a href="/s/root#m31">le voir</a>`
	if !strings.Contains(out, mark) {
		t.Errorf("thread lacks the mark %q", mark)
	}
	// After Bob's message (2), before the answer (9).
	if i, j, k := strings.Index(out, `id="m2"`), strings.Index(out, mark), strings.Index(out, `id="m9"`); !(i < j && j < k) {
		t.Errorf("mark at %d, between %d and %d", j, i, k)
	}
	if !strings.Contains(out, "Le rapport à la session parente n'a pas pu être écrit.") {
		t.Error("the thread does not say the report failed")
	}
	for _, ev := range []string{"sse:fork_report,", "sse:fork_reported,", "sse:fork_report_failed,"} {
		if !strings.Contains(page, ev) {
			t.Errorf("the thread does not reload on %s", ev)
		}
	}
}

// A fragment carries its version, a hash of what it shows; the page holds
// the versions its reloads would get, and one the page holds is a 204.
func TestRender_FragmentVersions(t *testing.T) {
	p := testPage("thread")
	p.Report = &ReportView{ReportState: session.ReportState{ParentSessionID: "root"}}
	page := httptest.NewRecorder()
	RenderPage(page, "page", p)
	for _, name := range []string{"thread", "tree-items", "report", "agents"} {
		v := p.Versions[name]
		if len(v) != 24 || !strings.Contains(page.Body.String(), `data-version="`+v+`"`) {
			t.Errorf("%s: version %q not on the page", name, v)
		}
		frag := testPage("thread")
		frag.Report, frag.Fragment = p.Report, name == "thread"
		w := httptest.NewRecorder()
		RenderFragment(w, name, frag, v)
		if w.Code != 204 || w.Body.Len() != 0 {
			t.Errorf("%s held already: %d %s", name, w.Code, w.Body)
		}
	}
	if strings.Contains(page.Body.String(), versionPlaceholder) || p.Fragment {
		t.Error("the page kept the placeholder, or the fragment flag")
	}

	// The page shows each fragment as it was rendered for its version, once.
	for name, id := range map[string]string{"thread": `id="thread-inner"`, "tree-items": `class="tree-items"`, "report": `class="report-inner"`, "agents": `class="agents-inner"`} {
		got := string(p.Rendered[name])
		if got == "" || !strings.Contains(page.Body.String(), got) || strings.Count(page.Body.String(), id) != 1 {
			t.Errorf("%s: not shown once as rendered for its version", name)
		}
	}
	if strings.Contains(string(p.Rendered["thread"]), `id="composer"`) {
		t.Error("the page's thread brought the fragment's composer along")
	}

	// Another content, another version, and the fragment comes.
	frag := testPage("thread")
	frag.Fragment, frag.Working = true, nil
	w := httptest.NewRecorder()
	RenderFragment(w, "thread", frag, p.Versions["thread"])
	if w.Code != 200 || !strings.Contains(w.Body.String(), `data-version="`+frag.Versions["thread"]+`"`) || frag.Versions["thread"] == p.Versions["thread"] {
		t.Errorf("a changed thread: %d", w.Code)
	}
}

// The rail, swapped whole (after an invitation), holds the report section
// and the Agents panel as their reloads render them: their next reloads,
// unchanged, are 204s.
func TestRender_RailKeepsItsFragmentsVersions(t *testing.T) {
	p := testPage("thread")
	p.Report = &ReportView{ReportState: session.ReportState{ParentSessionID: "root"}}
	rail := httptest.NewRecorder()
	RenderFragment(rail, "rail", p, "")
	if rail.Code != 200 {
		t.Fatalf("rail: %d", rail.Code)
	}
	for _, name := range []string{"report", "agents"} {
		v := p.Versions[name]
		if v == "" || !strings.Contains(rail.Body.String(), `data-version="`+v+`"`) {
			t.Errorf("%s: version %q not in the rail", name, v)
			continue
		}
		frag := testPage("thread")
		frag.Report = p.Report
		w := httptest.NewRecorder()
		RenderFragment(w, name, frag, v)
		if w.Code != 204 {
			t.Errorf("%s reloaded after the rail: %d", name, w.Code)
		}
	}
}

// The working line names the agents at work, several at once, and what
// each waits for, escaped.
func TestRender_WorkingLineNamesTheAgents(t *testing.T) {
	p := testPage("thread")
	for _, c := range []struct {
		working []WorkingAgent
		want    string
	}{
		{[]WorkingAgent{{Name: "Agent <Smith>"}}, "Agent &lt;Smith&gt; travaille…"},
		{[]WorkingAgent{{}}, "L&#39;agent travaille…"},
		{[]WorkingAgent{{Name: "Jarvis", Note: "Ton run attend un <worker> libre"}}, "Jarvis travaille… — Ton run attend un &lt;worker&gt; libre"},
		{[]WorkingAgent{{Name: "Jarvis"}, {Name: "Smith", Note: "attend"}}, "Jarvis et Smith travaillent… — Smith : attend"},
		{[]WorkingAgent{{Name: "A"}, {Name: "B"}, {Name: "C"}}, "A, B et C travaillent…"},
	} {
		p.Working = c.working
		if out := render(t, "thread-inner", p); !strings.Contains(out, c.want) {
			t.Errorf("working %+v: want %q in %s", c.working, c.want, out)
		}
	}
	p.Working = nil
	if out := render(t, "thread-inner", p); strings.Contains(out, "travaille") || strings.Contains(out, `id="working"`) {
		t.Errorf("nobody works: %s", out)
	}
	p.Working = []WorkingAgent{{Name: "Jarvis"}}
	if page := render(t, "page", p); !strings.Contains(page, `hx-trigger="`+mustReloadOn(t, "thread")+`"`) {
		t.Error("the thread does not reload on its events")
	}
}

// The Agents panel: a row per participant, its stop naming the turn it
// shows, Tout arrêter asking first; no button the viewer may not use. The
// thread's Arrêter shows only to who may stop a turn.
func TestRender_AgentsPanel(t *testing.T) {
	p := testPage("thread")
	p.Participants.Error = "<refusé>"
	page := render(t, "page", p)
	for _, want := range []string{
		`id="agents" hx-get="/s/fork/agents"`,
		`hx-trigger="` + mustReloadOn(t, "agents") + `"`,
		`id="agent-default"`, `id="agent-smith"`,
		`hx-post="/s/fork/participants/default/stop"`, `name="turn" value="m2.default"`,
		`hx-post="/s/fork/participants/default/clear"`, `hx-confirm="Arrêter Default Agent et jeter 1 message de sa file, ceux des autres membres compris ?"`,
		"Répond à Bob", "1 message en file", "&lt;refusé&gt;",
		`hx-post="/s/fork/cancel"`,
	} {
		if !strings.Contains(page, want) {
			t.Errorf("page lacks %s", want)
		}
	}
	if strings.Contains(page, "participants/smith/") {
		t.Error("a button on an idle agent")
	}

	p.Participants.Rows[0].CanClear, p.Participants.Rows[0].CanStop, p.Participants.CanStop = false, false, false
	page = render(t, "page", p)
	if strings.Contains(page, "/participants/") || strings.Contains(page, "/cancel") {
		t.Error("buttons the viewer may not use")
	}
	if !strings.Contains(page, "travaille…") {
		t.Error("the working line went with the button")
	}
}

func mustReloadOn(t *testing.T, pane string) string {
	t.Helper()
	got, err := reloadOn(pane)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// A pane reloads on the events the session package lists for it, on a
// reload of the stream, and once a minute; the thread on every event that
// changes the session's state.
func TestReloadOn(t *testing.T) {
	thread := mustReloadOn(t, "thread")
	for _, ev := range append(slices.Clone(session.StateEvents), session.EventUserMessage, "message", "tool_calls", "turn_started", "turn_done", "ask_user", "notice", "reload") {
		if !strings.Contains(thread, "sse:"+ev+",") {
			t.Errorf("the thread does not reload on %s: %s", ev, thread)
		}
	}
	if !strings.HasSuffix(thread, "sse:reload, every 60s") {
		t.Errorf("thread: %s", thread)
	}
	if slices.Contains(session.StateEvents, "notice") {
		t.Error("a notice is a state event: it would drop the statuses and ring the trees")
	}
	if report := mustReloadOn(t, "report"); strings.Contains(report, "sse:tool_calls") || strings.Contains(report, "sse:notice") || !strings.Contains(report, "sse:fork_reported,") {
		t.Errorf("report: %s", report)
	}
	agents := mustReloadOn(t, "agents")
	for _, ev := range []string{"turn_started", "turn_done", "ask_user", session.EventUserMessage, "notice", "reload"} {
		if !strings.Contains(agents, "sse:"+ev+",") {
			t.Errorf("the Agents panel does not reload on %s: %s", ev, agents)
		}
	}
	if strings.Contains(agents, "sse:tool_calls") || strings.Contains(agents, "sse:message,") {
		t.Errorf("agents: %s", agents)
	}
	if tree := mustReloadOn(t, "tree"); tree != "sse:changed, sse:reload, every 60s" {
		t.Errorf("tree: %s", tree)
	}
	if _, err := reloadOn("rail"); err == nil {
		t.Error("a pane with no events")
	}
}

// The placeholder a fragment's version replaces is drawn by each process: a
// member cannot write it in a message.
func TestRender_PlaceholderIsNotGuessable(t *testing.T) {
	if len(versionPlaceholder) != 33 || versionPlaceholder == "fragment-version-placeholder" {
		t.Fatalf("placeholder %q", versionPlaceholder)
	}
	p := testPage("thread")
	p.Thread = BuildThread([]store.MessageWithID{
		{ID: 2, Message: store.Message{Role: store.RoleUser, Content: j("fragment-version-placeholder v0123"), UserID: "u2", Author: "Bob"}},
	}, "u1", nil, nil, AgentDirectory{Session: p.Agent})
	w := httptest.NewRecorder()
	RenderFragment(w, "thread", p, "")
	if !strings.Contains(w.Body.String(), "fragment-version-placeholder v0123") {
		t.Error("a member's text was taken for the placeholder")
	}
}

// A turn's files are links to download them, under its answer; the thread
// reloads when one is published.
func TestRender_Files(t *testing.T) {
	p := testPage("thread")
	turn := store.TurnKey(2, "default")
	p.Thread = BuildThread([]store.MessageWithID{
		{ID: 2, Key: store.HumanMessageKey("a"), Message: store.Message{Role: store.RoleUser, Content: j("go"), UserID: "u2", Author: "Bob"}},
		{ID: 9, Key: store.TurnMessageKey(turn, 0), Message: store.Message{Role: store.RoleAssistant, Content: j("Done.")}},
		{ID: 10, Key: store.TurnEndKey(turn), Message: store.TurnEnd("default", "")},
	}, "u1", nil, nil, AgentDirectory{Session: p.Agent})
	p.Thread = AttachFiles(p.Thread, []store.File{{ID: "f1", TurnKey: turn, Name: `<b>x</b>.html`, Size: 10}}, AgentDirectory{})
	out := render(t, "thread-inner", p)
	link := `href="/files/f1" download="&lt;b&gt;x&lt;/b&gt;.html"`
	if !strings.Contains(out, link) || !strings.Contains(out, "10 o") {
		t.Fatalf("thread lacks the file link: %s", out)
	}
	if i, j := strings.Index(out, `id="m9"`), strings.Index(out, link); !(i >= 0 && i < j) {
		t.Errorf("the file is not under the answer: %d, %d", i, j)
	}
	if strings.Contains(out, "<b>x</b>") {
		t.Error("the file name is not escaped")
	}
	if page := render(t, "page", p); !strings.Contains(page, "sse:file_published") {
		t.Error("the thread does not reload on file_published")
	}
}
