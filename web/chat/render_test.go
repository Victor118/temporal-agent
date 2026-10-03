package chat

import (
	"net/http/httptest"
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
	p := testPage("thread")
	p.StreamFrom = "e-7"
	page := render(t, "page", p)
	for _, want := range []string{
		`sse-connect="/sessions/fork/stream?last_event_id=e-7"`, // live updates wired, from where the page stands
		`hx-post="/s/fork/messages"`,                            // composer
		`<span class="mention">@agent</span>`,                   // mention highlighted
		`&lt;b&gt;hi&lt;/b&gt;`,                                 // a member's HTML escaped
		`<strong>ok</strong>`,                                   // the agent's Markdown rendered
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
	// Reloads morph the panes in place; nothing is swapped whole.
	for _, want := range []string{
		`<script src="/static/idiomorph-ext-0.8.0.min.js"></script>`,
		`<body hx-boost="true" hx-ext="morph">`,
		`<div id="thread" hx-get="/s/fork/thread" hx-swap="morph:innerHTML"`,
		`hx-post="/s/fork/messages" hx-target="#thread" hx-swap="morph:innerHTML"`,
		`hx-swap="morph:innerHTML">`, // the tree
		`hx-target="#rail" hx-swap="morph"`,
		`id="composer-text"`, // focus comes back to it after a morph
		`id="q-fork-tool-ask_user-1"`,
	} {
		if !strings.Contains(page, want) {
			t.Errorf("page lacks %q", want)
		}
	}
	if strings.Contains(page, "outerHTML") || strings.Contains(page, "hx-preserve") {
		t.Error("a pane is still swapped whole")
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
	if out := render(t, "thread", frag); !strings.Contains(out, `id="composer" hx-swap-oob="morph"`) {
		t.Error("the thread fragment must bring the composer along, out of band, morphed")
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
	out := render(t, "thread", p)
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
	if out := render(t, "thread", p); !strings.Contains(out, `<span class="note">Fork inaccessible</span>`) || strings.Contains(out, `href="/s/f2"`) {
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
			[]string{`hx-get="/s/fork/report" hx-trigger="every 3s"`, "disabled", "Rapport en cours…", "Le rapport s&#39;écrit"}, nil},
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
			if !strings.Contains(render(t, "rail", p), `id="report"`) {
				t.Error("the rail lacks the report section")
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
	out := render(t, "thread", p)
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
	for _, name := range []string{"thread", "tree-items", "report"} {
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

	// Another content, another version, and the fragment comes.
	frag := testPage("thread")
	frag.Fragment, frag.Working = true, false
	w := httptest.NewRecorder()
	RenderFragment(w, "thread", frag, p.Versions["thread"])
	if w.Code != 200 || !strings.Contains(w.Body.String(), `data-version="`+frag.Versions["thread"]+`"`) || frag.Versions["thread"] == p.Versions["thread"] {
		t.Errorf("a changed thread: %d", w.Code)
	}
}
