package chat

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/victor/temporal-agent/store"
)

var t0 = time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)

func sess(id, parent string, at int64, created time.Time) store.Session {
	return store.Session{SessionID: id, Title: id, ParentSessionID: parent, ForkedAtMessageID: at, CreatedAt: created}
}

func ids(nodes []*TreeNode) []string {
	var out []string
	for _, n := range nodes {
		out = append(out, n.Session.SessionID)
	}
	return out
}

func TestBuildTree(t *testing.T) {
	sessions := []store.Session{
		sess("old", "", 0, t0),
		sess("root", "", 0, t0.Add(time.Hour)),
		sess("b2", "root", 9, t0.Add(2*time.Hour)),
		sess("b1", "root", 3, t0.Add(3*time.Hour)), // forked earlier in the thread
		sess("leaf", "b1", 5, t0.Add(4*time.Hour)),
		sess("orphan", "hidden", 1, t0.Add(5*time.Hour)), // parent not visible
	}
	stats := map[string]store.SessionStats{
		"old":  {LastActivity: t0.Add(10 * time.Hour)}, // recently active
		"leaf": {LastActivity: t0.Add(11 * time.Hour)}, // makes its root the most recent
	}
	statuses := map[string]Status{"leaf": StatusWaiting}

	roots := BuildTree(sessions, stats, statuses, "leaf")
	if got := strings.Join(ids(roots), ","); got != "root,old,orphan" {
		t.Errorf("roots %s", got)
	}
	root := roots[0]
	// Children in the order of the messages they forked from.
	if got := strings.Join(ids(root.Children), ","); got != "b1,b2" {
		t.Errorf("children %s", got)
	}
	leaf := Find(roots, "leaf")
	if leaf == nil || leaf.Depth != 2 || !leaf.Current || leaf.Status != StatusWaiting || Root(leaf) != root {
		t.Errorf("leaf %+v", leaf)
	}
	if got := strings.Join(ids(Path(leaf)), ","); got != "root,b1,leaf" {
		t.Errorf("path %s", got)
	}
	if o := Find(roots, "orphan"); !o.Orphan || o.Depth != 0 {
		t.Errorf("orphan %+v", o)
	}
	if Find(roots, "old").Status != StatusIdle {
		t.Error("no status means idle")
	}
	if Count(root) != 4 {
		t.Errorf("count %d", Count(root))
	}
}

func j(s string) string { b, _ := json.Marshal(s); return string(b) }

func TestBuildThread(t *testing.T) {
	msgs := []store.MessageWithID{
		{ID: 1, Message: store.Message{Role: store.RoleUser, Kind: store.KindForkSummary, Content: j("**brief**")}},
		{ID: 2, Message: store.Message{Role: store.RoleUser, Content: j("hello"), UserID: "u-me", Author: "Victor F"}},
		{ID: 3, Message: store.Message{Role: store.RoleUser, Content: j("@agent go"), UserID: "u-bob", Author: "Bob"}},
		// One agent turn over four messages.
		{ID: 4, Message: store.Message{Role: store.RoleAssistant, Content: j("Let me look."), ToolCalls: []store.ToolCall{{ID: "t1", Name: "read_file"}}}},
		{ID: 5, Message: store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t1", Content: "data"}}},
		{ID: 6, Message: store.Message{Role: store.RoleAssistant, ToolCalls: []store.ToolCall{{ID: "t2", Name: "read_file"}, {ID: "t3", Name: "grep"}}}},
		{ID: 7, Message: store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t2"}}},
		{ID: 8, Message: store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t3"}}},
		{ID: 9, Message: store.Message{Role: store.RoleAssistant, Content: j("Done.")}},
		{ID: 10, Message: store.Message{Role: store.RoleAssistant, Kind: store.KindTurnError, Content: j("call LLM: boom")}},
	}
	forks := map[int64][]ForkLink{9: {{SessionID: "f1", Title: "Fork"}}}
	questions := []Question{{WorkflowID: "s1-tool-ask_user-1-0", Text: "Which one?"}}

	jarvis := AgentInfo{ID: "default", Name: "Jarvis", Mention: "jarvis"}
	items := BuildThread(msgs, "u-me", forks, questions, AgentDirectory{Session: jarvis})
	var kinds []string
	for _, it := range items {
		kinds = append(kinds, it.Kind)
	}
	if got := strings.Join(kinds, ","); got != "brief,human,human,agent,error,question" {
		t.Fatalf("kinds %s", got)
	}
	if !strings.Contains(string(items[0].HTML), "<strong>brief</strong>") {
		t.Errorf("brief %s", items[0].HTML)
	}
	if !items[1].Mine || items[2].Mine || items[2].Author.Initials != "BO" || items[1].Author.Initials != "VF" {
		t.Errorf("humans %+v / %+v", items[1], items[2])
	}
	agent := items[3]
	// Its text, its tools once each in order, and its last message to fork from.
	if strings.Join(agent.Tools, ",") != "read_file,grep" || agent.ID != 9 ||
		!strings.Contains(string(agent.HTML), "Let me look.") || !strings.Contains(string(agent.HTML), "Done.") {
		t.Errorf("agent %+v", agent)
	}
	if agent.Agent != jarvis || items[4].Agent != jarvis {
		t.Errorf("signed %+v / %+v, want the session's agent for messages no agent signed", agent.Agent, items[4].Agent)
	}
	if len(agent.Forks) != 1 || agent.Forks[0].SessionID != "f1" {
		t.Errorf("forks %+v", agent.Forks)
	}
	// A failure is an item of its own, not more of the agent's answer.
	if items[4].Text != "call LLM: boom" || strings.Contains(string(agent.HTML), "boom") {
		t.Errorf("error %+v", items[4])
	}
	if items[5].WorkflowID != "s1-tool-ask_user-1-0" {
		t.Errorf("question %+v", items[5])
	}
}

// Several agents answer one after another: an item per agent, each signed by
// its agent as it is now, or by the name it had once it is gone.
func TestBuildThread_SignsEachAgent(t *testing.T) {
	jarvis := AgentInfo{ID: "default", Name: "Jarvis", Mention: "jarvis"}
	smith := AgentInfo{ID: "smith", Name: "Agent Smith", Mention: "agentSmith"}
	msgs := []store.MessageWithID{
		{ID: 1, Message: store.Message{Role: store.RoleUser, Content: j("@jarvis résume, @agentSmith juge"), UserID: "u-me", Author: "Victor"}},
		{ID: 2, Message: store.Message{Role: store.RoleAssistant, AgentID: "default", Author: "Old Jarvis", ToolCalls: []store.ToolCall{{ID: "t1", Name: "web_search"}}}},
		{ID: 3, Message: store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t1"}}},
		{ID: 4, Message: store.Message{Role: store.RoleAssistant, Content: j("Résumé."), AgentID: "default", Author: "Old Jarvis"}},
		{ID: 5, Message: store.Message{Role: store.RoleAssistant, Content: j("Utile."), AgentID: "smith", Author: "Smith"}},
		{ID: 6, Message: store.Message{Role: store.RoleAssistant, Content: j("Moi aussi."), AgentID: "gone", Author: "Ancien"}},
		{ID: 7, Message: store.Message{Role: store.RoleAssistant, Content: j("Sans nom."), AgentID: "nameless"}},
		{ID: 8, Message: store.Message{Role: store.RoleAssistant, Kind: store.KindTurnError, AgentID: "smith", Content: j("boom")}},
	}
	items := BuildThread(msgs, "u-me", nil, nil, AgentDirectory{ByID: map[string]AgentInfo{"default": jarvis, "smith": smith}, Session: jarvis})

	want := []struct {
		kind string
		id   int64
		who  AgentInfo
	}{
		{ItemHuman, 1, AgentInfo{}},
		{ItemAgent, 4, jarvis}, // its current name, not the one stored
		{ItemAgent, 5, smith},
		{ItemAgent, 6, AgentInfo{ID: "gone", Name: "Ancien"}},
		{ItemAgent, 7, AgentInfo{ID: "nameless", Name: "nameless"}},
		{ItemError, 8, smith},
	}
	if len(items) != len(want) {
		t.Fatalf("%d items, want %d: %+v", len(items), len(want), items)
	}
	for i, w := range want {
		if it := items[i]; it.Kind != w.kind || it.ID != w.id || it.Agent != w.who {
			t.Errorf("item %d: %s #%d by %+v, want %s #%d by %+v", i, it.Kind, it.ID, it.Agent, w.kind, w.id, w.who)
		}
	}
	if strings.Join(items[1].Tools, ",") != "web_search" || strings.Contains(string(items[2].HTML), "Résumé") {
		t.Errorf("jarvis's item %+v, smith's %s: each keeps its own", items[1], items[2].HTML)
	}
}

// The agent's text comes from a model: HTML in it must not reach the page.
// A fork's report is an item of its own: its sender, its Markdown rendered
// and escaped, its fork linked only for a viewer who is a member of it.
func TestBuildThread_ForkReport(t *testing.T) {
	report := func(id int64, fork string) store.MessageWithID {
		return store.MessageWithID{ID: id, CreatedAt: t0, Message: store.Message{Role: store.RoleUser, Kind: store.KindForkReport,
			Content: j("## Fait\n<script>x</script>"), UserID: "u2", Author: "Victor",
			Fork: &store.ForkRef{SessionID: fork, Title: "Export CSV", UpToMessageID: 9}}}
	}
	items := BuildThread([]store.MessageWithID{report(4, "f1"), report(5, "gone")}, "u1",
		map[int64][]ForkLink{2: {{SessionID: "f1", Title: "Export CSV"}}}, nil, AgentDirectory{})
	if len(items) != 2 {
		t.Fatalf("%d items", len(items))
	}
	it := items[0]
	if it.Kind != ItemReport || it.ID != 4 || it.Author.Name != "Victor" || it.Mine ||
		it.Report != (ReportLink{SessionID: "f1", Title: "Export CSV", Accessible: true}) {
		t.Errorf("item %+v", it)
	}
	if !strings.Contains(string(it.HTML), "<h2>Fait</h2>") || strings.Contains(string(it.HTML), "<script>") {
		t.Errorf("html %s", it.HTML)
	}
	// A fork the viewer is not a member of, or deleted: named, not linked.
	if items[1].Report.Accessible || items[1].Report.Title != "Export CSV" {
		t.Errorf("unreachable fork %+v", items[1].Report)
	}
}

// The mark of the latest report goes after the last item it covers, before
// the questions waiting; the parent is linked only for its members.
func TestMarkReported(t *testing.T) {
	items := []ThreadItem{{Kind: ItemBrief, ID: 1}, {Kind: ItemHuman, ID: 2}, {Kind: ItemAgent, ID: 5}, {Kind: ItemHuman, ID: 8}, {Kind: ItemQuestion}}
	at := t0
	fork := store.Session{SessionID: "f", ParentSessionID: "p", LastReportedMessageID: 6, LastReportID: 40, LastReportedAt: &at}
	got := MarkReported(append([]ThreadItem(nil), items...), fork, true)
	if len(got) != 6 || got[3].Kind != ItemReported || got[4].ID != 8 ||
		got[3].Report != (ReportLink{SessionID: "p", MessageID: 40, Accessible: true}) || !got[3].Time.Equal(t0) {
		t.Errorf("marked %+v", got)
	}
	if got := MarkReported(items, fork, false); got[3].Report.Accessible {
		t.Error("the parent linked for a non-member")
	}
	fork.LastReportedMessageID = 99 // past the thread: before the question
	if got := MarkReported(items, fork, true); got[4].Kind != ItemReported || got[5].Kind != ItemQuestion {
		t.Errorf("marked %+v", got)
	}
	if got := MarkReported(items, store.Session{SessionID: "f"}, true); len(got) != len(items) {
		t.Error("a fork that never reported is marked")
	}
	// The items handed in are left as they were.
	if items[3].Kind != ItemHuman {
		t.Error("MarkReported changed its input")
	}
}

func TestMarkdownIsSafe(t *testing.T) {
	out := string(Markdown("hi <script>alert(1)</script> [x](javascript:alert(1)) **bold**"))
	if strings.Contains(out, "<script>") || strings.Contains(out, "javascript:") {
		t.Errorf("unsafe output %s", out)
	}
	if !strings.Contains(out, "<strong>bold</strong>") {
		t.Errorf("markdown not rendered %s", out)
	}
}

func TestBuildMap(t *testing.T) {
	roots := BuildTree([]store.Session{
		sess("root", "", 0, t0), sess("a", "root", 1, t0), sess("b", "root", 2, t0), sess("a1", "a", 3, t0),
	}, nil, nil, "")
	m := BuildMap(roots[0])
	pos := map[string]MapNode{}
	for _, n := range m.Nodes {
		pos[n.Session.SessionID] = n
	}
	// Two leaves (a1, b) side by side; a over a1; root centred over a and b.
	if pos["a1"].X >= pos["b"].X || pos["a"].X != pos["a1"].X || pos["root"].X != (pos["a"].X+pos["b"].X)/2 {
		t.Errorf("x: %+v", pos)
	}
	if pos["root"].Y >= pos["a"].Y || pos["a"].Y >= pos["a1"].Y || pos["a"].Y != pos["b"].Y {
		t.Errorf("y: %+v", pos)
	}
	if len(m.Edges) != 3 || m.Nodes[0].Session.SessionID != "root" {
		t.Errorf("edges %d, first node %s", len(m.Edges), m.Nodes[0].Session.SessionID)
	}
	if m.Width < pos["b"].X+MapNodeW || m.Height < pos["a1"].Y+MapNodeH {
		t.Errorf("size %dx%d too small", m.Width, m.Height)
	}
}

func TestInitials(t *testing.T) {
	for name, want := range map[string]string{"Victor Freches": "VF", "bob@example.com": "BO", "Élodie": "ÉL", "": "?", "jean-paul sartre": "JP"} {
		if got := initials(name); got != want {
			t.Errorf("initials(%q) = %q, want %q", name, got, want)
		}
	}
}
