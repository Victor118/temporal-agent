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

	items := BuildThread(msgs, "u-me", forks, questions)
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

// The agent's text comes from a model: HTML in it must not reach the page.
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
