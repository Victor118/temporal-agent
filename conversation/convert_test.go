package conversation

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/tool"
)

// stored numbers messages as the store would, each under its key: "" for a
// person's message, a turn key for a turn's (its index counted per turn).
type stored struct {
	msgs  []store.MessageWithID
	index map[string]int
}

func (s *stored) add(turn string, m store.Message) *stored {
	if s.index == nil {
		s.index = map[string]int{}
	}
	key := store.HumanMessageKey(string(rune('a' + len(s.msgs))))
	if turn != "" {
		key = store.TurnMessageKey(turn, s.index[turn])
		s.index[turn]++
	}
	s.msgs = append(s.msgs, store.MessageWithID{ID: int64(len(s.msgs) + 1), Key: key, Message: m})
	return s
}

// read is the conversation as the model of view.Self reads it.
func (s *stored) read(view View) []provider.ChatMessage {
	ordered := Order(s.msgs)
	msgs := make([]store.Message, len(ordered))
	for i, m := range ordered {
		msgs[i] = m.Message
	}
	return Convert(msgs, view)
}

// textOf is a converted message's text.
func textOf(m provider.ChatMessage) string {
	var s string
	json.Unmarshal(m.Content, &s)
	return s
}

// In a shared session the model must know who speaks: each user message
// reaches it prefixed with its author, while the stored message keeps the text
// and the author apart.
func TestConvert_NamesTheAuthor(t *testing.T) {
	msgs := Convert([]store.Message{
		{Role: store.RoleUser, Content: `"hello"`, UserID: "u-alice", Author: "Alice"},
		{Role: store.RoleAssistant, Content: `"hi Alice"`},
		{Role: store.RoleUser, Content: `"scheduled prompt"`}, // no author: a scheduled run
	}, View{Self: "default"})
	for i, want := range []string{`"[Alice] hello"`, `"hi Alice"`, `"scheduled prompt"`} {
		if got := string(msgs[i].Content); got != want {
			t.Errorf("message %d = %s, want %s", i, got, want)
		}
	}
}

// Several agents answer in a session: the model reads another agent's turn
// as text under its name and mention, in a user message, and its own as they
// are. An agent gone from the catalog keeps the name it signed with, or its
// ID; a message signed by no agent is read as the reader's own.
func TestConvert_NamesTheOtherAgents(t *testing.T) {
	view := View{Self: "smith", Agents: map[string]Label{
		"jarvis": {Name: "Jarvis", Mention: "jarvis"},
		"smith":  {Name: "Agent Smith", Mention: "smith"},
	}}
	msgs := Convert([]store.Message{
		{Role: store.RoleUser, Content: `"@jarvis résume, @smith juge"`, Author: "Alice"},
		{Role: store.RoleAssistant, Content: `"voici le résumé"`, AgentID: "jarvis", Author: "Jarvis"},
		{Role: store.RoleAssistant, Content: `"gone"`, AgentID: "old", Author: "Old One"},
		{Role: store.RoleAssistant, Content: `"nameless"`, AgentID: "older"},
		{Role: store.RoleAssistant, Content: `"mine"`, AgentID: "smith", Author: "Agent Smith"},
		{Role: store.RoleAssistant, Content: `"unsigned"`},
	}, view)

	want := []struct{ role, content string }{
		{"user", "[Alice] @jarvis résume, @smith juge\n\n[agent Jarvis (@jarvis)] voici le résumé\n\n[agent Old One] gone\n\n[agent older] nameless"},
		{"assistant", "mine"},
		{"assistant", "unsigned"},
	}
	if len(msgs) != len(want) {
		t.Fatalf("%d messages, want %d: %+v", len(msgs), len(want), msgs)
	}
	for i, w := range want {
		if msgs[i].Role != w.role || textOf(msgs[i]) != w.content {
			t.Errorf("message %d = %s %q, want %s %q", i, msgs[i].Role, textOf(msgs[i]), w.role, w.content)
		}
	}
}

// An agent without tools answers after one that used some: its request holds
// no tool block, or the API would reject it, and ends on a user message, or
// the model would continue the other agent's answer. A member's message
// written while the other agent worked comes after that agent's turn; a
// private input and its result stay hidden.
func TestConvert_OtherAgentsToolsAsText(t *testing.T) {
	view := View{
		Self:    "smith",
		Agents:  map[string]Label{"jarvis": {Name: "Jarvis", Mention: "jarvis"}},
		Private: tool.PrivateSet{"save_user_memory": true},
	}
	var s stored
	s.add("", store.Message{Role: store.RoleUser, Content: `"@jarvis cherche, @smith juge"`, Author: "Alice"}).
		add("r-1.0", store.Message{Role: store.RoleAssistant, Content: `"je cherche"`, AgentID: "jarvis", Author: "Jarvis", ToolCalls: []store.ToolCall{
			{ID: "t1", Name: "web_search", Input: json.RawMessage(`{"q":"temporal"}`)},
			{ID: "t2", Name: "save_user_memory", Input: json.RawMessage(`{"content":"Alice's secret"}`)},
		}}).
		add("r-1.0", store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t1", Content: "found " + strings.Repeat("x", 3000)}}).
		add("", store.Message{Role: store.RoleUser, Content: `"meanwhile"`, Author: "Bob"}).
		add("r-1.0", store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t2", Content: "Current version: Alice's other secret", IsError: true}}).
		add("r-1.0", store.Message{Role: store.RoleAssistant, Content: `"voici"`, AgentID: "jarvis", Author: "Jarvis"})
	msgs := s.read(view)

	if len(msgs) != 1 || msgs[0].Role != "user" {
		t.Fatalf("messages %+v, want one user message", msgs)
	}
	for _, m := range msgs {
		if len(m.ToolCalls) > 0 || m.ToolResult != nil {
			t.Errorf("a tool block reached an agent that did not make it: %+v", m)
		}
	}
	text := textOf(msgs[0])
	for _, want := range []string{
		"[Alice] @jarvis cherche, @smith juge\n\n[agent Jarvis (@jarvis)] je cherche\n",
		`[agent Jarvis (@jarvis) called web_search {"q":"temporal"}]`,
		`[agent Jarvis (@jarvis) called save_user_memory {"content":"(private)"}]`,
		"[result of web_search, called by agent Jarvis (@jarvis)] found xxx",
		"[error from save_user_memory, called by agent Jarvis (@jarvis)] (private)\n\n[agent Jarvis (@jarvis)] voici\n\n[Bob] meanwhile",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("model reads %q\nwant %q in it", text, want)
		}
	}
	if strings.Contains(text, "secret") {
		t.Error("a private tool input or result reached another agent")
	}
	if len(text) > 2500 {
		t.Errorf("another agent's tool result was not clipped: %d bytes", len(text))
	}
}

// The reader's own tool calls keep their blocks, each followed by its result.
// A member's message written between the call and the result comes after the
// whole turn, its answer included: the next turn ends on that message, not on
// the answer. Another agent's turn later on is text.
func TestConvert_KeepsItsOwnToolPairing(t *testing.T) {
	view := View{Self: "smith", Agents: map[string]Label{"jarvis": {Name: "Jarvis", Mention: "jarvis"}}}
	var s stored
	s.add("", store.Message{Role: store.RoleUser, Content: `"@smith lis le dépôt"`, Author: "Alice"}).
		add("r-1.0", store.Message{Role: store.RoleAssistant, AgentID: "smith", ToolCalls: []store.ToolCall{{ID: "s1", Name: "read_file"}}}).
		add("", store.Message{Role: store.RoleUser, Content: `"meanwhile"`, Author: "Bob"}).
		add("r-1.0", store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "s1", Content: "main.go"}}).
		add("r-1.0", store.Message{Role: store.RoleAssistant, Content: `"lu"`, AgentID: "smith"}).
		add("", store.Message{Role: store.RoleUser, Content: `"@jarvis cherche, @smith juge"`, Author: "Alice"}).
		add("r-3.0", store.Message{Role: store.RoleAssistant, AgentID: "jarvis", ToolCalls: []store.ToolCall{{ID: "j1", Name: "web_search"}}}).
		add("r-3.0", store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "j1", Content: "found"}}).
		add("r-3.0", store.Message{Role: store.RoleAssistant, Content: `"voici"`, AgentID: "jarvis"})
	msgs := s.read(view)

	want := []struct{ role, content, call, result string }{
		{role: "user", content: "[Alice] @smith lis le dépôt"},
		{role: "assistant", call: "s1"},
		{role: "tool", result: "s1"},
		{role: "assistant", content: "lu"},
		{role: "user", content: "[Bob] meanwhile\n\n[Alice] @jarvis cherche, @smith juge\n\n[agent Jarvis (@jarvis) called web_search {}]\n\n[result of web_search, called by agent Jarvis (@jarvis)] found\n\n[agent Jarvis (@jarvis)] voici"},
	}
	if len(msgs) != len(want) {
		t.Fatalf("%d messages, want %d: %+v", len(msgs), len(want), msgs)
	}
	for i, w := range want {
		m := msgs[i]
		if m.Role != w.role || (w.content != "" && textOf(m) != w.content) {
			t.Errorf("message %d = %s %q, want %s %q", i, m.Role, textOf(m), w.role, w.content)
		}
		if w.call != "" && (len(m.ToolCalls) != 1 || m.ToolCalls[0].ID != w.call) {
			t.Errorf("message %d: calls %+v, want %s", i, m.ToolCalls, w.call)
		}
		if w.result != "" && (m.ToolResult == nil || m.ToolResult.ToolCallID != w.result) {
			t.Errorf("message %d: result %+v, want the result of %s", i, m.ToolResult, w.result)
		}
	}
}

// Why a turn failed is for the members: the model never sees it, or it would
// answer the error instead of the user. The two user messages around it are
// read as one.
func TestConvert_SkipsTurnErrors(t *testing.T) {
	msgs := Convert([]store.Message{
		{Role: store.RoleUser, Content: `"analyse the repo"`},
		{Role: store.RoleAssistant, Kind: store.KindTurnError, Content: `"call LLM: credit balance is too low"`},
		{Role: store.RoleUser, Content: `"try again"`},
	}, View{Self: "default"})
	if len(msgs) != 1 || textOf(msgs[0]) != "analyse the repo\n\ntry again" {
		t.Errorf("messages = %+v, want the two user messages alone", msgs)
	}
}

// The summary reaches the model as context carried over, not as something the
// user said.
func TestConvert_FramesForkSummary(t *testing.T) {
	msgs := Convert([]store.Message{{Role: store.RoleUser, Kind: store.KindForkSummary, Content: `"what happened"`}}, View{Self: "default"})
	got := textOf(msgs[0])
	if !strings.HasPrefix(got, "[Context carried over from an earlier conversation") || !strings.HasSuffix(got, "what happened") {
		t.Errorf("model sees %q", got)
	}
}

func TestClip(t *testing.T) {
	if got := Clip("héllo", 2); got != "h…" {
		t.Errorf("Clip cut a rune: %q", got)
	}
	if got := Clip("short", 10); got != "short" {
		t.Errorf("Clip changed a short text: %q", got)
	}
}
