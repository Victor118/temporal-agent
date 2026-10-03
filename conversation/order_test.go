package conversation

import (
	"fmt"
	"testing"

	"github.com/victor/temporal-agent/store"
)

// keysOf lists the keys of msgs, in order.
func keysOf(msgs []store.MessageWithID) string {
	keys := make([]string, len(msgs))
	for i, m := range msgs {
		keys[i] = m.Key
	}
	return fmt.Sprint(keys)
}

// A member wrote while the agent was between a tool call and its results: the
// message comes after the turn, so the model sees each result right after its
// call (the API rejects it otherwise), and the message is kept.
func TestOrder_DefersAMessageWrittenDuringATurn(t *testing.T) {
	var s stored
	s.add("", store.Message{Role: store.RoleUser, Content: `"go"`}).
		add("r-1.0", store.Message{Role: store.RoleAssistant, ToolCalls: []store.ToolCall{{ID: "t1"}, {ID: "t2"}}}).
		add("r-1.0", store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t1"}}).
		add("", store.Message{Role: store.RoleUser, Content: `"meanwhile"`, Author: "Bob"}).
		add("r-1.0", store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t2"}}).
		add("r-1.0", store.Message{Role: store.RoleAssistant, Content: `"done"`})

	got := keysOf(Order(s.msgs))
	if want := "[msg:a r-1.0:0 r-1.0:1 r-1.0:2 r-1.0:3 msg:d]"; got != want {
		t.Errorf("order %s, want %s", got, want)
	}
}

// A message written between an agent's results and its final answer used to
// stay there: the next turn, the one answering it, then ended on the
// assistant's answer, which the API takes for a start to continue. It now
// ends on the message.
func TestOrder_TheNextTurnEndsOnTheMessage(t *testing.T) {
	var s stored
	s.add("", store.Message{Role: store.RoleUser, Content: `"read the repo"`, Author: "Alice"}).
		add("r-1.0", store.Message{Role: store.RoleAssistant, AgentID: "smith", ToolCalls: []store.ToolCall{{ID: "s1", Name: "read_file"}}}).
		add("r-1.0", store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "s1", Content: "main.go"}}).
		add("", store.Message{Role: store.RoleUser, Content: `"and the tests?"`, Author: "Bob"}).
		add("r-1.0", store.Message{Role: store.RoleAssistant, Content: `"read"`, AgentID: "smith"})

	msgs := s.read(View{Self: "smith"})
	last := msgs[len(msgs)-1]
	if last.Role != "user" || textOf(last) != "[Bob] and the tests?" {
		t.Errorf("the conversation ends on %s %q, want Bob's message", last.Role, textOf(last))
	}
}

// The turns of the agents a message addresses, one after the other, are one
// block, which starts at the snapshot they read: a message written while the
// first agent thought, before it wrote anything, comes after the last
// agent's answer, never before or between them.
func TestOrder_KeepsTheTurnsOfOneMessageTogether(t *testing.T) {
	group := store.TurnGroupKey("r-1", 1) // the snapshot: the question, message 1
	var s stored
	s.add("", store.Message{Role: store.RoleUser, Content: `"@jarvis cherche, @smith juge"`}).
		add("", store.Message{Role: store.RoleUser, Content: `"while jarvis thinks"`}).
		add(store.TurnKey(group, 0), store.Message{Role: store.RoleAssistant, AgentID: "jarvis", Content: `"cherché"`}).
		add("", store.Message{Role: store.RoleUser, Content: `"meanwhile"`}).
		add(store.TurnKey(group, 1), store.Message{Role: store.RoleAssistant, AgentID: "smith", Content: `"jugé"`}).
		add("", store.Message{Role: store.RoleUser, Content: `"after"`})

	got := keysOf(Order(s.msgs))
	want := fmt.Sprint([]string{"msg:a", store.TurnKey(group, 0) + ":0", store.TurnKey(group, 1) + ":0", "msg:b", "msg:d", "msg:f"})
	if got != want {
		t.Errorf("order %s, want %s", got, want)
	}
}

// A task result or a fork summary is placed as it was stored; a conversation
// with no turn in it is left alone.
func TestOrder_LeavesTheRestInPlace(t *testing.T) {
	msgs := []store.MessageWithID{
		{ID: 1, Key: store.TurnMessageKey("fork-summary", 0)},
		{ID: 2, Key: store.HumanMessageKey("a")},
		{ID: 3, Key: store.ScheduledMessageKey("s", 1)},
		{ID: 4, Key: store.HumanMessageKey("b")},
	}
	if got, want := keysOf(Order(msgs)), keysOf(msgs); got != want {
		t.Errorf("order %s, want %s", got, want)
	}
}

// Two messages written while a turn ran: the first is answered next, so its
// turns start inside the first one's; the second, also inside them, comes
// after both blocks, where its own turn will read it.
func TestOrder_OverlappingGroups(t *testing.T) {
	first := store.TurnKey(store.TurnGroupKey("r-1", 1), 0)
	second := store.TurnKey(store.TurnGroupKey("r-2", 3), 0)
	var s stored
	s.add("", store.Message{Role: store.RoleUser, Content: `"M1"`}).
		add(first, store.Message{Role: store.RoleAssistant, ToolCalls: []store.ToolCall{{ID: "t1"}}}).
		add("", store.Message{Role: store.RoleUser, Content: `"M2"`}).
		add("", store.Message{Role: store.RoleUser, Content: `"M3"`}).
		add(first, store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t1"}}).
		add(second, store.Message{Role: store.RoleAssistant, Content: `"R2"`}).
		add("", store.Message{Role: store.RoleUser, Content: `"M4"`})

	got := keysOf(Order(s.msgs))
	want := fmt.Sprint([]string{"msg:a", first + ":0", first + ":1", "msg:c", second + ":0", "msg:d", "msg:g"})
	if got != want {
		t.Errorf("order %s, want %s", got, want)
	}
}

// A fork's report posted while a turn ran is a message no turn wrote: it
// comes after the turn, as a member's would, never between a call and its
// result.
func TestOrder_DefersAForkReportWrittenDuringATurn(t *testing.T) {
	msgs := []store.MessageWithID{
		{ID: 1, Key: store.HumanMessageKey("a"), Message: store.Message{Role: store.RoleUser, Content: `"go"`}},
		{ID: 2, Key: store.TurnMessageKey("r@1.0", 0), Message: store.Message{Role: store.RoleAssistant, ToolCalls: []store.ToolCall{{ID: "t1"}}}},
		{ID: 3, Key: store.ForkReportKey("f1", 0, 7), Message: store.Message{Role: store.RoleUser, Kind: store.KindForkReport, Content: `"report"`}},
		{ID: 4, Key: store.TurnMessageKey("r@1.0", 1), Message: store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t1"}}},
		{ID: 5, Key: store.TurnMessageKey("r@1.0", 2), Message: store.Message{Role: store.RoleAssistant, Content: `"done"`}},
	}
	got := keysOf(Order(msgs))
	if want := "[msg:a r@1.0:0 r@1.0:1 r@1.0:2 report:f1:0-7]"; got != want {
		t.Errorf("order %s, want %s", got, want)
	}
}
