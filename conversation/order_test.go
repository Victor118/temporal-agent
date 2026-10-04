package conversation

import (
	"cmp"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"strings"
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
		add("m1.a", store.Message{Role: store.RoleAssistant, ToolCalls: []store.ToolCall{{ID: "t1"}, {ID: "t2"}}}).
		add("m1.a", store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t1"}}).
		add("", store.Message{Role: store.RoleUser, Content: `"meanwhile"`, Author: "Bob"}).
		add("m1.a", store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t2"}}).
		add("m1.a", store.Message{Role: store.RoleAssistant, Content: `"done"`})

	got := keysOf(Order(s.msgs))
	if want := "[msg:a m1.a:0 m1.a:1 m1.a:2 m1.a:3 msg:d]"; got != want {
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
		add("m1.smith", store.Message{Role: store.RoleAssistant, AgentID: "smith", ToolCalls: []store.ToolCall{{ID: "s1", Name: "read_file"}}}).
		add("m1.smith", store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "s1", Content: "main.go"}}).
		add("", store.Message{Role: store.RoleUser, Content: `"and the tests?"`, Author: "Bob"}).
		add("m1.smith", store.Message{Role: store.RoleAssistant, Content: `"read"`, AgentID: "smith"})

	msgs := s.read(View{Self: "smith"})
	last := msgs[len(msgs)-1]
	if last.Role != "user" || textOf(last) != "[Bob] and the tests?" {
		t.Errorf("the conversation ends on %s %q, want Bob's message", last.Role, textOf(last))
	}
}

// A relay: the turns of the agents a message addresses, one after the other,
// follow it as blocks in the order they ran; a message written meanwhile,
// even before the first agent wrote anything, comes after them.
func TestOrder_KeepsTheTurnsOfOneMessageTogether(t *testing.T) {
	jarvis, smith := store.TurnKey(1, "jarvis"), store.TurnKey(1, "smith")
	var s stored
	s.add("", store.Message{Role: store.RoleUser, Content: `"@jarvis cherche, @smith juge"`}).
		add("", store.Message{Role: store.RoleUser, Content: `"while jarvis thinks"`}).
		add(jarvis, store.Message{Role: store.RoleAssistant, AgentID: "jarvis", Content: `"cherché"`}).
		add("", store.Message{Role: store.RoleUser, Content: `"meanwhile"`}).
		add(smith, store.Message{Role: store.RoleAssistant, AgentID: "smith", Content: `"jugé"`}).
		add("", store.Message{Role: store.RoleUser, Content: `"after"`})

	got := keysOf(Order(s.msgs))
	want := fmt.Sprint([]string{"msg:a", jarvis + ":0", smith + ":0", "msg:b", "msg:d", "msg:f"})
	if got != want {
		t.Errorf("order %s, want %s", got, want)
	}
}

// A task result or a fork summary is placed as it was stored; a conversation
// with no turn in it is left alone.
func TestOrder_LeavesTheRestInPlace(t *testing.T) {
	msgs := []store.MessageWithID{
		{ID: 1, Key: store.ForkSummaryKey},
		{ID: 2, Key: store.HumanMessageKey("a")},
		{ID: 3, Key: store.ScheduledMessageKey("s", 1)},
		{ID: 4, Key: store.HumanMessageKey("b")},
	}
	if got, want := keysOf(Order(msgs)), keysOf(msgs); got != want {
		t.Errorf("order %s, want %s", got, want)
	}
}

// Two messages written while a turn ran, the first answered next by the
// same participant: its turn follows it, before the second message; the
// last message, unanswered, comes last.
func TestOrder_TwoMessagesDuringATurn(t *testing.T) {
	first, second := store.TurnKey(1, "a"), store.TurnKey(3, "a")
	var s stored
	s.add("", store.Message{Role: store.RoleUser, Content: `"M1"`}).
		add(first, store.Message{Role: store.RoleAssistant, ToolCalls: []store.ToolCall{{ID: "t1"}}}).
		add("", store.Message{Role: store.RoleUser, Content: `"M2"`}).
		add("", store.Message{Role: store.RoleUser, Content: `"M3"`}).
		add(first, store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t1"}}).
		add(second, store.Message{Role: store.RoleAssistant, Content: `"R3"`}).
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
	turn := store.TurnKey(1, "a")
	msgs := []store.MessageWithID{
		{ID: 1, Key: store.HumanMessageKey("a"), Message: store.Message{Role: store.RoleUser, Content: `"go"`}},
		{ID: 2, Key: store.TurnMessageKey(turn, 0), Message: store.Message{Role: store.RoleAssistant, ToolCalls: []store.ToolCall{{ID: "t1"}}}},
		{ID: 3, Key: store.ForkReportKey("f1", 0, 7), Message: store.Message{Role: store.RoleUser, Kind: store.KindForkReport, Content: `"report"`}},
		{ID: 4, Key: store.TurnMessageKey(turn, 1), Message: store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t1"}}},
		{ID: 5, Key: store.TurnMessageKey(turn, 2), Message: store.Message{Role: store.RoleAssistant, Content: `"done"`}},
	}
	got := keysOf(Order(msgs))
	if want := "[msg:a m1.a:0 m1.a:1 m1.a:2 report:f1:0-7]"; got != want {
		t.Errorf("order %s, want %s", got, want)
	}
}

// --- Participants in parallel ---

// session builds a history message by message: IDs in order, each turn's
// messages indexed in order.
type session struct {
	msgs  []store.MessageWithID
	index map[string]int
}

func (s *session) add(key string, m store.Message) int64 {
	id := int64(len(s.msgs) + 1)
	s.msgs = append(s.msgs, store.MessageWithID{ID: id, Key: key, Message: m})
	return id
}

func (s *session) human(text string) int64 {
	return s.add(store.HumanMessageKey(fmt.Sprint(len(s.msgs)+1)), store.Message{Role: store.RoleUser, Content: `"` + text + `"`})
}

func (s *session) turn(turn string, m store.Message) int64 {
	if s.index == nil {
		s.index = map[string]int{}
	}
	m.AgentID = cmp.Or(m.AgentID, store.TurnParticipant(turn))
	if m.Role == "" {
		m.Role = store.RoleAssistant
	}
	i := s.index[turn]
	s.index[turn]++
	return s.add(store.TurnMessageKey(turn, i), m)
}

func (s *session) call(turn, id string) int64 {
	return s.turn(turn, store.Message{ToolCalls: []store.ToolCall{{ID: id, Name: "x"}}})
}

func (s *session) result(turn, id string) int64 {
	return s.turn(turn, store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: id}})
}

func (s *session) answer(turn, text string) int64 {
	return s.turn(turn, store.Message{Content: `"` + text + `"`})
}

func (s *session) end(turn string) int64 {
	return s.add(store.TurnEndKey(turn), store.TurnEnd(store.TurnParticipant(turn), ""))
}

// load is what a turn of scope reads of s, in the order the model reads it.
func (s *session) load(scope store.TurnScope) []store.MessageWithID {
	ends := store.TurnEndIDs(s.msgs)
	var loaded []store.MessageWithID
	for _, m := range s.msgs {
		if store.TurnReads(m.ID, m.Key, scope, ends) {
			loaded = append(loaded, m)
		}
	}
	return Order(loaded)
}

// checkConversation checks what the LLM API and the model need of a
// conversation the turn of scope reads: no message between a tool call and
// its results, each turn after the message it answers, and the conversation
// not ending on another participant's turn.
func checkConversation(t *testing.T, name string, ordered []store.MessageWithID, scope store.TurnScope) {
	t.Helper()
	pending := map[string]bool{}
	seen := map[int64]bool{}
	for _, m := range ordered {
		turn, isTurn := store.TurnOf(m.Key)
		if !isTurn && len(pending) > 0 {
			t.Errorf("%s: message %d between a tool call and its result", name, m.ID)
		}
		if isTurn {
			if anchor, _ := store.TurnAnchor(turn); !seen[anchor] && slices.ContainsFunc(ordered, func(o store.MessageWithID) bool { return o.ID == anchor }) {
				t.Errorf("%s: turn %s before the message it answers", name, turn)
			}
		}
		seen[m.ID] = true
		for _, tc := range m.ToolCalls {
			pending[tc.ID] = true
		}
		if m.ToolResult != nil {
			delete(pending, m.ToolResult.ToolCallID)
		}
	}
	if len(pending) > 0 {
		t.Errorf("%s: calls without their results: %v", name, pending)
	}
	if n := len(ordered); n > 0 {
		if turn, ok := store.TurnOf(ordered[n-1].Key); ok && store.TurnParticipant(turn) != scope.Participant && !slices.Contains(scope.Turns, turn) {
			t.Errorf("%s: ends on another participant's turn %s", name, turn)
		}
	}
}

// Jarvis answers message 10 while Smith answers message 12 (§5 of the
// design): Smith's answer follows 12, Jarvis's whole turn follows 10, and
// no message falls between a call and its result.
func TestOrder_OverlappingParticipants(t *testing.T) {
	var s session
	for i := 1; i <= 9; i++ {
		s.human(fmt.Sprint("m", i))
	}
	m10 := s.human("@jarvis go")
	j := store.TurnKey(m10, "jarvis")
	s.call(j, "j1")
	m12 := s.human("@smith go")
	sm := store.TurnKey(m12, "smith")
	s.call(sm, "s1")
	s.result(j, "j1")
	s.result(sm, "s1")
	s.answer(sm, "S done")
	s.end(sm)
	m18 := s.human("meanwhile")
	s.answer(j, "J done")
	s.end(j)
	m21 := s.human("@jarvis next")
	j2 := store.TurnKey(m21, "jarvis")
	s.answer(j2, "J2")

	for _, c := range []struct {
		name  string
		scope store.TurnScope
		want  []int64
	}{
		{"jarvis on 21", store.ScopeOf(j2, nil), []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 14, 19, 12, 13, 15, 16, 18, 21, 22}},
		{"bob on 18", store.ScopeOf(store.TurnKey(m18, "bob"), nil), []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 15, 16, 18}},
		{"relay to smith on 10", store.ScopeOf(store.TurnKey(m10, "smith"), []string{j}), []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 14, 19}},
	} {
		got := s.load(c.scope)
		var gotIDs []int64
		for _, m := range got {
			gotIDs = append(gotIDs, m.ID)
		}
		if !slices.Equal(gotIDs, c.want) {
			t.Errorf("%s: order %v, want %v", c.name, gotIDs, c.want)
		}
		checkConversation(t, c.name, got, c.scope)
	}

	// The whole history, as the thread would order it: each turn follows its
	// message, whole.
	var all []int64
	for _, m := range Order(s.msgs) {
		all = append(all, m.ID)
	}
	if want := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 14, 19, 20, 12, 13, 15, 16, 17, 18, 21, 22}; !slices.Equal(all, want) {
		t.Errorf("full order %v, want %v", all, want)
	}
}

// A fork's summary comes first; a report and a task result posted during a
// turn come after it, as a person's message would.
func TestOrder_SummaryReportAndTaskResult(t *testing.T) {
	var s session
	s.add(store.ForkSummaryKey, store.Message{Role: store.RoleUser, Kind: store.KindForkSummary, Content: `"sum"`})
	q := s.human("@jarvis q")
	tk := store.TurnKey(q, "jarvis")
	s.call(tk, "t")
	s.add(store.ForkReportKey("f", 0, 3), store.Message{Role: store.RoleUser, Kind: store.KindForkReport, Content: `"rep"`})
	s.add(store.ScheduledMessageKey("s", 1), store.Message{Role: store.RoleUser, Content: `"sched"`})
	s.result(tk, "t")
	s.answer(tk, "ok")
	s.end(tk)
	n := s.human("@smith n")
	scope := store.ScopeOf(store.TurnKey(n, "smith"), nil)

	got := s.load(scope)
	want := fmt.Sprint([]string{store.ForkSummaryKey, "msg:2", tk + ":0", tk + ":1", tk + ":2", "report:f:0-3", "sched:s:1", "msg:9"})
	if keysOf(got) != want {
		t.Errorf("order %s, want %s", keysOf(got), want)
	}
	checkConversation(t, "smith after the turn", got, scope)
}

// --- The order of a session answering one message at a time ---

// legacyOrder is Order as it was when a session answered its messages one at
// a time: a turn's group spanned from its anchor to its last message, and
// a message stored inside a span was released after the furthest end of
// the spans that started before it. The anchor order must give the same
// conversations on such histories.
func legacyOrder(messages []store.MessageWithID) []store.MessageWithID {
	type span struct{ from, to int64 }
	group := func(m store.MessageWithID) (string, bool) {
		turn, ok := store.TurnOf(m.Key)
		if !ok {
			return "", false
		}
		return turn[:strings.LastIndexByte(turn, '.')], true
	}
	spans := map[string]*span{}
	for _, m := range messages {
		g, ok := group(m)
		if !ok {
			continue
		}
		s := spans[g]
		if s == nil {
			anchor, _ := store.TurnAnchor(g + ".x")
			s = &span{from: anchor}
			spans[g] = s
		}
		s.to = m.ID
	}
	sorted := make([]span, 0, len(spans))
	for _, s := range spans {
		sorted = append(sorted, *s)
	}
	slices.SortFunc(sorted, func(a, b span) int { return cmp.Compare(a.from, b.from) })
	furthest := make([]int64, len(sorted))
	for i, s := range sorted {
		furthest[i] = s.to
		if i > 0 {
			furthest[i] = max(furthest[i], furthest[i-1])
		}
	}
	releasedAfter := func(id int64) int64 {
		n := sort.Search(len(sorted), func(i int) bool { return sorted[i].from >= id })
		if n > 0 && furthest[n-1] > id {
			return furthest[n-1]
		}
		return 0
	}
	release := map[int64][]store.MessageWithID{}
	out := make([]store.MessageWithID, 0, len(messages))
	for _, m := range messages {
		if _, ok := group(m); !ok {
			if after := releasedAfter(m.ID); after > 0 {
				release[after] = append(release[after], m)
				continue
			}
		}
		out = append(out, m)
		out = append(out, release[m.ID]...)
	}
	return out
}

// legacyReads is what a turn read when a session answered its messages one
// at a time: the session up to its message, its group's turns (turns), and
// the earlier messages' turns, even written after it.
func legacyReads(id int64, key string, upTo int64, turns []string) bool {
	if id <= upTo {
		return true
	}
	turn, ok := store.TurnOf(key)
	if !ok {
		return false
	}
	if slices.Contains(turns, turn) {
		return true
	}
	anchor, _ := store.TurnAnchor(turn)
	return anchor < upTo
}

// sequentialTurn is a turn of a random sequential history: its key, and the
// turns that answered its message before it.
type sequentialTurn struct {
	key     string
	earlier []string
}

// randomSequential builds a history answered one message at a time: each
// message by one or two participants in turn (a relay), people writing
// before and during the turns, sometimes a task result at the end. Each
// turn writes a call and its result, or an answer, and ends.
func randomSequential(rng *rand.Rand) (*session, []sequentialTurn) {
	var s session
	var turns []sequentialTurn
	pending := []int64{s.human("h")}
	nTurns := 1 + rng.Intn(6)
	for t := 0; t < nTurns; t++ {
		if len(pending) == 0 {
			pending = append(pending, s.human("h"))
		}
		anchor := pending[0]
		pending = pending[1:]
		var earlier []string
		nAgents := 1 + rng.Intn(2)
		for a := 0; a < nAgents; a++ {
			key := store.TurnKey(anchor, fmt.Sprint("p", rng.Intn(3)))
			if slices.Contains(earlier, key) {
				continue // a participant answers a message once
			}
			turns = append(turns, sequentialTurn{key: key, earlier: slices.Clone(earlier)})
			if rng.Intn(3) == 0 {
				pending = append(pending, s.human("before"))
			}
			nMsgs := 1 + rng.Intn(3)
			for i := 0; i < nMsgs; i++ {
				if rng.Intn(2) == 0 {
					id := fmt.Sprintf("c%d-%d-%d", t, a, i)
					s.call(key, id)
					if rng.Intn(3) == 0 {
						pending = append(pending, s.human("during"))
					}
					s.result(key, id)
				} else {
					s.answer(key, "x")
				}
				if rng.Intn(3) == 0 {
					pending = append(pending, s.human("during"))
				}
			}
			s.end(key)
			earlier = append(earlier, key)
		}
	}
	if rng.Intn(3) == 0 {
		s.add(store.ScheduledMessageKey("s", int64(len(s.msgs))), store.Message{Role: store.RoleUser, Content: `"sched"`})
	}
	return &s, turns
}

// On histories answered one message at a time, the anchor order is the order
// a session gave, on the whole history and on what each turn reads (by the
// former reading rule and by the new one); and every conversation a turn
// reads is one the API accepts.
func TestOrder_SameAsOneAtATime(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	compared := 0
	for iter := 0; iter < 3000; iter++ {
		s, turns := randomSequential(rng)
		if a, b := keysOf(legacyOrder(s.msgs)), keysOf(Order(s.msgs)); a != b {
			t.Fatalf("history %d, full: one at a time %s\nby anchor %s", iter, a, b)
		}
		for _, tr := range turns {
			upTo, _ := store.TurnAnchor(tr.key)
			var legacy []store.MessageWithID
			for _, m := range s.msgs {
				if legacyReads(m.ID, m.Key, upTo, append(slices.Clone(tr.earlier), tr.key)) {
					legacy = append(legacy, m)
				}
			}
			if a, b := keysOf(legacyOrder(legacy)), keysOf(Order(legacy)); a != b {
				t.Fatalf("history %d, turn %s: one at a time %s\nby anchor %s", iter, tr.key, a, b)
			}
			scope := store.ScopeOf(tr.key, tr.earlier)
			ends := store.TurnEndIDs(s.msgs)
			var loaded []store.MessageWithID
			for _, m := range s.msgs {
				if store.TurnReads(m.ID, m.Key, scope, ends) {
					loaded = append(loaded, m)
				}
			}
			if a, b := keysOf(legacyOrder(loaded)), keysOf(Order(loaded)); a != b {
				t.Fatalf("history %d, turn %s, new rule: one at a time %s\nby anchor %s", iter, tr.key, a, b)
			}
			checkConversation(t, fmt.Sprintf("history %d, turn %s", iter, tr.key), Order(loaded), scope)
			compared++
		}
	}
	if compared < 3000 {
		t.Errorf("only %d turns compared", compared)
	}
}
