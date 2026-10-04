package store

import (
	"strings"
	"testing"
	"unicode/utf8"
)

// A message's key tells which turn wrote it, its end included; the turn's
// key tells the message it answers and its participant.
func TestTurnOf(t *testing.T) {
	for _, c := range []struct {
		key, turn   string
		ok          bool
		anchor      int64
		participant string
	}{
		{TurnMessageKey(TurnKey(12, "jarvis"), 3), "m12.jarvis", true, 12, "jarvis"},
		{TurnEndKey(TurnKey(12, "jarvis")), "m12.jarvis", true, 12, "jarvis"},
		{TurnMessageKey(TurnKey(7, "i=42"), 0), "m7.i=42", true, 7, "i=42"},
		{TurnMessageKey(TurnKey(7, "jarvis~btw"), 0), "m7.jarvis~btw", true, 7, "jarvis~btw"},
		// A fork's summary is no turn's, though its key looks like one.
		{ForkSummaryKey, "", false, 0, ""},
		{HumanMessageKey("0b6c"), "", false, 0, ""},
		{HumanMessageKey("m1.x:2"), "", false, 0, ""},
		{ScheduledMessageKey("sched-1", 42), "", false, 0, ""},
		// A fork's report is no turn's, whatever its colons.
		{ForkReportKey("6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b", 0, 12), "", false, 0, ""},
		{"nokey", "", false, 0, ""},
		{"m12.jarvis:x", "", false, 0, ""},  // neither an index nor the end
		{"m+12.jarvis:0", "", false, 0, ""}, // the anchor is digits alone
		{"m12.:0", "", false, 0, ""},        // no participant
		{"mx.jarvis:0", "", false, 0, ""},
		{"m10.a.b:0", "", false, 0, ""}, // one dot: no participant holds one
		{"m10..b:end", "", false, 0, ""},
		{"m.jarvis:0", "", false, 0, ""},
	} {
		turn, ok := TurnOf(c.key)
		if turn != c.turn || ok != c.ok {
			t.Errorf("TurnOf(%q) = %q %v, want %q %v", c.key, turn, ok, c.turn, c.ok)
			continue
		}
		if !ok {
			continue
		}
		if anchor, ok := TurnAnchor(turn); !ok || anchor != c.anchor {
			t.Errorf("TurnAnchor(%q) = %d %v, want %d", turn, anchor, ok, c.anchor)
		}
		if p := TurnParticipant(turn); p != c.participant {
			t.Errorf("TurnParticipant(%q) = %q, want %q", turn, p, c.participant)
		}
	}
	if _, ok := TurnAnchor("fork-summary"); ok {
		t.Error("fork-summary has an anchor")
	}
	if TurnParticipant("m10.a.b") != "" || TurnParticipant("r-1@3.0x") != "" {
		t.Error("a key of another form has a participant")
	}
}

// Only a turn's end is one: a message at index 0 is not.
func TestIsTurnEnd(t *testing.T) {
	turn := TurnKey(10, "jarvis")
	for key, want := range map[string]bool{
		TurnEndKey(turn):            true,
		TurnMessageKey(turn, 0):     false,
		HumanMessageKey("end"):      false,
		"fork-summary:end":          false,
		ScheduledMessageKey("x", 1): false,
	} {
		if got := IsTurnEnd(key); got != want {
			t.Errorf("IsTurnEnd(%q) = %v, want %v", key, got, want)
		}
	}
}

// A turn's end keeps why it failed, cut on a rune under the bound; one that
// did not fail carries nothing.
func TestTurnEnd(t *testing.T) {
	ok := TurnEnd("jarvis", "")
	if ok.Kind != KindTurnEnd || ok.AgentID != "jarvis" || ok.Content != "" || TurnEndError(ok) != "" {
		t.Errorf("end of a turn that did not fail: %+v", ok)
	}
	failed := TurnEnd("jarvis", "call LLM: "+strings.Repeat("é", 2000))
	reason := TurnEndError(failed)
	if !strings.HasPrefix(reason, "call LLM: é") || len(reason) > maxTurnErrorBytes+len("…") || !utf8.ValidString(reason) {
		t.Errorf("reason of %d bytes (valid %v): want it cut on a rune, under the bound", len(reason), utf8.ValidString(reason))
	}
	if TurnEndError(Message{Role: RoleAssistant, Content: `"an answer"`}) != "" {
		t.Error("an answer read as a turn's error")
	}
}

// history is a session as the store numbers it: each message's ID is its
// position, from 1.
type history []string

func (h history) messages() []MessageWithID {
	out := make([]MessageWithID, len(h))
	for i, key := range h {
		out[i] = MessageWithID{ID: int64(i + 1), Key: key}
	}
	return out
}

// reads lists the IDs a turn of scope reads in h.
func (h history) reads(scope TurnScope) []int64 {
	msgs := h.messages()
	ends := TurnEndIDs(msgs)
	var ids []int64
	for _, m := range msgs {
		if TurnReads(m.ID, m.Key, scope, ends) {
			ids = append(ids, m.ID)
		}
	}
	return ids
}

// overlap is the case of the design (§5): Jarvis answers message 10 while
// Smith answers message 12, and ends after him.
//
//	10 M10 @jarvis      11 jarvis call     12 M12 @smith    13 smith call
//	14 jarvis result    15 smith result    16 smith answer  17 smith end
//	18 M18 (no agent)   19 jarvis answer   20 jarvis end    21 M21 @jarvis
//	22 jarvis (M21)
func overlap() history {
	h := make(history, 0, 22)
	for i := 1; i <= 9; i++ {
		h = append(h, HumanMessageKey(string(rune('a'+i))))
	}
	j, s, j2 := TurnKey(10, "jarvis"), TurnKey(12, "smith"), TurnKey(21, "jarvis")
	return append(h,
		HumanMessageKey("m10"), TurnMessageKey(j, 0), HumanMessageKey("m12"), TurnMessageKey(s, 0),
		TurnMessageKey(j, 1), TurnMessageKey(s, 1), TurnMessageKey(s, 2), TurnEndKey(s),
		HumanMessageKey("m18"), TurnMessageKey(j, 2), TurnEndKey(j), HumanMessageKey("m21"),
		TurnMessageKey(j2, 0))
}

func ids(from, to int64, more ...int64) []int64 {
	var out []int64
	for i := from; i <= to; i++ {
		out = append(out, i)
	}
	return append(out, more...)
}

func TestTurnReads(t *testing.T) {
	h := overlap()
	for name, c := range map[string]struct {
		h     history
		scope TurnScope
		want  []int64
	}{
		// Smith answers 12 while Jarvis is between his call (11) and its
		// result: nothing of Jarvis's, though 11 <= 12.
		"smith on 12, jarvis in flight": {h[:16], ScopeOf(TurnKey(12, "smith"), nil), ids(1, 10, 12, 13, 15, 16)},
		// A third participant on 18 reads Smith (ended at 17), not Jarvis
		// (ends at 20), whose call (11) is under 18.
		"bob on 18": {h, ScopeOf(TurnKey(18, "bob"), nil), ids(1, 10, 12, 13, 15, 16, 18)},
		// Jarvis on 21: his own turn on 10 whole, Smith's (ended by 21), 18.
		"jarvis on 21": {h, ScopeOf(TurnKey(21, "jarvis"), nil), ids(1, 16, 18, 19, 21, 22)},
		// A turn reads its own messages, written after its anchor.
		"jarvis on 10, its own": {h, ScopeOf(TurnKey(10, "jarvis"), nil), ids(1, 10, 11, 14, 19)},
		// Smith answered 12, then gets 10 relayed by Jarvis: Jarvis's turn on
		// 10 whole (the relay), not his own on 12, anchored after 10.
		"relay to smith on 10": {h, ScopeOf(TurnKey(10, "smith"), []string{TurnKey(10, "jarvis")}), ids(1, 11, 14, 19)},
		// A participant's turn that never ended (its participant stopped
		// from outside): never read by the others, read by its own.
		"dead turn, another": {history{HumanMessageKey("a"), TurnMessageKey(TurnKey(1, "jarvis"), 0), TurnMessageKey(TurnKey(1, "jarvis"), 1), HumanMessageKey("b")},
			ScopeOf(TurnKey(4, "smith"), nil), ids(1, 1, 4)},
		"dead turn, its own": {history{HumanMessageKey("a"), TurnMessageKey(TurnKey(1, "jarvis"), 0), TurnMessageKey(TurnKey(1, "jarvis"), 1), HumanMessageKey("b")},
			ScopeOf(TurnKey(4, "jarvis"), nil), ids(1, 4)},
		// A fork's summary, a report and a task result are read up to the
		// anchor, like a person's message.
		"no turn's": {history{ForkSummaryKey, HumanMessageKey("q"), ForkReportKey("f", 0, 3), ScheduledMessageKey("s", 1), HumanMessageKey("r"), ScheduledMessageKey("s", 2)},
			ScopeOf(TurnKey(5, "jarvis"), nil), ids(1, 5)},
		// An aside is another participant: the main turn in flight is not read.
		"aside": {h[:16], ScopeOf(TurnKey(12, "jarvis~btw"), nil), ids(1, 10, 12)},
	} {
		got := c.h.reads(c.scope)
		if !slicesEqual(got, c.want) {
			t.Errorf("%s: reads %v, want %v", name, got, c.want)
		}
	}
}

// A turn's end is never read, its own included.
func TestTurnReads_NeverAnEnd(t *testing.T) {
	turn := TurnKey(1, "jarvis")
	h := history{HumanMessageKey("a"), TurnMessageKey(turn, 0), TurnEndKey(turn), HumanMessageKey("b")}
	if got := h.reads(ScopeOf(turn, nil)); !slicesEqual(got, ids(1, 2)) {
		t.Errorf("reads %v, want the message and the answer", got)
	}
	if got := h.reads(ScopeOf(TurnKey(4, "smith"), nil)); !slicesEqual(got, []int64{1, 2, 4}) {
		t.Errorf("reads %v, want the turn without its end", got)
	}
}

func slicesEqual(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
