package store

import "testing"

// A message's key tells which turn wrote it, and the turns answering one
// message share a group.
func TestTurnOf(t *testing.T) {
	for _, c := range []struct {
		key, turn, group string
		ok               bool
	}{
		{TurnMessageKey(TurnKey(TurnGroupKey("run-7", 12), 1), 3), "run-7@12.1", "run-7@12", true},
		{TurnMessageKey("fork-summary", 0), "fork-summary", "fork-summary", true},
		{HumanMessageKey("0b6c"), "", "", false},
		{ScheduledMessageKey("sched-1", 42), "", "", false},
		{"nokey", "", "", false},
	} {
		turn, ok := TurnOf(c.key)
		if turn != c.turn || ok != c.ok || (ok && TurnGroup(turn) != c.group) {
			t.Errorf("TurnOf(%q) = %q %v (group %q), want %q %v (group %q)", c.key, turn, ok, TurnGroup(turn), c.turn, c.ok, c.group)
		}
	}
}

// The turns answering a message say which snapshot they read.
func TestTurnSnapshot(t *testing.T) {
	if upTo, ok := TurnSnapshot(TurnGroup(TurnKey(TurnGroupKey("run-7", 12), 0))); !ok || upTo != 12 {
		t.Errorf("snapshot %d %v, want 12", upTo, ok)
	}
	for _, group := range []string{"fork-summary", "run-7@x"} {
		if _, ok := TurnSnapshot(group); ok {
			t.Errorf("%q has a snapshot", group)
		}
	}
}
