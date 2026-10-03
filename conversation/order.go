package conversation

import (
	"cmp"
	"slices"
	"sort"

	"github.com/victor/temporal-agent/store"
)

// Order puts the turns answering one message together, and a message someone
// stored while they ran after them, the rest in the order of their IDs.
// People write to a shared session while agents work; read where it was
// stored, such a message would sit before the turns' answer, or between a tool
// call and its result, which the LLM API rejects, or between an agent's
// results and its answer; and the next turn, the one answering it, would end
// on that answer, which the API reads as a start to continue.
//
// The turns of a message ran from their snapshot (store.TurnSnapshot) to
// their last message: what someone else stored in between was written while
// they ran. A group named without a snapshot runs from its first message.
//
// A tool call and its results are always written by one turn (a turn never
// stores a call without its results), so no message can come between them
// once each group of turns is in one piece.
func Order(messages []store.MessageWithID) []store.MessageWithID {
	// span is a group's IDs: what lies strictly between was written meanwhile.
	type span struct{ from, to int64 }
	spans := map[string]*span{}
	for _, m := range messages {
		g, ok := group(m)
		if !ok {
			continue
		}
		s := spans[g]
		if s == nil {
			s = &span{from: m.ID - 1}
			if upTo, ok := store.TurnSnapshot(g); ok {
				s.from = upTo
			}
			spans[g] = s
		}
		s.to = m.ID
	}

	// By start, with the furthest end among those starting at or before each:
	// a message falls in a span when one starting before it ends after it,
	// and is released after the furthest such end.
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

// group returns the group of turns that wrote m; false when no turn did.
func group(m store.MessageWithID) (string, bool) {
	turn, ok := store.TurnOf(m.Key)
	if !ok {
		return "", false
	}
	return store.TurnGroup(turn), true
}
