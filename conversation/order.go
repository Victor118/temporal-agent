package conversation

import (
	"cmp"
	"slices"

	"github.com/victor/temporal-agent/store"
)

// Order puts each turn, as one block, right after the message it answers,
// its anchor (store.TurnAnchor): after the last message no turn wrote whose
// ID is not above the anchor. The turns of one anchor (a relay) come in the
// order of their first message, and the messages no turn wrote in the order
// of their IDs.
//
// Participants answer in parallel: Jarvis answers message 10 with IDs 11 to
// 40 while Smith answers message 12 with IDs 13 to 20. Read by ID, Smith's
// answer would sit inside Jarvis's turn, and message 12 between Jarvis's
// call and its result, which the LLM API rejects. Placed by anchor, each
// turn follows its question and stays whole: a tool call keeps its result
// next to it, since a turn stores a call with its results.
//
// The reading turn comes last: its anchor is the last message no turn wrote
// that it reads (store.TurnReads), and a turn of another participant
// anchored there has not ended, so it is not read. The conversation never
// ends on another participant's answer. With one turn at a time, this is
// the order of the IDs with each message stored during a turn released
// after it, as it was when a session answered its messages one by one.
func Order(messages []store.MessageWithID) []store.MessageWithID {
	type block struct {
		anchor, first int64
		msgs          []store.MessageWithID
	}
	byTurn := map[string]*block{}
	var blocks []*block
	var plain []store.MessageWithID
	for _, m := range messages {
		turn, ok := store.TurnOf(m.Key)
		if !ok {
			plain = append(plain, m)
			continue
		}
		b := byTurn[turn]
		if b == nil {
			anchor, _ := store.TurnAnchor(turn) // a turn always has one
			b = &block{anchor: anchor, first: m.ID}
			byTurn[turn] = b
			blocks = append(blocks, b)
		}
		b.first = min(b.first, m.ID)
		b.msgs = append(b.msgs, m)
	}
	byID := func(a, b store.MessageWithID) int { return cmp.Compare(a.ID, b.ID) }
	slices.SortStableFunc(plain, byID)
	slices.SortStableFunc(blocks, func(a, b *block) int {
		return cmp.Or(cmp.Compare(a.anchor, b.anchor), cmp.Compare(a.first, b.first))
	})

	out := make([]store.MessageWithID, 0, len(messages))
	next := 0
	for _, p := range plain {
		// The blocks anchored before p follow the message before it.
		for ; next < len(blocks) && blocks[next].anchor < p.ID; next++ {
			out = append(out, sortedBlock(blocks[next].msgs, byID)...)
		}
		out = append(out, p)
	}
	for ; next < len(blocks); next++ {
		out = append(out, sortedBlock(blocks[next].msgs, byID)...)
	}
	return out
}

// sortedBlock is a turn's messages in the order it wrote them.
func sortedBlock(msgs []store.MessageWithID, byID func(a, b store.MessageWithID) int) []store.MessageWithID {
	slices.SortStableFunc(msgs, byID)
	return msgs
}
