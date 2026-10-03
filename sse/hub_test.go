package sse

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/victor/temporal-agent/activity"
)

func ev(typ string) activity.SSEEvent { return activity.SSEEvent{Type: typ, Data: []byte(`{}`)} }

// types are the types of events, in order.
func types(events []Event) []string {
	var out []string
	for _, e := range events {
		out = append(out, e.Type)
	}
	return out
}

func TestHub_DeliversToSubscribers(t *testing.T) {
	h := NewHub()
	a, b := h.Subscribe("", "s1"), h.Subscribe("", "s1")
	other := h.Subscribe("", "s2")
	h.Publish("s1", ev("message"))

	for _, sub := range []*Subscription{a, b} {
		if e := <-sub.C; e.Type != "message" || e.ID == "" {
			t.Errorf("got %+v", e)
		}
	}
	select {
	case e := <-other.C:
		t.Errorf("another session got %q", e.Type)
	default:
	}

	h.Unsubscribe(a)
	if _, open := <-a.C; open {
		t.Error("an unsubscribed channel stays open")
	}
	h.Publish("s1", ev("again"))
	if e := <-b.C; e.Type != "again" {
		t.Errorf("the remaining subscriber got %q", e.Type)
	}
}

// A client that reconnects with the ID of its last event is sent the ones it
// missed, in order; one that missed nothing, nothing; one starting from the
// position its page was rendered at, what came after.
func TestHub_ReplaysWhatAClientMissed(t *testing.T) {
	h := NewHub()
	from := h.Position("s1")
	sub := h.Subscribe("", "s1")
	h.Publish("s1", ev("user_message"))
	h.Publish("s2", ev("elsewhere"))
	h.Publish("s1", ev("turn_started"))
	first := <-sub.C
	<-sub.C
	h.Unsubscribe(sub)
	h.Publish("s1", ev("message"))
	h.Publish("s1", ev("turn_done"))

	again := h.Subscribe(first.ID, "s1")
	defer h.Unsubscribe(again)
	if got := fmt.Sprint(types(again.Missed)); again.Stale || got != "[turn_started message turn_done]" {
		t.Errorf("missed %s (stale %v)", got, again.Stale)
	}
	if page := h.Subscribe(from, "s1"); fmt.Sprint(types(page.Missed)) != "[user_message turn_started message turn_done]" {
		t.Errorf("from the page's position: %v", types(page.Missed))
	}
	if now := h.Subscribe(h.Position("s1"), "s1"); now.Stale || len(now.Missed) != 0 {
		t.Errorf("a client up to date: %+v", now)
	}
	if plain := h.Subscribe("", "s1"); plain.Stale || len(plain.Missed) != 0 {
		t.Errorf("a plain subscriber: %+v", plain)
	}
}

// Events a topic no longer keeps cannot be replayed: the client reloads. The
// limit is per topic: another topic's events do not push them out.
func TestHub_AGapLeftTheBufferIsStale(t *testing.T) {
	h := NewHub()
	h.keep = 3
	sub := h.Subscribe("", "s1")
	h.Publish("s1", ev("a"))
	last := (<-sub.C).ID
	for i := 0; i < 10; i++ {
		h.Publish("s2", ev("other"))
	}
	h.Publish("s1", ev("b"))
	h.Publish("s1", ev("c"))
	if s := h.Subscribe(last, "s1"); s.Stale || fmt.Sprint(types(s.Missed)) != "[b c]" {
		t.Errorf("within the buffer: %v (stale %v)", types(s.Missed), s.Stale)
	}
	h.Publish("s1", ev("d"))
	h.Publish("s1", ev("e")) // a is gone: b c d e do not fit in 3
	s := h.Subscribe(last, "s1")
	if !s.Stale || len(s.Missed) != 0 {
		t.Errorf("past the buffer: %v (stale %v)", types(s.Missed), s.Stale)
	}
	if s.At != h.Position("s1") {
		t.Errorf("reload at %s, want %s", s.At, h.Position("s1"))
	}
}

// An ID from another hub (before a restart), or one it never gave, is stale.
func TestHub_AnotherEpochIsStale(t *testing.T) {
	old, h := NewHub(), NewHub()
	h.epoch = old.epoch + "x"
	old.Publish("s1", ev("message"))
	id := old.Position("s1")
	h.Publish("s1", ev("message"))
	for _, last := range []string{id, h.epoch + "-99", "garbage", h.epoch + "-x"} {
		if s := h.Subscribe(last, "s1"); !s.Stale {
			t.Errorf("%q is not stale", last)
		}
	}
}

// A topic nobody listens to is dropped after a while; a client coming back
// then reloads.
func TestHub_IdleTopicsGo(t *testing.T) {
	h := NewHub()
	now := time.Now()
	h.now = func() time.Time { return now }
	h.Publish("s1", ev("message"))
	last := h.Position("s1")
	h.Publish("s1", ev("message"))
	now = now.Add(DefaultIdle + time.Minute)
	h.Publish("s2", ev("message"))
	if _, ok := h.topics["s1"]; ok {
		t.Fatal("an idle topic stayed")
	}
	if s := h.Subscribe(last, "s1"); !s.Stale {
		t.Error("a client of a dropped topic does not reload")
	}
}

// A subscriber too slow to keep up is dropped rather than skipped: its
// stream ends, and its client reconnects and catches up.
func TestHub_ASlowSubscriberIsDropped(t *testing.T) {
	h := NewHub()
	slow := h.Subscribe("", "s1")
	var last string
	for i := 0; i <= subscriberBuffer; i++ {
		h.Publish("s1", ev("tool_calls"))
	}
	for e := range slow.C {
		last = e.ID
	}
	h.Unsubscribe(slow) // closed already: no panic
	if s := h.Subscribe(last, "s1"); s.Stale || len(s.Missed) != 1 {
		t.Errorf("after the drop: %d missed (stale %v)", len(s.Missed), s.Stale)
	}
}

// Every event goes through the observers before its subscribers get it, and
// an observer may publish.
func TestHub_ObserversSeeEventsFirst(t *testing.T) {
	h := NewHub()
	sub := h.Subscribe("", "s1")
	tree := h.Subscribe("", "tree:u1")
	var seen []string
	h.Observe(func(topic string, e activity.SSEEvent) {
		seen = append(seen, topic+" "+e.Type)
		if topic == "s1" {
			select {
			case <-sub.C:
				t.Error("a subscriber got the event before the observer")
			default:
			}
			h.Publish("tree:u1", ev("changed"))
		}
	})
	h.Publish("s1", ev("turn_done"))
	if fmt.Sprint(seen) != "[s1 turn_done tree:u1 changed]" || (<-tree.C).Type != "changed" || (<-sub.C).Type != "turn_done" {
		t.Errorf("seen %v", seen)
	}
}

// A browser closing its stream while an agent answers is the common case:
// publishing, subscribing with a replay and unsubscribing at the same time
// must never send on a closed channel, nor lose the order. Run with -race.
func TestHub_ConcurrentUse(t *testing.T) {
	h := NewHub()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			last := ""
			for j := 0; j < 200; j++ {
				sub := h.Subscribe(last, "s1")
				for _, e := range sub.Missed {
					last = e.ID
				}
				select {
				case e, ok := <-sub.C:
					if ok {
						last = e.ID
					}
				default:
				}
				h.Unsubscribe(sub)
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				h.Publish("s1", ev("message"))
				h.Position("s1")
			}
		}()
	}
	wg.Wait()
	if n := len(h.topics["s1"].subs); n != 0 {
		t.Errorf("%d subscribers left", n)
	}
	if n := len(h.topics["s1"].recent); n != DefaultKeep {
		t.Errorf("%d events kept, want %d", n, DefaultKeep)
	}
}

// A page has one stream for its topics: their events come on one channel, in
// order, and one ID places the client in each when it reconnects.
func TestHub_OneStreamForSeveralTopics(t *testing.T) {
	h := NewHub()
	from := h.Position("s1", "tree:u1")
	h.Publish("tree:u1", ev("changed"))
	h.Publish("s2", ev("elsewhere"))
	h.Publish("s1", ev("turn_started"))
	page := h.Subscribe(from, "s1", "tree:u1")
	if got := fmt.Sprint(types(page.Missed)); page.Stale || got != "[changed turn_started]" {
		t.Errorf("missed %s (stale %v)", got, page.Stale)
	}
	h.Publish("s1", ev("message"))
	h.Publish("tree:u1", ev("changed"))
	if a, b := <-page.C, <-page.C; a.Type != "message" || b.Type != "changed" {
		t.Errorf("live %s, %s", a.Type, b.Type)
	}
	h.Unsubscribe(page)
	if _, open := <-page.C; open || len(h.topics["s1"].subs)+len(h.topics["tree:u1"].subs) != 0 {
		t.Error("the subscriber stays on a topic")
	}

	// Stale as soon as one of the topics lost the client's events.
	h.keep = 1
	last := h.Position()
	h.Publish("tree:u1", ev("changed"))
	h.Publish("tree:u1", ev("changed"))
	if s := h.Subscribe(last, "s1", "tree:u1"); !s.Stale || len(s.Missed) != 0 {
		t.Errorf("one topic past its buffer: %v (stale %v)", types(s.Missed), s.Stale)
	}

	// Dropped as too slow on one topic, it is not sent to on the other.
	slow := h.Subscribe("", "s1", "tree:u1")
	for i := 0; i <= subscriberBuffer; i++ {
		h.Publish("s1", ev("tool_calls"))
	}
	h.Publish("tree:u1", ev("changed")) // no send on the closed channel
	h.Unsubscribe(slow)
}

// A topic created after the client's last event (its first event is the one
// the client missed) replays it: a topic that never had events missed none.
func TestHub_ANewTopicReplaysFromItsStart(t *testing.T) {
	h := NewHub()
	h.Publish("s2", ev("elsewhere"))
	from := h.Position()
	h.Publish("s2", ev("elsewhere"))
	h.Publish("s1", ev("turn_started")) // s1 is created here
	if s := h.Subscribe(from, "s1"); s.Stale || fmt.Sprint(types(s.Missed)) != "[turn_started]" {
		t.Errorf("missed %v (stale %v)", types(s.Missed), s.Stale)
	}
}
