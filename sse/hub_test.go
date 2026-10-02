package sse

import (
	"sync"
	"testing"

	"github.com/victor/temporal-agent/activity"
)

func TestHub_DeliversToSubscribers(t *testing.T) {
	h := NewHub()
	a, b := h.Subscribe("s1"), h.Subscribe("s1")
	other := h.Subscribe("s2")
	h.Publish("s1", activity.SSEEvent{Type: "message"})

	for _, ch := range []chan activity.SSEEvent{a, b} {
		if ev := <-ch; ev.Type != "message" {
			t.Errorf("got %q", ev.Type)
		}
	}
	select {
	case ev := <-other:
		t.Errorf("another session got %q", ev.Type)
	default:
	}

	h.Unsubscribe("s1", a)
	if _, open := <-a; open {
		t.Error("an unsubscribed channel stays open")
	}
	h.Publish("s1", activity.SSEEvent{Type: "again"})
	if ev := <-b; ev.Type != "again" {
		t.Errorf("the remaining subscriber got %q", ev.Type)
	}
}

// A browser closing its stream while an agent answers is the common case:
// publishing and unsubscribing at the same time must never send on a closed
// channel. Run with -race.
func TestHub_PublishAndUnsubscribeConcurrently(t *testing.T) {
	h := NewHub()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				ch := h.Subscribe("s1")
				h.Unsubscribe("s1", ch)
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				h.Publish("s1", activity.SSEEvent{Type: "message"})
			}
		}()
	}
	wg.Wait()
	if len(h.subscribers) != 0 {
		t.Errorf("%d sessions left subscribed", len(h.subscribers))
	}
}
