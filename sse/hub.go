// Package sse is the server's in-process pub/sub for the events the pages
// listen to, by topic: a session, a user's notifications, a user's tree.
//
// Every event gets an ID, and each topic keeps its latest events: a client
// that reconnects with the ID of the last event it got is sent the ones it
// missed, or told to reload when they are gone.
package sse

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/victor/temporal-agent/activity"
)

// Defaults of a hub.
const (
	// DefaultKeep is how many events a topic keeps for the clients that
	// reconnect.
	DefaultKeep = 100
	// DefaultIdle is how long a topic nobody listens to keeps its events.
	DefaultIdle = 10 * time.Minute
	// subscriberBuffer is how many events a subscriber may lag behind before
	// it is dropped.
	subscriberBuffer = 64
)

// Event is an event as a topic's subscribers get it: with its ID, which a
// client sends back when it reconnects.
type Event struct {
	ID string
	activity.SSEEvent
}

// Hub is the pub/sub. Build it with NewHub.
//
// An event ID is "<epoch>-<seq>": the epoch names this hub (one process),
// so an ID from before a restart is known as stale; seq counts the events of
// all topics, so it orders them and tells the ones a topic no longer keeps.
type Hub struct {
	epoch string
	keep  int
	idle  time.Duration
	now   func() time.Time

	mu        sync.Mutex
	seq       uint64
	topics    map[string]*topic
	swept     time.Time
	observers []func(topic string, ev activity.SSEEvent)
}

// topic is what a hub holds for one topic.
type topic struct {
	subs []chan Event
	// recent are its latest events, oldest first, at most keep.
	recent []Event
	seqs   []uint64 // the seq of each of recent
	// floor: the topic knows every event of its own after this seq. Its
	// events up to it may have been dropped, or predate the topic.
	floor uint64
	// used is when it last published or lost a subscriber.
	used time.Time
}

// NewHub returns a hub keeping DefaultKeep events per topic.
func NewHub() *Hub {
	return &Hub{
		epoch:  strconv.FormatInt(time.Now().UnixNano(), 36),
		keep:   DefaultKeep,
		idle:   DefaultIdle,
		now:    time.Now,
		topics: map[string]*topic{},
	}
}

// Observe adds f to the functions every event goes through before it is
// published: the server learns from them (a session's turn state). f runs
// on the publisher's goroutine, without the hub's lock: it may publish.
// Set the observers before publishing.
func (h *Hub) Observe(f func(topic string, ev activity.SSEEvent)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.observers = append(h.observers, f)
}

// Publish sends an event to the topic's subscribers, and keeps it for the
// ones that reconnect. A subscriber too slow to take it is dropped: its
// channel closes, its client reconnects and is sent what it missed. Sending
// never blocks, so holding the lock costs nothing; sending after releasing
// it would race with Unsubscribe, which closes the channel.
func (h *Hub) Publish(name string, event activity.SSEEvent) {
	h.mu.Lock()
	observers := h.observers
	h.mu.Unlock()
	for _, observe := range observers {
		observe(name, event)
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	h.sweep()
	t := h.topic(name)
	h.seq++
	ev := Event{ID: h.id(h.seq), SSEEvent: event}
	if len(t.recent) == h.keep {
		t.floor = t.seqs[0]
		t.recent, t.seqs = t.recent[1:], t.seqs[1:]
	}
	t.recent, t.seqs = append(t.recent, ev), append(t.seqs, h.seq)
	t.used = h.now()

	kept := t.subs[:0]
	for _, ch := range t.subs {
		select {
		case ch <- ev:
			kept = append(kept, ch)
		default:
			close(ch)
		}
	}
	clear(t.subs[len(kept):])
	t.subs = kept
}

// Subscription is a subscriber's start.
type Subscription struct {
	C <-chan Event
	// Missed are the events published after the client's last one, which it
	// did not get: they come before anything on C.
	Missed []Event
	// Stale: the client's last event is unknown here (another epoch, or
	// older than what the topic keeps). It must reload what it shows.
	Stale bool
	// At is the ID of the last event published before the subscription:
	// where the client stands once it has reloaded.
	At string

	ch chan Event
}

// Subscribe starts a subscriber of the topic. lastID is the ID of the last
// event the client got, or the position its page was rendered at (Position);
// empty for a client that missed nothing. The caller must Unsubscribe.
func (h *Hub) Subscribe(name, lastID string) *Subscription {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sweep()
	t := h.topic(name)
	ch := make(chan Event, subscriberBuffer)
	t.subs = append(t.subs, ch)
	sub := &Subscription{C: ch, ch: ch, At: h.id(h.seq)}
	if lastID == "" {
		return sub
	}
	last, ok := h.parse(lastID)
	if !ok || last < t.floor {
		sub.Stale = true
		return sub
	}
	for i, seq := range t.seqs {
		if seq > last {
			sub.Missed = append(sub.Missed, t.recent[i:]...)
			break
		}
	}
	return sub
}

// Unsubscribe ends a subscriber and closes its channel, unless a Publish it
// lagged behind has closed it already.
func (h *Hub) Unsubscribe(name string, sub *Subscription) {
	h.mu.Lock()
	defer h.mu.Unlock()
	t, ok := h.topics[name]
	if !ok {
		return
	}
	for i, ch := range t.subs {
		if ch == sub.ch {
			t.subs = append(t.subs[:i], t.subs[i+1:]...)
			close(ch)
			break
		}
	}
	t.used = h.now()
}

// Position is the ID of the last event published: a page rendered now has
// seen every event up to it, and its stream starts from there.
func (h *Hub) Position(name string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sweep()
	h.topic(name).used = h.now()
	return h.id(h.seq)
}

// topic returns the topic, created knowing every event from now on.
func (h *Hub) topic(name string) *topic {
	t, ok := h.topics[name]
	if !ok {
		t = &topic{floor: h.seq, used: h.now()}
		h.topics[name] = t
	}
	return t
}

// sweep drops the topics nobody listens to that have been idle for long, at
// most once a minute: a client coming back after that reloads.
func (h *Hub) sweep() {
	now := h.now()
	if now.Sub(h.swept) < time.Minute {
		return
	}
	h.swept = now
	for name, t := range h.topics {
		if len(t.subs) == 0 && now.Sub(t.used) >= h.idle {
			delete(h.topics, name)
		}
	}
}

func (h *Hub) id(seq uint64) string { return fmt.Sprintf("%s-%d", h.epoch, seq) }

// parse returns the seq of an ID of this hub; false for another epoch's, or
// one this hub never gave.
func (h *Hub) parse(id string) (uint64, bool) {
	epoch, seq, ok := strings.Cut(id, "-")
	if !ok || epoch != h.epoch {
		return 0, false
	}
	n, err := strconv.ParseUint(seq, 10, 64)
	return n, err == nil && n <= h.seq
}
