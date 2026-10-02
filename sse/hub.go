package sse

import (
	"sync"

	"github.com/victor/temporal-agent/activity"
)

// Hub is an in-process pub/sub for SSE events, keyed by session ID.
type Hub struct {
	mu          sync.RWMutex
	subscribers map[string][]chan activity.SSEEvent
}

func NewHub() *Hub {
	return &Hub{
		subscribers: make(map[string][]chan activity.SSEEvent),
	}
}

// Subscribe returns a channel that receives events for the given session.
// The caller must call Unsubscribe when done.
func (h *Hub) Subscribe(sessionID string) chan activity.SSEEvent {
	ch := make(chan activity.SSEEvent, 64)
	h.mu.Lock()
	h.subscribers[sessionID] = append(h.subscribers[sessionID], ch)
	h.mu.Unlock()
	return ch
}

// Unsubscribe removes a channel from the session's subscriber list and closes
// it. It takes the write lock, which waits for any Publish still sending: a
// channel is never closed while a send to it may be in flight.
func (h *Hub) Unsubscribe(sessionID string, ch chan activity.SSEEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()

	subs := h.subscribers[sessionID]
	for i, sub := range subs {
		if sub == ch {
			h.subscribers[sessionID] = append(subs[:i], subs[i+1:]...)
			close(ch)
			break
		}
	}
	if len(h.subscribers[sessionID]) == 0 {
		delete(h.subscribers, sessionID)
	}
}

// Publish sends an event to all subscribers of the given session. The read
// lock is held for the whole loop: sending after releasing it raced with
// Unsubscribe, and a send on the channel it had just closed panicked — taking
// the server down, since the internal router has no recoverer. The sends never
// block, so holding the lock costs nothing.
func (h *Hub) Publish(sessionID string, event activity.SSEEvent) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, ch := range h.subscribers[sessionID] {
		select {
		case ch <- event:
		default:
			// Drop if subscriber is slow
		}
	}
}
