package session

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/workflow"
)

// parentWithMessages is a session of two messages to fork from.
func parentWithMessages() *memStore {
	return &memStore{
		session: &store.Session{SessionID: sid, Title: "Plan", AgentID: "default"},
		messages: []store.MessageWithID{
			{ID: 1, Message: store.Message{Role: store.RoleUser, Content: `"spec"`}},
			{ID: 2, Message: store.Message{Role: store.RoleAssistant, Content: `"ok"`}},
		},
	}
}

// A fork opened for a purpose is titled by it, keeps it, and its summary is
// written for it.
func TestFork_Purpose(t *testing.T) {
	st, tc := parentWithMessages(), &fakeTemporal{}
	alice := &store.User{ID: "u-alice", Email: "alice@example.com"}
	f, err := newTest(st, tc).Fork(context.Background(), sid, 2, "  Implémenter l'export CSV\nselon la spec  ", alice)
	if err != nil {
		t.Fatal(err)
	}
	if f.Title != "Implémenter l'export CSV" || f.ForkPurpose != "Implémenter l'export CSV\nselon la spec" || st.session.ForkPurpose != f.ForkPurpose {
		t.Errorf("fork %+v", f)
	}
	if len(tc.inputs) != 1 {
		t.Fatalf("workflows started %v", tc.started)
	}
	in, ok := tc.inputs[0].(workflow.ForkSessionInput)
	if !ok || in.Purpose != f.ForkPurpose || in.ParentSessionID != sid || in.UpToMessageID != 2 || in.ForkSessionID != f.SessionID {
		t.Errorf("summary input %+v", tc.inputs[0])
	}
}

// Without a purpose, the fork is titled after its parent, as before; a
// purpose too long for one is refused, before anything is created.
func TestFork_WithoutOrWithTooLongAPurpose(t *testing.T) {
	alice := &store.User{ID: "u-alice", Email: "alice@example.com"}
	st, tc := parentWithMessages(), &fakeTemporal{}
	f, err := newTest(st, tc).Fork(context.Background(), sid, 2, "   ", alice)
	if err != nil || f.Title != "Fork : Plan" || f.ForkPurpose != "" {
		t.Errorf("fork %+v, %v", f, err)
	}

	st, tc = parentWithMessages(), &fakeTemporal{}
	if _, err := newTest(st, tc).Fork(context.Background(), sid, 2, strings.Repeat("é", MaxPurposeRunes+1), alice); !errors.Is(err, ErrPurposeTooLong) {
		t.Errorf("a purpose too long: %v", err)
	}
	if st.session.SessionID != sid || len(tc.started) != 0 {
		t.Error("a refused fork was created")
	}
	// A long first line is cut, as any title.
	if got := forkTitle("Plan", strings.Repeat("a", 100)); got != strings.Repeat("a", maxTitleRunes)+"..." {
		t.Errorf("title %q", got)
	}
}
