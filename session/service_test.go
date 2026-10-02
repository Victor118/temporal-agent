package session

import (
	"context"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/victor/temporal-agent/store"
)

const sid = "6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b"

func newTest(st *memStore, tc *fakeTemporal) *Service {
	return New(st, tc, &nopHub{}, Config{WorkflowQueue: "agent", DefaultAgentID: "default"})
}

// A title is cut in characters: cut in bytes, an accented letter can be split
// and Postgres refuses the string.
func TestSetTitleFrom_CutsOnCharacters(t *testing.T) {
	st := &memStore{}
	newTest(st, &fakeTemporal{}).setTitleFrom(sid, strings.Repeat("é", 100))
	if !utf8.ValidString(st.title) || st.title != strings.Repeat("é", maxTitleRunes)+"..." {
		t.Errorf("title %q", st.title)
	}
}

// A session ID goes into a visibility query between quotes: anything but a
// canonical UUID is refused, whatever the route checked before.
func TestVisibilityQueriesTakeOnlyUUIDs(t *testing.T) {
	for _, id := range []string{"", "s1", "x' OR WorkflowId STARTS_WITH '", "{" + sid + "}", "urn:uuid:" + sid, strings.ToUpper(sid)} {
		if q, err := runningSessionQuery(id); err == nil {
			t.Errorf("runningSessionQuery(%q) = %q", id, q)
		}
		if q, err := pendingQuestionsQuery(id); err == nil {
			t.Errorf("pendingQuestionsQuery(%q) = %q", id, q)
		}
	}
	if _, err := pendingQuestionsQuery(sid); err != nil {
		t.Error(err)
	}

	tc := &fakeTemporal{running: []string{sid + "-tool-ask_user-1"}}
	if newTest(&memStore{}, tc).AnswerPending(context.Background(), "x' OR '1'='1", "yes") || len(tc.lists) != 0 {
		t.Errorf("a forged session ID reached Temporal: %v", tc.lists)
	}
}

// An answer from a channel without buttons reaches a question a sub-agent
// asked: they are found by type, not by the session agent's own ID prefix.
func TestAnswerPending_FindsSubAgentQuestions(t *testing.T) {
	question := sid + "-tool-agent_analyst-c1-tool-ask_user-c2"
	tc := &fakeTemporal{running: []string{question}}
	if !newTest(&memStore{}, tc).AnswerPending(context.Background(), sid, "yes") {
		t.Fatal("the answer was not delivered")
	}
	if len(tc.lists) != 1 || !strings.Contains(tc.lists[0], "WorkflowType = 'AskUserWorkflow'") || !strings.Contains(tc.lists[0], "STARTS_WITH '"+sid+"-'") {
		t.Errorf("query %v", tc.lists)
	}
	if len(tc.signals) != 1 || tc.signals[0] != question {
		t.Errorf("signalled %v", tc.signals)
	}
}

// Answering through a session reaches that session's questions only.
func TestAnswer_OwnQuestionsOnly(t *testing.T) {
	tc := &fakeTemporal{}
	s := newTest(&memStore{}, tc)
	if err := s.Answer(context.Background(), sid, "other-tool-ask_user-1", "yes"); !errors.Is(err, ErrForeignQuestion) {
		t.Errorf("another session's question: %v", err)
	}
	if err := s.Answer(context.Background(), sid, sid+"-tool-ask_user-1", "  "); !errors.Is(err, ErrEmptyAnswer) {
		t.Errorf("an empty answer: %v", err)
	}
	if err := s.Answer(context.Background(), sid, sid+"-tool-ask_user-1", "yes"); err != nil || len(tc.signals) != 1 {
		t.Errorf("own question: %v, %v", err, tc.signals)
	}
}

// Every tab refreshes the tree: the states are read from Temporal once for
// all of them, and again after an action changes them.
func TestStatuses_SharedForAFewSeconds(t *testing.T) {
	tc := &fakeTemporal{running: []string{sid + "-turn-1"}}
	s := newTest(&memStore{}, tc)
	for i := 0; i < 10; i++ {
		s.Statuses(context.Background())
	}
	if len(tc.lists) != 4 {
		t.Errorf("%d visibility queries for 10 reads, want 4", len(tc.lists))
	}
	// The fake answers every query with the same workflow: the strongest
	// state, a question waiting, wins.
	if got := s.Statuses(context.Background())[sid]; got != StatusWaiting {
		t.Errorf("status %q, want waiting", got)
	}
	s.statuses.invalidate()
	s.Statuses(context.Background())
	if len(tc.lists) != 8 {
		t.Errorf("%d visibility queries after an invalidation, want 8", len(tc.lists))
	}
}

// An empty message would poison the session; a fork takes none before its
// summary.
func TestDeliver_Refusals(t *testing.T) {
	st := &memStore{members: []store.SessionMember{{UserID: "u-alice"}}}
	s := newTest(st, &fakeTemporal{})
	alice := &store.User{ID: "u-alice", Email: "alice@example.com"}
	sess := &store.Session{SessionID: sid, AgentID: "default"}

	if _, err := s.Deliver(context.Background(), sess, alice, " \n"); !errors.Is(err, ErrEmptyMessage) {
		t.Errorf("empty message: %v", err)
	}
	fork := &store.Session{SessionID: sid, AgentID: "default", ForkedAtMessageID: 3}
	// No summary yet and its workflow is not running: failed, not pending,
	// so the message goes through.
	if _, err := s.Deliver(context.Background(), fork, alice, "hello"); err != nil {
		t.Errorf("fork with a failed summary: %v", err)
	}
	if len(st.appended) != 1 {
		t.Errorf("%d messages stored, want 1", len(st.appended))
	}
}
