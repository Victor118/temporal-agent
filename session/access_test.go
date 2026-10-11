package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/victor/temporal-agent/access"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/workflow"
)

// closedTo closes the agents of ids to everyone but holder.
func closedTo(holder string, ids ...string) access.Snapshot {
	var snap access.Snapshot
	for _, id := range ids {
		snap.Rules = append(snap.Rules, access.Rule{Kind: access.KindPattern, Target: "agent_" + id, Users: []string{holder}})
	}
	return snap
}

// userMessage is the last user_message event of the session.
func userMessage(t *testing.T, s *Service) (event struct {
	Called  bool     `json:"agent_called"`
	Agents  []string `json:"agents"`
	Refused []string `json:"refused_agents"`
}) {
	t.Helper()
	for _, ev := range s.hub.(*nopHub).on(sid) {
		if ev.Type == EventUserMessage {
			json.Unmarshal(ev.Data, &event)
		}
	}
	return event
}

// The agents closed to the author are taken out after the list is built:
// the others answer, in order, told their part among themselves only; the
// members are told who will not answer.
func TestDeliver_TakesOutTheClosedAgents(t *testing.T) {
	st := &memStore{members: []store.SessionMember{{UserID: "u-alice"}}, agents: team, rules: closedTo("u-bob", "smith")}
	tc := &fakeTemporal{}
	s := newTest(st, tc)
	alice := &store.User{ID: "u-alice", Email: "alice@example.com"}
	sess := &store.Session{SessionID: sid, AgentID: "default"}

	d, err := s.Deliver(context.Background(), sess, alice, "@agentSmith résume, @jarvis @analyst juge")
	if err != nil || !d.Called || len(d.Refused) != 1 || d.Refused[0].ID != "smith" {
		t.Fatalf("Deliver = %+v, %v", d, err)
	}
	if len(tc.signalStarts) != 1 {
		t.Fatalf("%d signals", len(tc.signalStarts))
	}
	msg := tc.signalStarts[0].arg.(workflow.ParticipantMessage)
	if first, _ := workflow.ParticipantOf(tc.signalStarts[0].id); first != "default" || fmt.Sprint(mentions(msg.Next)) != "[analyst]" {
		t.Errorf("delivered to %s, then %v", first, mentions(msg.Next))
	}
	if msg.Part == nil || len(msg.Part.Agents) != 2 {
		t.Errorf("part %+v, want the two agents that answer", msg.Part)
	}
	if ev := userMessage(t, s); fmt.Sprint(ev.Refused) != "[agentSmith]" || fmt.Sprint(ev.Agents) != "[Jarvis Analyst]" {
		t.Errorf("event %+v", ev)
	}
	if got := RefusedText(d.Refused); got != "@agentSmith ne t'est pas ouvert : ton message est enregistré, sans réponse de sa part." {
		t.Errorf("refusal %q", got)
	}
	s.background.Wait()
}

// A message whose mentions are all closed to its author calls nobody: never
// the session's agent in their place. It is stored all the same: a message
// between the members. So is one the session's agent, closed, would have
// answered.
func TestDeliver_NobodyWhenEveryAgentIsClosed(t *testing.T) {
	for name, c := range map[string]struct {
		text    string
		refused string
	}{
		"mentions closed":       {"@agentSmith juge", "[smith]"},
		"session agent closed":  {"bonjour", "[default]"},
		"mentions and fallback": {"@agentSmith @analyst ?", "[smith analyst]"},
	} {
		t.Run(name, func(t *testing.T) {
			rules := closedTo("u-bob", "smith", "analyst")
			if c.text == "bonjour" {
				rules = closedTo("u-bob", "default")
			}
			st := &memStore{members: []store.SessionMember{{UserID: "u-alice"}}, agents: team, rules: rules}
			tc := &fakeTemporal{}
			s := newTest(st, tc)
			alice := &store.User{ID: "u-alice", Email: "alice@example.com"}
			d, err := s.Deliver(context.Background(), &store.Session{SessionID: sid, AgentID: "default"}, alice, c.text)
			var refused []string
			for _, a := range d.Refused {
				refused = append(refused, a.ID)
			}
			if err != nil || d.Called || fmt.Sprint(refused) != c.refused {
				t.Fatalf("Deliver = %+v, %v", d, err)
			}
			if len(tc.signalStarts) != 0 || len(st.appended) != 1 {
				t.Errorf("signalled %v, stored %d", tc.signalStarts, len(st.appended))
			}
			if ev := userMessage(t, s); ev.Called || len(ev.Agents) != 0 {
				t.Errorf("event %+v", ev)
			}
			s.background.Wait()
		})
	}
}

// The cap is that of the first agent that answers: a closed agent's full
// queue refuses nothing.
func TestDeliver_QueueCapOfTheAgentsLeft(t *testing.T) {
	st := &memStore{members: []store.SessionMember{{UserID: "u-alice"}}, agents: team}
	tc := &fakeTemporal{}
	s := newTest(st, tc)
	now := time.Now()
	s.statuses.now = func() time.Time { return now }
	alice := &store.User{ID: "u-alice", Email: "alice@example.com"}
	sess := &store.Session{SessionID: sid, AgentID: "default"}
	ctx := context.Background()
	for i := range MaxQueued {
		if _, err := s.Deliver(ctx, sess, alice, fmt.Sprint("M", i)); err != nil {
			t.Fatal(err)
		}
	}
	// Closed since: the rules read again past their few seconds.
	st.rules = closedTo("u-bob", "default")
	now = now.Add(accessTTL)
	d, err := s.Deliver(ctx, sess, alice, "@jarvis @analyst ?")
	if err != nil || !d.Called {
		t.Fatalf("Deliver = %+v, %v", d, err)
	}
	if first, _ := workflow.ParticipantOf(tc.signalStarts[len(tc.signalStarts)-1].id); first != "analyst" {
		t.Errorf("delivered to %s", first)
	}
	s.background.Wait()
}

// The first answer of a session on a channel where the member did not
// choose its agent is signed, when it is not the default one.
func TestDeliver_SignsTheFirstAnswerOfAnAgentNotChosen(t *testing.T) {
	for title, want := range map[string]bool{"": true, "Bonjour": false} {
		st := &memStore{members: []store.SessionMember{{UserID: "u-alice"}}, agents: team}
		tc := &fakeTemporal{}
		s := newTest(st, tc)
		sess := &store.Session{SessionID: sid, AgentID: "analyst", Channel: "telegram", ChannelID: "42", Title: title}
		if _, err := s.Deliver(context.Background(), sess, &store.User{ID: "u-alice"}, "bonjour"); err != nil {
			t.Fatal(err)
		}
		if got := tc.signalStarts[0].arg.(workflow.ParticipantMessage).SignReply; got != want {
			t.Errorf("title %q: signed %v, want %v", title, got, want)
		}
		s.background.Wait()
	}
}

// A session opens with an agent open to its member: the one asked for,
// refused if closed; without one, the default agent, or else the first
// open; none open, refused.
func TestOpen_AnAgentOpenToTheMember(t *testing.T) {
	alice := &store.User{ID: "u-alice"}
	cases := []struct {
		name  string
		asked string
		rules access.Snapshot
		want  string
		err   error
	}{
		{"default open", "", access.Snapshot{}, "default", nil},
		{"default closed", "", closedTo("u-bob", "default"), "smith", nil},
		{"asked and open", "analyst", closedTo("u-bob", "default"), "analyst", nil},
		{"asked and closed", "smith", closedTo("u-bob", "smith"), "", ErrAgentNotOpen},
		{"none open", "", access.Snapshot{Rules: []access.Rule{{Kind: access.KindPattern, Target: "agent_*", Users: []string{"u-bob"}}}}, "", ErrAgentNotOpen},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st := &memStore{agents: team, rules: c.rules}
			_, err := newTest(st, &fakeTemporal{}).Open(context.Background(), alice, OpenOptions{AgentID: c.asked})
			if !errors.Is(err, c.err) {
				t.Fatalf("Open: %v, want %v", err, c.err)
			}
			if c.err != nil {
				if st.session != nil {
					t.Errorf("a refused session was created: %+v", st.session)
				}
				return
			}
			if st.session.AgentID != c.want {
				t.Errorf("agent %s, want %s", st.session.AgentID, c.want)
			}
		})
	}
}

// A fork takes its parent's agent: closed to the member who forks, it is
// refused, before anything is created.
func TestFork_RefusedWhenTheAgentIsClosed(t *testing.T) {
	st, tc := parentWithMessages(), &fakeTemporal{}
	st.rules = closedTo("u-bob", "default")
	alice := &store.User{ID: "u-alice", Email: "alice@example.com"}
	if _, err := newTest(st, tc).Fork(context.Background(), sid, 2, "", alice); !errors.Is(err, ErrAgentNotOpen) {
		t.Fatalf("fork: %v", err)
	}
	if st.session.SessionID != sid || len(tc.started) != 0 {
		t.Error("a refused fork was created")
	}
	bob := &store.User{ID: "u-bob", Email: "bob@example.com"}
	if _, err := newTest(st, tc).Fork(context.Background(), sid, 2, "", bob); err != nil {
		t.Errorf("the holder's fork: %v", err)
	}
}

// The agents a member may call: those open to them, in order.
func TestOpenAgents(t *testing.T) {
	st := &memStore{agents: team, rules: closedTo("u-bob", "smith", "analyst")}
	s := newTest(st, &fakeTemporal{})
	var ids []string
	for _, a := range s.OpenAgents(context.Background(), "u-alice", team) {
		ids = append(ids, a.ID)
	}
	if fmt.Sprint(ids) != "[default code-reviewer]" {
		t.Errorf("open to alice %v", ids)
	}
	if len(s.OpenAgents(context.Background(), "u-bob", team)) != len(team) {
		t.Error("an agent closed to its holder")
	}
}
