package session

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/workflow"
)

var team = []store.Agent{
	{ID: "default", Name: "Jarvis", Mention: "jarvis"},
	{ID: "smith", Name: "Agent Smith", Mention: "agentSmith"},
	{ID: "code-reviewer", Name: "Reviewer"}, // no mention: its ID
	{ID: "analyst", Name: "Analyst", Mention: "analyst"},
}

// mentions lists the mentions of the agents called, in order.
func mentions(called []workflow.AddressedAgent) []string {
	var out []string
	for _, a := range called {
		out = append(out, a.Mention)
	}
	return out
}

func TestMentionedAgents(t *testing.T) {
	for text, want := range map[string][]string{
		"@jarvis résume, @agentSmith à partir de là juge": {"jarvis", "agentSmith"},
		"@agentsmith d'abord, puis @Jarvis":               {"agentSmith", "jarvis"}, // order of appearance, any case
		"@jarvis, @jarvis encore, puis @JARVIS":           {"jarvis"},               // once each
		"(@code-reviewer) regarde":                        {"code-reviewer"},        // its ID, having no mention
		"@agent résume":                                   nil,                      // the generic name calls no one
		"demande à @default":                              nil,                      // nor an ID, once there is a mention
		"écris à victor@jarvis.fr":                        nil,                      // an email address
		"@jarvisette n'est pas une mention":               nil,
		"@victor tu en penses quoi ?":                     nil, // a member, not an agent
		"jarvis, résume":                                  nil,
		"":                                                nil,
	} {
		called, dropped := mentionedAgents(text, team, maxAgentsPerMessage)
		if fmt.Sprint(mentions(called)) != fmt.Sprint(want) || dropped != nil {
			t.Errorf("mentionedAgents(%q) = %v (dropped %v), want %v", text, mentions(called), dropped, want)
		}
	}
}

// The agent called is the one the turn needs: its ID, name and mention.
func TestMentionedAgents_CarriesTheAgent(t *testing.T) {
	called, _ := mentionedAgents("@agentSmith ?", team, maxAgentsPerMessage)
	if want := (workflow.AddressedAgent{ID: "smith", Name: "Agent Smith", Mention: "agentSmith"}); len(called) != 1 || called[0] != want {
		t.Errorf("called %+v, want %+v", called, want)
	}
}

// Were a mention to be another agent's ID (the store refuses it), the explicit
// mention wins, whatever the order of the agents.
func TestMentionedAgents_AMentionWinsOverAnID(t *testing.T) {
	byID := store.Agent{ID: "smith", Name: "Smith"}
	byMention := store.Agent{ID: "x", Name: "X", Mention: "Smith"}
	for _, agents := range [][]store.Agent{{byID, byMention}, {byMention, byID}} {
		called, _ := mentionedAgents("@smith ?", agents, maxAgentsPerMessage)
		if len(called) != 1 || called[0].ID != "x" {
			t.Errorf("agents %v: called %+v, want x", agents, called)
		}
	}
}

// The live event names the agents that answer: those mentioned, or the
// session's, or the default one when the session's is gone.
func TestAnswering(t *testing.T) {
	two := []workflow.AddressedAgent{{ID: "smith", Name: "Agent Smith"}, {ID: "default", Name: "Jarvis"}}
	for _, c := range []struct {
		called    bool
		mentioned []workflow.AddressedAgent
		session   string
		want      string
	}{
		{false, nil, "default", "[]"},
		{true, two, "default", "[Agent Smith Jarvis]"},
		{true, nil, "code-reviewer", "[Reviewer]"},
		{true, nil, "gone", "[Jarvis]"},
		{true, nil, "", "[Jarvis]"},
	} {
		if got := fmt.Sprint(answering(c.called, c.mentioned, c.session, "default", team)); got != c.want {
			t.Errorf("answering(%v, %v, %q) = %s, want %s", c.called, c.mentioned, c.session, got, c.want)
		}
	}
}

// Each agent runs a full turn: past the cap, the mentions are dropped, and
// reported to be logged.
func TestMentionedAgents_Cap(t *testing.T) {
	called, dropped := mentionedAgents("@analyst @jarvis @agentSmith @code-reviewer @jarvis", team, 3)
	if fmt.Sprint(mentions(called)) != "[analyst jarvis agentSmith]" || fmt.Sprint(dropped) != "[code-reviewer]" {
		t.Errorf("called %v, dropped %v", mentions(called), dropped)
	}
}

func TestAnswered(t *testing.T) {
	smith := []workflow.AddressedAgent{{ID: "smith"}}
	for _, c := range []struct {
		mode      string
		members   int
		mentioned []workflow.AddressedAgent
		want      bool
	}{
		{"auto", 1, nil, true},   // alone: talking to the agent
		{"auto", 2, nil, false},  // shared: talking to each other
		{"auto", 2, smith, true}, // ...unless an agent is called
		{"always", 3, nil, true}, // every message
		{"mention", 1, nil, false},
		{"mention", 1, smith, true}, // a mentioned agent answers whatever the mode
		{"", 1, nil, true},          // an unset mode is auto
	} {
		if got := answered(c.mode, c.members, c.mentioned); got != c.want {
			t.Errorf("answered(%q, %d, %v) = %v, want %v", c.mode, c.members, c.mentioned, got, c.want)
		}
	}
}

// The server resolves the mentions and hands the workflow the agents to run:
// a mentioned agent answers even where the mode would not call the
// session's; with no mention, the mode decides, and the list stays empty.
func TestDeliver_SignalsTheMentionedAgents(t *testing.T) {
	alice := &store.User{ID: "u-alice", Email: "alice@example.com"}
	for _, c := range []struct {
		name, mode, text string
		members          int
		wantCalled       bool
		wantAgents       string
	}{
		{"mention mode, mentioned", store.AgentModeMention, "@agentSmith juge", 1, true, "[agentSmith]"},
		{"mention mode, no mention", store.AgentModeMention, "bonjour", 1, false, ""},
		{"auto shared, two mentioned", store.AgentModeAuto, "@jarvis résume, @agentSmith juge", 2, true, "[jarvis agentSmith]"},
		{"auto shared, no mention", store.AgentModeAuto, "bonjour", 2, false, ""},
		{"auto alone, no mention", store.AgentModeAuto, "bonjour", 1, true, "[]"},
	} {
		t.Run(c.name, func(t *testing.T) {
			st := &memStore{agents: team}
			for i := 0; i < c.members; i++ {
				st.members = append(st.members, store.SessionMember{UserID: fmt.Sprint("u-", i)})
			}
			tc := &fakeTemporal{}
			sess := &store.Session{SessionID: sid, AgentID: "default", AgentMode: c.mode}
			svc := newTest(st, tc)
			called, err := svc.Deliver(context.Background(), sess, alice, c.text)
			if err != nil || called != c.wantCalled {
				t.Fatalf("Deliver = %v, %v; want %v", called, err, c.wantCalled)
			}
			if !c.wantCalled {
				if len(tc.signalStarts) != 0 {
					t.Errorf("signalled %+v, want nothing", tc.signalStarts)
				}
				return
			}
			if len(tc.signalStarts) != 1 {
				t.Fatalf("%d signals, want 1", len(tc.signalStarts))
			}
			msg := tc.signalStarts[0].arg.(workflow.UserMessage)
			if got := fmt.Sprint(mentions(msg.Agents)); got != c.wantAgents {
				t.Errorf("agents %s, want %s", got, c.wantAgents)
			}
			// The members see live who answers.
			ev := svc.hub.(*nopHub).on(sid)
			var data struct {
				Called bool     `json:"agent_called"`
				Agents []string `json:"agents"`
			}
			if len(ev) != 1 || json.Unmarshal(ev[0].Data, &data) != nil || !data.Called || len(data.Agents) == 0 {
				t.Errorf("event %+v, want the agents that answer", ev)
			}
		})
	}
}
