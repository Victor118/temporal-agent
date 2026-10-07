package session

import (
	"encoding/json"
	"slices"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/workflow"
)

// The hub's topics: a session's events go under its ID; every other topic is
// named "<kind>:<id>" (a user's tree, a user's notifications).

// IsSessionTopic reports whether a hub topic is a session's: its ID, a
// canonical UUID. No other topic is one, whatever its name.
func IsSessionTopic(topic string) bool { return checkSessionID(topic) == nil }

// TreeTopic is the hub topic of a user's tree: it rings when one of their
// sessions changes in a way the tree shows (a turn, a question, a new
// message or title, a fork, a member).
func TreeTopic(userID string) string { return "tree:" + userID }

// EventTreeChanged is the event of a tree topic.
const EventTreeChanged = "changed"

// EventUserMessage is a member's message, published as soon as it is stored.
const EventUserMessage = "user_message"

// EventMemberLeft is published on a session's topic when members are out of
// it: one left or was removed, or the session was deleted (all of them). Its
// data lists them ({"user_ids": [...]}). A stream relaying the session checks
// at once that its user still is a member.
const EventMemberLeft = "member_left"

// EventSessionGone is the last event of a session's stream whose user is no
// member of it (any more), sent to that stream alone and without an ID: it is
// never published, nor replayed. The page leaves the session on it.
const EventSessionGone = "session_gone"

// StateEvents change what a session is doing, or a fork's state: its status,
// its row in the trees, a question, a summary, a report. The server learns
// from them (Observe): the cached statuses go, the members' trees ring.
var StateEvents = []string{
	workflow.EventTurnStarted, workflow.EventTurnDone, activity.EventAskUser,
	workflow.EventForkReady, workflow.EventForkFailed,
	workflow.EventForkReport, workflow.EventForkReported, workflow.EventForkReportFailed,
	workflow.EventTaskResult,
}

// ThreadEvents are the session's events after which its thread shows
// something new: its state, a message, a tool call, what the turn waits for
// (a notice, shown on the working line: not a state event, it changes no
// status), files a turn published.
var ThreadEvents = slices.Concat(StateEvents, []string{EventUserMessage, activity.EventMessage, activity.EventToolCalls, activity.EventNotice, activity.EventFilePublished})

// ReportEvents are the fork's events after which its report section may
// change: its state, a message (there is something new to report).
var ReportEvents = slices.Concat(StateEvents, []string{EventUserMessage, activity.EventMessage})

// AgentsEvents are the session's events after which its Agents panel may
// change: a turn, a question or a task's end (StateEvents), a message
// delivered (a participant's queue), what a turn or a task waits for (a
// notice), a task started.
var AgentsEvents = slices.Concat(StateEvents, []string{EventUserMessage, activity.EventNotice, workflow.EventTaskStarted})

// publishMembersLeft tells the session's streams that these users are out
// of it.
func (s *Service) publishMembersLeft(sessionID string, userIDs ...string) {
	data, _ := json.Marshal(map[string][]string{"user_ids": userIDs})
	s.hub.Publish(sessionID, activity.SSEEvent{Type: EventMemberLeft, Data: data})
}
