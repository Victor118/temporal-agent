package session

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/workflow"
)

// bgTask is a task of Jarvis's in the session, asked by userID.
func bgTask(call, userID string) store.BackgroundTask {
	return store.BackgroundTask{ID: sid + ":p:jarvis:m3:bg:" + call, SessionID: sid, Participant: "jarvis", UserID: userID, UserName: "Bob",
		Tool: "analyze_repo", TurnKey: "m3.jarvis", CallID: call, Channel: "telegram", ChannelID: "42",
		State: store.BackgroundRunning, StartedAt: time.Now().Add(-time.Hour)}
}

// Who asked for a task may stop it, and the session's creator; another
// member may not. The stop names who stopped it, then cancels its workflow.
func TestStopTask_Rights(t *testing.T) {
	task := bgTask("c1", bob.ID)
	for _, c := range []struct {
		name string
		me   *store.User
		want error
	}{
		{"who asked", bob, nil},
		{"the creator", alice, nil},
		{"another member", &store.User{ID: "u-carol", DisplayName: "Carol"}, ErrStopNotAllowed},
	} {
		st := &memStore{tasks: []store.BackgroundTask{task}}
		tc := &fakeTemporal{}
		err := newTest(st, tc).StopTask(context.Background(), creatorSession(), task.ID, c.me)
		if !errors.Is(err, c.want) {
			t.Errorf("%s: %v", c.name, err)
		}
		stopped := c.want == nil
		_, named := st.cancelledBy[task.ID]
		if stopped != (len(tc.cancelled) == 1) || stopped != named {
			t.Errorf("%s: cancelled %v, by %v", c.name, tc.cancelled, st.cancelledBy)
		}
	}

	// Not the session's, over, or closed already.
	other := bgTask("c2", bob.ID)
	other.SessionID = "another"
	over := bgTask("c3", bob.ID)
	over.State = store.BackgroundDone
	st := &memStore{tasks: []store.BackgroundTask{other, over, bgTask("c4", bob.ID)}}
	tc := &fakeTemporal{cancelErr: serviceerror.NewNotFound("closed")}
	s := newTest(st, tc)
	for id, want := range map[string]error{other.ID: ErrNoSuchTask, over.ID: ErrTaskOver, "nope": ErrNoSuchTask, bgTask("c4", "").ID: ErrTaskOver} {
		if err := s.StopTask(context.Background(), creatorSession(), id, alice); !errors.Is(err, want) {
			t.Errorf("%s: %v, want %v", id, err, want)
		}
	}
}

// Deleting the session, or its last member leaving, ends its background
// tasks before the session goes: those the visibility lists and those the
// store says run.
func TestDeleteAndLeave_EndTheTasks(t *testing.T) {
	listed := sid + ":p:jarvis:m3:bg:c1"
	stored := bgTask("c2", bob.ID)
	for _, leave := range []bool{false, true} {
		st := &memStore{session: &store.Session{SessionID: sid, CreatedBy: "u-alice"}, tasks: []store.BackgroundTask{stored}}
		if !leave {
			st.members = []store.SessionMember{{UserID: "u-alice"}}
		}
		tc := &fakeTemporal{byType: map[string][]string{"BackgroundTaskWorkflow": {listed}}}
		var endedBefore []string
		st.onDelete = func() { endedBefore = slices.Clone(tc.terminated) }
		s := newTest(st, tc)
		var err error
		if leave {
			err = s.Leave(context.Background(), sid, "u-alice")
		} else {
			err = s.Delete(context.Background(), sid, "u-alice")
		}
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Contains(endedBefore, listed) || !slices.Contains(endedBefore, stored.ID) {
			t.Errorf("leave %v: ended %v before the deletion", leave, endedBefore)
		}
		s.background.Wait()
	}
}

// The Agents panel shows each agent's tasks under it: one with nothing
// else running too, idle. A task's notice is its own, never its agent's
// line; its question does not make its agent wait.
func TestParticipants_ShowTheTasks(t *testing.T) {
	task := bgTask("c1", bob.ID)
	smithTask := task
	smithTask.ID, smithTask.Participant = sid+":p:smith:m5:bg:c9", "smith"
	st := &memStore{tasks: []store.BackgroundTask{task, smithTask}}
	tc := &fakeTemporal{byType: map[string][]string{
		"ParticipantWorkflow": {sid + ":p:jarvis"},
		"AskUserWorkflow":     {task.ID + ":tool:agent_x:c1:tool:ask_user:q1"},
	}, states: map[string]interface{}{sid + ":p:jarvis": answering("jarvis", 7, alice.ID)}}
	s := newTest(st, tc)
	notice, _ := json.Marshal(map[string]string{"text": "Analyse sur la machine « pc »", "participant": "jarvis", "task": task.ID})
	s.Observe(sid, activity.SSEEvent{Type: activity.EventNotice, Data: notice})

	ps := s.Participants(context.Background(), sid)
	if len(ps) != 2 || ps[0].Participant != "jarvis" || ps[1].Participant != "smith" {
		t.Fatalf("participants %+v", ps)
	}
	j := ps[0]
	if j.Waiting || j.Note != "" || len(j.Tasks) != 1 {
		t.Fatalf("jarvis %+v", j)
	}
	if tk := j.Tasks[0]; tk.ID != task.ID || tk.Note != "Analyse sur la machine « pc »" || !tk.Waiting || tk.UserID != bob.ID {
		t.Errorf("jarvis's task %+v", tk)
	}
	if sm := ps[1]; sm.Working || len(sm.Tasks) != 1 {
		t.Errorf("smith %+v", sm)
	}
	// The session waits for an answer all the same.
	if got := s.Statuses(context.Background())[sid]; got != StatusWaiting {
		t.Errorf("status %s", got)
	}

	// Its end takes its note off.
	ended, _ := json.Marshal(map[string]string{"task": task.ID, "agent_id": "jarvis"})
	s.Observe(sid, activity.SSEEvent{Type: workflow.EventTaskResult, Data: ended})
	if note := s.turns.taskNote(sid, task.ID); note != "" {
		t.Errorf("note after the end: %q", note)
	}
	s.background.Wait()
}

// The sweep ends a task whose workflow closed without ending it: failed,
// its message posted, its participant woken on the launching turn's
// channel. A task still running, or one Temporal cannot tell of, is left.
func TestSweepTasks(t *testing.T) {
	closed, running, unknown, gone := bgTask("c1", bob.ID), bgTask("c2", bob.ID), bgTask("c3", bob.ID), bgTask("c4", bob.ID)
	st := &memStore{session: &store.Session{SessionID: sid, CreatedBy: "u-alice", AgentID: "jarvis", Channel: "web"},
		tasks: []store.BackgroundTask{closed, running, unknown, gone}}
	tc := &fakeTemporal{
		byType:    map[string][]string{"BackgroundTaskWorkflow": {running.ID}},
		closed:    map[string]enumspb.WorkflowExecutionStatus{closed.ID: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED},
		queryErrs: map[string]error{},
	}
	// unknown: described with an error (not running, says the fake);
	// gone: Temporal knows it no more.
	tc.running = []string{unknown.ID}
	tc.describeNotFound = []string{gone.ID}
	hub := &nopHub{}
	s := New(st, tc, hub, Config{WorkflowQueue: "agent", DefaultAgentID: "default"})
	s.SweepTasks(context.Background())

	ended := map[string]bool{}
	for _, task := range st.tasks {
		ended[task.ID] = task.State != store.BackgroundRunning
	}
	if !ended[closed.ID] || ended[running.ID] || ended[unknown.ID] || !ended[gone.ID] {
		t.Errorf("ended %v", ended)
	}
	if len(st.appended) != 2 || st.appended[0].Kind != store.KindTaskResult || !strings.Contains(st.appended[0].Content, "stopped without a result") {
		t.Fatalf("posted %+v", st.appended)
	}
	if len(tc.signalStarts) != 2 {
		t.Fatalf("woken %+v", tc.signalStarts)
	}
	w := tc.signalStarts[0]
	msg := w.arg.(workflow.ParticipantMessage)
	if w.id != sid+":p:jarvis" || msg.MessageID != 1 || msg.UserID != bob.ID || msg.Channel != "telegram" || msg.SignReply || w.options.TaskQueue != "agent" {
		t.Errorf("wake %+v %+v", w, msg)
	}
	if len(hub.on(sid)) != 2 {
		t.Errorf("events %+v", hub.on(sid))
	}

	// Again: nothing more.
	s.SweepTasks(context.Background())
	if len(st.appended) != 2 || len(tc.signalStarts) != 2 {
		t.Errorf("swept twice: %d messages, %d wakes", len(st.appended), len(tc.signalStarts))
	}
}
