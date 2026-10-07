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

// The sweep ends a closed task a member stopped as cancelled, waking
// nobody; tells again a stop its workflow never heard; leaves a task whose
// workflow Temporal knows not while its start may still come.
func TestSweepTasks_StoppedAndLate(t *testing.T) {
	stopped, untold, recent := bgTask("c1", bob.ID), bgTask("c2", bob.ID), bgTask("c3", bob.ID)
	sent := time.Now().Add(-time.Hour)
	stopped.CancelledBy, stopped.CancelSentAt = "Alice", &sent
	untold.CancelledBy = "Bob"
	recent.StartedAt = time.Now().Add(-10 * time.Minute)
	st := &memStore{session: &store.Session{SessionID: sid, CreatedBy: "u-alice", AgentID: "jarvis"},
		tasks: []store.BackgroundTask{stopped, untold, recent}}
	tc := &fakeTemporal{
		byType:           map[string][]string{"BackgroundTaskWorkflow": {untold.ID}},
		closed:           map[string]enumspb.WorkflowExecutionStatus{stopped.ID: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED},
		describeNotFound: []string{recent.ID},
	}
	s := newTest(st, tc)
	s.SweepTasks(context.Background())
	if st.tasks[0].State != store.BackgroundCancelled || len(st.appended) != 1 || st.appended[0].Task.CancelledBy != "Alice" || len(tc.signalStarts) != 0 {
		t.Errorf("stopped: %+v, posted %+v, woken %d", st.tasks[0], st.appended, len(tc.signalStarts))
	}
	if !slices.Contains(tc.cancelled, untold.ID) || st.tasks[1].CancelSentAt == nil || slices.Contains(tc.cancelled, stopped.ID) {
		t.Errorf("cancels %v, untold %+v", tc.cancelled, st.tasks[1])
	}
	if st.tasks[2].State != store.BackgroundRunning {
		t.Errorf("a task whose start may still come was ended: %+v", st.tasks[2])
	}
}

// An ended task whose participant was never woken (its waker stopped in
// between) is woken by the sweep, once it is old enough; a failed wake is
// tried again, then given up: its turn ended saying so, its channel told.
func TestSweepTasks_WakesAgain(t *testing.T) {
	ended := func(call string, ago time.Duration) store.BackgroundTask {
		task := bgTask(call, bob.ID)
		at := time.Now().Add(-ago)
		task.State, task.EndedAt, task.ResultMessageID, task.Channel, task.ChannelID = store.BackgroundDone, &at, 9, "", ""
		return task
	}
	fresh, late, old := ended("c1", time.Minute), ended("c2", 10*time.Minute), ended("c3", time.Hour)
	st := &memStore{session: &store.Session{SessionID: sid, CreatedBy: "u-alice", AgentID: "jarvis"}, tasks: []store.BackgroundTask{fresh, late, old}}
	tc := &fakeTemporal{}
	s := newTest(st, tc)
	s.SweepTasks(context.Background())
	if len(tc.signalStarts) != 2 || st.tasks[0].WokenAt != nil || st.tasks[1].WokenAt == nil || st.tasks[2].WokenAt == nil {
		t.Fatalf("woken %d: %+v", len(tc.signalStarts), st.tasks)
	}

	// Temporal refuses: tried again later, given up past the bound.
	late.WokenAt, old.WokenAt = nil, nil
	hub := &nopHub{}
	st = &memStore{session: &store.Session{SessionID: sid, CreatedBy: "u-alice", AgentID: "jarvis"}, tasks: []store.BackgroundTask{late, old}}
	tc = &fakeTemporal{startErr: errors.New("temporal away")}
	s = New(st, tc, hub, Config{WorkflowQueue: "agent"})
	s.SweepTasks(context.Background())
	if st.tasks[0].WokenAt != nil || st.tasks[1].WokenAt == nil {
		t.Errorf("after a failed wake: %+v", st.tasks)
	}
	if len(st.appended) != 1 || store.TurnEndError(st.appended[0]) == "" {
		t.Errorf("the given-up turn's end: %+v", st.appended)
	}
	told := false
	for _, ev := range hub.on(sid) {
		told = told || (ev.Type == activity.EventMessage && strings.Contains(string(ev.Data), "Error processing message"))
	}
	if !told {
		t.Error("the channel was not told")
	}
}

// A stop recorded whose workflow cannot be told now is no failure: the
// sweep tells it.
func TestStopTask_PendingWhenTemporalRefuses(t *testing.T) {
	task := bgTask("c1", bob.ID)
	st := &memStore{tasks: []store.BackgroundTask{task}}
	tc := &fakeTemporal{cancelErr: errors.New("temporal away")}
	err := newTest(st, tc).StopTask(context.Background(), creatorSession(), task.ID, bob)
	if !errors.Is(err, ErrStopPending) || st.tasks[0].CancelledBy != bob.ID || st.tasks[0].CancelSentAt != nil {
		t.Errorf("%v; %+v", err, st.tasks[0])
	}
	if list, _ := st.ListTasksToCancel(context.Background()); len(list) != 1 {
		t.Errorf("not left for the sweep: %+v", list)
	}
}
