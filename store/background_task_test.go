package store

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestTaskOfWorkflow(t *testing.T) {
	task := BackgroundTaskID("s1:p:jarvis:m12", "toolu_1")
	if task != "s1:p:jarvis:m12:bg:toolu_1" {
		t.Fatalf("task ID %q", task)
	}
	for id, want := range map[string]string{
		task:                                  task,
		task + ":tool:analyze_repo:toolu_1":   task,
		task + ":tool:agent_x:toolu_1:tool:a": task,
		"s1:p:jarvis:m12":                     "",
		"s1:p:jarvis:m12:tool:ask_user:t":     "",
		"s1:p:jarvis:m12:bg:":                 "",
	} {
		got, ok := TaskOfWorkflow(id)
		if got != want || ok != (want != "") {
			t.Errorf("TaskOfWorkflow(%q) = %q, %v; want %q", id, got, ok, want)
		}
	}
	// A task's end is no turn's message, whatever its call's ID.
	for _, call := range []string{"toolu_1", "7", "0-1"} {
		if turn, ok := TurnOf(TaskResultKey(BackgroundTaskID("s1:p:jarvis:m12", call))); ok {
			t.Errorf("call %q: its end read as turn %q's", call, turn)
		}
	}
}

func TestBackgroundTasks(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	clean := func() {
		s.db.Exec("DELETE FROM messages WHERE session_id LIKE 'zz-bg-%'")
		s.db.Exec("DELETE FROM sessions WHERE session_id LIKE 'zz-bg-%'")
		s.db.Exec("DELETE FROM users WHERE id LIKE 'zz-bg-%'")
	}
	clean()
	t.Cleanup(clean)
	if err := s.CreateUser(ctx, User{ID: "zz-bg-alice", Email: "zz-bg-alice@example.com", Role: UserRoleStandard, PasswordHash: "h"}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateSession(ctx, Session{SessionID: "zz-bg-s1", CreatedBy: "zz-bg-alice", Channel: "web"}); err != nil {
		t.Fatal(err)
	}
	task := func(n string) BackgroundTask {
		return BackgroundTask{ID: "zz-bg-s1:p:jarvis:m1:bg:" + n, SessionID: "zz-bg-s1", Participant: "jarvis", UserID: "zz-bg-alice",
			UserName: "Alice", Tool: "agent_smith", Summary: "review", TurnKey: "m1.jarvis", CallID: n, Channel: "telegram", ChannelID: "42"}
	}

	// Three at most per participant, counted once each, even registered
	// again; registered together, they cannot all pass.
	var wg sync.WaitGroup
	errs := make([]error, 5)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = s.RegisterTask(ctx, task(string(rune('a'+i))), 3)
		}()
	}
	wg.Wait()
	ok, refused := 0, 0
	for _, err := range errs {
		switch {
		case err == nil:
			ok++
		case errors.Is(err, ErrTooManyTasks):
			refused++
		default:
			t.Fatal(err)
		}
	}
	if ok != 3 || refused != 2 {
		t.Fatalf("registered %d, refused %d", ok, refused)
	}
	running, err := s.ListRunningTasks(ctx, "zz-bg-s1", "jarvis")
	if err != nil || len(running) != 3 {
		t.Fatalf("running: %d %v", len(running), err)
	}
	first := running[0]
	if err := s.RegisterTask(ctx, first, 3); err != nil {
		t.Errorf("registered again: %v", err)
	}
	if other := task("z"); true {
		other.Participant = "smith"
		if err := s.RegisterTask(ctx, other, 3); err != nil {
			t.Errorf("another participant: %v", err)
		}
	}
	if all, _ := s.ListRunningTasks(ctx, "zz-bg-s1", ""); len(all) != 4 {
		t.Errorf("all the session's: %d", len(all))
	}
	gone := task("x")
	gone.SessionID = "zz-bg-nope"
	if err := s.RegisterTask(ctx, gone, 3); !errors.Is(err, ErrTaskSessionGone) {
		t.Errorf("no session: %v", err)
	}
	if got, err := s.GetTask(ctx, first.ID); err != nil || got == nil || got.Channel != "telegram" || got.State != BackgroundRunning || got.StartedAt.IsZero() {
		t.Fatalf("get: %+v %v", got, err)
	}

	// Instructions: on a running task of this participant, up to max.
	f := TaskFollowUp{Text: "then implement it", UserID: "zz-bg-alice", UserName: "Alice", At: time.Now()}
	if err := s.AddTaskFollowUp(ctx, "zz-bg-s1", "jarvis", first.ID, f, 2); err != nil {
		t.Fatal(err)
	}
	if err := s.AddTaskFollowUp(ctx, "zz-bg-s1", "jarvis", first.ID, f, 1); !errors.Is(err, ErrTooManyFollowUps) {
		t.Errorf("past max: %v", err)
	}
	if err := s.AddTaskFollowUp(ctx, "zz-bg-s1", "smith", first.ID, f, 2); !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("another participant's: %v", err)
	}
	if err := s.SetTaskCancelledBy(ctx, first.ID, "Alice"); err != nil {
		t.Fatal(err)
	}
	// Stopped, its workflow not told yet: the sweep's to tell.
	if stopped, err := s.ListTasksToCancel(ctx); err != nil || len(stopped) != 1 || stopped[0].ID != first.ID {
		t.Fatalf("to cancel: %+v %v", stopped, err)
	}
	if err := s.SetTaskCancelSent(ctx, first.ID); err != nil {
		t.Fatal(err)
	}
	if stopped, _ := s.ListTasksToCancel(ctx); len(stopped) != 0 {
		t.Errorf("told, still to cancel: %+v", stopped)
	}

	// The first to end a task writes its message and its state; a second
	// writer does nothing; the first again (a retry) finds it its own.
	build := func(t BackgroundTask) Message {
		b, _ := json.Marshal(t.State + " by " + t.EndedBy)
		ref := t.Ref()
		return Message{Role: RoleUser, Kind: KindTaskResult, UserID: t.UserID, AgentID: t.Participant, Content: string(b), Task: &ref}
	}
	end, err := s.EndTask(ctx, first.ID, TaskEndedByTask, BackgroundDone, build)
	if err != nil || !end.Mine || end.Gone || end.MessageID == 0 || end.Task.State != BackgroundDone {
		t.Fatalf("end: %+v %v", end, err)
	}
	// Stopped too late: it ended done, cancelled by nobody.
	if len(end.Task.FollowUps) != 1 || end.Task.CancelledBy != "" || end.Task.WokenAt != nil {
		t.Errorf("its instructions and canceller: %+v", end.Task)
	}
	if got, _ := s.GetTask(ctx, first.ID); got.CancelledBy != "" {
		t.Errorf("stored canceller %q", got.CancelledBy)
	}
	// Ended, not woken: the sweep's to wake, once it is old enough.
	if late, err := s.ListTasksToWake(ctx, time.Now().Add(time.Minute)); err != nil || len(late) != 1 || late[0].ID != first.ID {
		t.Errorf("to wake: %+v %v", late, err)
	}
	if late, _ := s.ListTasksToWake(ctx, time.Now().Add(-time.Minute)); len(late) != 0 {
		t.Errorf("to wake, too recent: %+v", late)
	}
	if err := s.SetTaskWoken(ctx, first.ID); err != nil {
		t.Fatal(err)
	}
	if late, _ := s.ListTasksToWake(ctx, time.Now().Add(time.Minute)); len(late) != 0 {
		t.Errorf("woken, still to wake: %+v", late)
	}
	again, err := s.EndTask(ctx, first.ID, TaskEndedByTask, BackgroundFailed, build)
	if err != nil || !again.Mine || again.MessageID != end.MessageID || again.Task.State != BackgroundDone {
		t.Errorf("ended again by the same: %+v %v", again, err)
	}
	swept, err := s.EndTask(ctx, first.ID, TaskEndedBySweep, BackgroundFailed, build)
	if err != nil || swept.Mine || swept.MessageID != end.MessageID {
		t.Errorf("ended again by another: %+v %v", swept, err)
	}
	msgs, _ := s.LoadMessagesWithID(ctx, "zz-bg-s1")
	if len(msgs) != 1 || msgs[0].Key != TaskResultKey(first.ID) || msgs[0].Kind != KindTaskResult || msgs[0].Task == nil || msgs[0].Task.State != BackgroundDone {
		t.Fatalf("messages: %+v", msgs)
	}
	if err := s.AddTaskFollowUp(ctx, "zz-bg-s1", "jarvis", first.ID, f, 5); !errors.Is(err, ErrTaskOver) {
		t.Errorf("an instruction once over: %v", err)
	}
	if err := s.SetTaskCancelledBy(ctx, first.ID, "Bob"); !errors.Is(err, ErrTaskOver) {
		t.Errorf("cancelled once over: %v", err)
	}

	// Two writers at once: one message, one winner.
	second := running[1]
	results := make([]TaskEnding, 2)
	for i, by := range []string{TaskEndedByTask, TaskEndedBySweep} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			if results[i], err = s.EndTask(ctx, second.ID, by, BackgroundFailed, build); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if results[0].Mine == results[1].Mine || results[0].MessageID != results[1].MessageID {
		t.Errorf("two writers: %+v", results)
	}

	// The sweep's list: running, started before.
	if since, err := s.ListTasksRunningSince(ctx, time.Now().Add(time.Minute)); err != nil || len(since) < 2 {
		t.Errorf("running since: %d %v", len(since), err)
	}
	if since, _ := s.ListTasksRunningSince(ctx, time.Now().Add(-time.Hour)); len(since) != 0 {
		t.Errorf("running since an hour: %d", len(since))
	}

	// A task never started is dropped; the session deleted takes the rest.
	if err := s.DropTask(ctx, running[2].ID); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.GetTask(ctx, running[2].ID); got != nil {
		t.Errorf("dropped: %+v", got)
	}
	if err := s.DeleteSession(ctx, "zz-bg-s1"); err != nil {
		t.Fatal(err)
	}
	if end, err := s.EndTask(ctx, first.ID, TaskEndedByTask, BackgroundDone, build); err != nil || !end.Gone {
		t.Errorf("session deleted: %+v %v", end, err)
	}
	if msgs, _ := s.LoadMessagesWithID(ctx, "zz-bg-s1"); len(msgs) != 0 {
		t.Errorf("messages left: %d", len(msgs))
	}
}
