package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"
)

// openTestStore connects to TEST_DATABASE_URL, a throwaway database: the test
// writes agents into it. Without it the integration tests are skipped.
func openTestStore(t *testing.T) *PostgresStore {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	s, err := NewPostgresStore(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestAgentLifecycle(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	const id = "zz-store-test"
	s.DeleteAgent(ctx, id) // leftover from an aborted run
	t.Cleanup(func() { s.DeleteAgent(ctx, id) })

	if err := s.CreateAgent(ctx, Agent{ID: id, Name: "Test"}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateAgent(ctx, Agent{ID: id, Name: "Again"}); !errors.Is(err, ErrAgentExists) {
		t.Errorf("second create: %v, want ErrAgentExists", err)
	}

	a, err := s.GetAgent(ctx, id)
	if err != nil || a == nil {
		t.Fatalf("get: %v %v", a, err)
	}
	// A nil allowlist is stored as [], which grants nothing.
	if a.Tools == nil || len(a.Tools) != 0 || a.Revision != 1 {
		t.Errorf("created %+v", a)
	}

	rev, err := s.UpdateAgent(ctx, Agent{ID: id, Name: "Renamed", Tools: []string{"read_file"}}, 1)
	if err != nil || rev != 2 {
		t.Fatalf("update: rev %d, %v", rev, err)
	}
	if _, err := s.UpdateAgent(ctx, Agent{ID: id, Name: "Stale"}, 1); !errors.Is(err, ErrAgentConflict) {
		t.Errorf("stale update: %v, want ErrAgentConflict", err)
	}
	if _, err := s.UpdateAgent(ctx, Agent{ID: "zz-nobody", Name: "X"}, 1); !errors.Is(err, ErrAgentNotFound) {
		t.Errorf("update of a missing agent: %v, want ErrAgentNotFound", err)
	}

	a, _ = s.GetAgent(ctx, id)
	if a.Name != "Renamed" || !reflect.DeepEqual(a.Tools, []string{"read_file"}) || a.UpdatedAt.Before(a.CreatedAt) {
		t.Errorf("after update %+v", a)
	}

	if err := s.DeleteAgent(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteAgent(ctx, id); !errors.Is(err, ErrAgentNotFound) {
		t.Errorf("second delete: %v, want ErrAgentNotFound", err)
	}
}

func TestMigrate_SpawnSessionBecomesAgentTools(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	const id = "zz-migrate-test"
	s.DeleteAgent(ctx, id)
	t.Cleanup(func() { s.DeleteAgent(ctx, id) })

	if err := s.CreateAgent(ctx, Agent{ID: id, Name: "Old", Tools: []string{"read_file", "spawn_session"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO tools (name, task_queue, input_schema, kind, schema_hash)
		VALUES ('spawn_session', 'agent', '{}', 'workflow', 'x') ON CONFLICT (name) DO NOTHING`); err != nil {
		t.Fatal(err)
	}

	// Migrations run at every start: running it twice must be harmless.
	for range 2 {
		if err := s.migrate(); err != nil {
			t.Fatal(err)
		}
	}

	a, _ := s.GetAgent(ctx, id)
	if !reflect.DeepEqual(a.Tools, []string{"read_file", "agent_*"}) || a.Revision != 2 {
		t.Errorf("after migration: tools %v, revision %d", a.Tools, a.Revision)
	}
	tools, _ := s.ListTools(ctx)
	for _, tr := range tools {
		if tr.Name == "spawn_session" {
			t.Error("the spawn_session row survived the migration")
		}
	}
}

func TestUsersAndLoginSessions(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	alice := User{ID: "zz-u-alice", Email: "Alice@Example.com", DisplayName: "Alice", Role: UserRoleStandard, PasswordHash: "h"}
	t.Cleanup(func() { s.db.Exec("DELETE FROM users WHERE id LIKE 'zz-%'") })
	s.db.Exec("DELETE FROM users WHERE id LIKE 'zz-%'")

	if err := s.CreateUser(ctx, alice); err != nil {
		t.Fatal(err)
	}
	// The email is unique whatever its case.
	if err := s.CreateUser(ctx, User{ID: "zz-u-dup", Email: "alice@example.COM", Role: UserRoleStandard, PasswordHash: "h"}); !errors.Is(err, ErrUserExists) {
		t.Errorf("duplicate email: %v", err)
	}
	if u, _ := s.GetUserByEmail(ctx, " ALICE@example.com "); u == nil || u.ID != alice.ID {
		t.Errorf("lookup by email: %+v", u)
	}

	if err := s.CreateLoginSession(ctx, "zz-tok", alice.ID, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateLoginSession(ctx, "zz-old", alice.ID, time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if u, _ := s.GetLoginSessionUser(ctx, "zz-tok"); u == nil || u.ID != alice.ID {
		t.Errorf("valid session: %+v", u)
	}
	if u, _ := s.GetLoginSessionUser(ctx, "zz-old"); u != nil {
		t.Error("an expired session logs in")
	}

	// Disabling ends the sessions; re-enabling does not bring them back.
	if err := s.SetUserDisabled(ctx, alice.ID, true); err != nil {
		t.Fatal(err)
	}
	if u, _ := s.GetUser(ctx, alice.ID); u.DisabledAt == nil {
		t.Error("not disabled")
	}
	s.SetUserDisabled(ctx, alice.ID, false)
	if u, _ := s.GetLoginSessionUser(ctx, "zz-tok"); u != nil {
		t.Error("a session survived disabling")
	}

	s.CreateLoginSession(ctx, "zz-tok2", alice.ID, time.Now().Add(time.Hour))
	if err := s.SetUserPassword(ctx, alice.ID, "h2"); err != nil {
		t.Fatal(err)
	}
	if u, _ := s.GetLoginSessionUser(ctx, "zz-tok2"); u != nil {
		t.Error("a session survived a password change")
	}
	if err := s.SetUserPassword(ctx, "zz-nobody", "h"); !errors.Is(err, ErrUserNotFound) {
		t.Errorf("password of a missing user: %v", err)
	}
}

func TestSessionMembers(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	t.Cleanup(func() {
		s.db.Exec("DELETE FROM sessions WHERE session_id LIKE 'zz-%'")
		s.db.Exec("DELETE FROM users WHERE id LIKE 'zz-%'")
	})
	s.db.Exec("DELETE FROM sessions WHERE session_id LIKE 'zz-%'")
	s.db.Exec("DELETE FROM users WHERE id LIKE 'zz-%'")
	for _, id := range []string{"zz-alice", "zz-bob"} {
		if err := s.CreateUser(ctx, User{ID: id, Email: id + "@example.com", Role: UserRoleStandard, PasswordHash: "h"}); err != nil {
			t.Fatal(err)
		}
	}

	if err := s.CreateSession(ctx, Session{SessionID: "zz-s1", CreatedBy: "zz-alice", Channel: "web"}); err != nil {
		t.Fatal(err)
	}
	// The creator is the first member.
	if ok, _ := s.IsSessionMember(ctx, "zz-s1", "zz-alice"); !ok {
		t.Error("creator not a member")
	}
	if ok, _ := s.IsSessionMember(ctx, "zz-s1", "zz-bob"); ok {
		t.Error("bob a member before being added")
	}

	s.AddSessionMember(ctx, "zz-s1", "zz-bob", "zz-alice")
	if err := s.AddSessionMember(ctx, "zz-s1", "zz-bob", "zz-alice"); err != nil {
		t.Errorf("adding twice: %v", err)
	}
	members, _ := s.ListSessionMembers(ctx, "zz-s1")
	if len(members) != 2 || members[1].UserID != "zz-bob" || members[1].AddedBy != "zz-alice" || members[1].Email != "zz-bob@example.com" {
		t.Errorf("members %+v", members)
	}
	if list, _ := s.ListSessionsByUser(ctx, "zz-bob"); len(list) != 1 || list[0].SessionID != "zz-s1" || list[0].CreatedBy != "zz-alice" {
		t.Errorf("bob's sessions %+v", list)
	}

	s.RemoveSessionMember(ctx, "zz-s1", "zz-bob")
	if list, _ := s.ListSessionsByUser(ctx, "zz-bob"); len(list) != 0 {
		t.Errorf("bob still sees %+v", list)
	}

	// Deleting the session takes its members with it.
	if err := s.DeleteSession(ctx, "zz-s1"); err != nil {
		t.Fatal(err)
	}
	var n int
	s.db.QueryRow("SELECT count(*) FROM session_members WHERE session_id = 'zz-s1'").Scan(&n)
	if n != 0 {
		t.Errorf("%d members left after delete", n)
	}
}

func TestForks(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	cleanup := func() {
		s.db.Exec("DELETE FROM messages WHERE session_id LIKE 'zz-%'")
		s.db.Exec("DELETE FROM sessions WHERE session_id LIKE 'zz-f%'")
		s.db.Exec("DELETE FROM sessions WHERE session_id LIKE 'zz-%'")
		s.db.Exec("DELETE FROM users WHERE id LIKE 'zz-%'")
	}
	cleanup()
	t.Cleanup(cleanup)
	for _, id := range []string{"zz-alice", "zz-bob"} {
		if err := s.CreateUser(ctx, User{ID: id, Email: id + "@example.com", Role: UserRoleStandard, PasswordHash: "h"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.CreateSession(ctx, Session{SessionID: "zz-parent", CreatedBy: "zz-alice", Channel: "web"}); err != nil {
		t.Fatal(err)
	}
	for i, m := range []string{"one", "two", "three"} {
		s.AppendMessage(ctx, "zz-parent", fmt.Sprintf("k%d", i), Message{Role: RoleUser, Content: `"` + m + `"`})
	}
	all, _ := s.LoadMessagesWithID(ctx, "zz-parent")
	if len(all) != 3 {
		t.Fatalf("%d messages", len(all))
	}
	upTo, _ := s.LoadMessagesUpTo(ctx, "zz-parent", all[1].ID)
	if len(upTo) != 2 || upTo[1].ID != all[1].ID {
		t.Errorf("up to the second message: %+v", upTo)
	}

	// A plain session has no parent; a fork records where it started.
	if p, _ := s.GetSession(ctx, "zz-parent"); p.ParentSessionID != "" || p.ForkedAtMessageID != 0 || p.ForkedBy != "" {
		t.Errorf("plain session %+v", p)
	}
	for _, f := range []struct{ id, by string }{{"zz-f-alice", "zz-alice"}, {"zz-f-bob", "zz-bob"}} {
		if err := s.CreateSession(ctx, Session{SessionID: f.id, CreatedBy: f.by, Channel: "web",
			ParentSessionID: "zz-parent", ForkedAtMessageID: all[1].ID, ForkedBy: f.by}); err != nil {
			t.Fatal(err)
		}
	}
	if f, _ := s.GetSession(ctx, "zz-f-alice"); f.ParentSessionID != "zz-parent" || f.ForkedAtMessageID != all[1].ID || f.ForkedBy != "zz-alice" {
		t.Errorf("fork %+v", f)
	}
	// Each user sees only the forks they are a member of.
	if forks, _ := s.ListForks(ctx, "zz-parent", "zz-alice"); len(forks) != 1 || forks[0].SessionID != "zz-f-alice" {
		t.Errorf("alice's forks %+v", forks)
	}

	// Deleting the parent keeps the fork, without its link.
	if err := s.DeleteSession(ctx, "zz-parent"); err != nil {
		t.Fatal(err)
	}
	if f, _ := s.GetSession(ctx, "zz-f-alice"); f == nil || f.ParentSessionID != "" || f.ForkedAtMessageID == 0 {
		t.Errorf("fork after the parent's deletion: %+v", f)
	}
}

func TestTaskLogsBelongToTheirUser(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	ids := []string{"zz-task-alice", "zz-task-bob"}
	cleanup := func() {
		for _, id := range ids {
			s.db.ExecContext(ctx, "DELETE FROM task_logs WHERE schedule_id = $1", id)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	for i, user := range []string{"zz-alice", "zz-bob"} {
		if err := s.SaveTaskLog(ctx, TaskLog{ScheduleID: ids[i], Type: "schedule", Description: user, Prompt: "p", UserID: user, Status: "scheduled"}); err != nil {
			t.Fatal(err)
		}
	}
	logs, err := s.ListTaskLogsByUser(ctx, "zz-alice")
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 1 || logs[0].ScheduleID != "zz-task-alice" {
		t.Errorf("alice's tasks: %+v", logs)
	}

	got, err := s.GetTaskLog(ctx, "zz-task-bob")
	if err != nil || got == nil || got.UserID != "zz-bob" {
		t.Errorf("get bob's task: %+v, %v", got, err)
	}
	if got, err := s.GetTaskLog(ctx, "zz-task-nobody"); err != nil || got != nil {
		t.Errorf("get a missing task: %+v, %v", got, err)
	}
}

func TestToolProperties(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	names := []string{"zz-private", "save_user_memory", "exec", "ask_user"}
	cleanup := func() {
		for _, n := range names {
			s.db.ExecContext(ctx, "DELETE FROM tools WHERE name = $1", n)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	rec := ToolRecord{Name: "zz-private", TaskQueue: "q", InputSchema: []byte(`{}`), Kind: "activity",
		PrivateInput: true, Sensitive: true, NeedsCallContext: true, Timeout: 330 * time.Second, SchemaHash: "h"}
	if err := s.UpsertTool(ctx, rec); err != nil {
		t.Fatal(err)
	}
	tools, err := s.ListTools(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, got := range tools {
		if got.Name == "zz-private" && !(got.PrivateInput && got.Sensitive && got.NeedsCallContext && got.Timeout == 330*time.Second) {
			t.Errorf("read back %+v", got)
		}
	}

	// Rows published before the columns existed get the properties the code
	// used to hardcode, so a user's memory stays hidden across the upgrade.
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE tools DROP COLUMN sensitive, DROP COLUMN private_input, DROP COLUMN needs_call_context`); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"save_user_memory", "exec", "ask_user"} {
		if _, err := s.db.ExecContext(ctx, `INSERT INTO tools (name, task_queue, input_schema, kind, schema_hash) VALUES ($1, 'q', '{}', 'activity', 'h')`, n); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.migrate(); err != nil {
		t.Fatal(err)
	}
	tools, _ = s.ListTools(ctx)
	got := map[string]ToolRecord{}
	for _, r := range tools {
		got[r.Name] = r
	}
	if !got["save_user_memory"].PrivateInput || !got["exec"].Sensitive || !got["ask_user"].NeedsCallContext || got["exec"].PrivateInput {
		t.Errorf("backfilled %+v", got)
	}
}

// A queue withdraws only the tools it published.
func TestDeleteTool_OnlyOnItsQueue(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	cleanup := func() { s.db.ExecContext(ctx, "DELETE FROM tools WHERE name = 'zz-gone'") }
	cleanup()
	t.Cleanup(cleanup)

	rec := ToolRecord{Name: "zz-gone", TaskQueue: "q1", InputSchema: []byte(`{}`), Kind: "mcp", SchemaHash: "h"}
	if err := s.UpsertTool(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if deleted, err := s.DeleteTool(ctx, "zz-gone", "q2"); err != nil || deleted {
		t.Errorf("another queue's delete: %v, %v", deleted, err)
	}
	if deleted, err := s.DeleteTool(ctx, "zz-gone", "q1"); err != nil || !deleted {
		t.Errorf("its queue's delete: %v, %v", deleted, err)
	}
	tools, _ := s.ListTools(ctx)
	for _, r := range tools {
		if r.Name == "zz-gone" {
			t.Error("still published")
		}
	}
}

// A call's time limit is a duration or the default (0): the table refuses a
// negative one, which the SDK would refuse at every call.
func TestUpsertTool_RefusesANegativeTimeout(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	cleanup := func() { s.db.ExecContext(ctx, "DELETE FROM tools WHERE name = 'zz-timeout'") }
	cleanup()
	t.Cleanup(cleanup)

	rec := ToolRecord{Name: "zz-timeout", TaskQueue: "q1", InputSchema: []byte(`{}`), Kind: "mcp", SchemaHash: "h", Timeout: -time.Second}
	if err := s.UpsertTool(ctx, rec); err == nil {
		t.Error("a negative timeout was stored")
	}
	rec.Timeout = 90 * time.Second
	if err := s.UpsertTool(ctx, rec); err != nil {
		t.Fatal(err)
	}
}

// A mention calls one agent: two cannot share it, case aside, on create as on
// update. An empty mention (the ID stands in) is never a clash.
func TestAgentMentionIsUnique(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	const a, b = "zz-mention-a", "zz-mention-b"
	for _, id := range []string{a, b} {
		s.DeleteAgent(ctx, id)
		t.Cleanup(func() { s.DeleteAgent(ctx, id) })
	}

	if err := s.CreateAgent(ctx, Agent{ID: a, Name: "A", Mention: "Jarvis"}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateAgent(ctx, Agent{ID: b, Name: "B", Mention: "jarvis"}); !errors.Is(err, ErrMentionTaken) {
		t.Errorf("create with a taken mention: %v, want ErrMentionTaken", err)
	}
	if err := s.CreateAgent(ctx, Agent{ID: b, Name: "B"}); err != nil {
		t.Fatalf("create without a mention: %v", err)
	}
	if _, err := s.UpdateAgent(ctx, Agent{ID: b, Name: "B", Mention: "JARVIS"}, 1); !errors.Is(err, ErrMentionTaken) {
		t.Errorf("update to a taken mention: %v, want ErrMentionTaken", err)
	}
	got, _ := s.GetAgent(ctx, a)
	if got == nil || got.Mention != "Jarvis" || got.MentionName() != "Jarvis" {
		t.Errorf("agent a %+v", got)
	}
	if got, _ := s.GetAgent(ctx, b); got == nil || got.MentionName() != b {
		t.Errorf("agent b %+v: want its ID as mention", got)
	}
}

// An assistant message keeps the agent that wrote it, and its name then.
func TestMessagesKeepTheirAgent(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	cleanup := func() { s.db.Exec("DELETE FROM messages WHERE session_id = 'zz-agents'") }
	cleanup()
	t.Cleanup(cleanup)

	if err := s.AppendMessages(ctx, "zz-agents", "run-1", 0, []Message{
		{Role: RoleAssistant, Content: `"résumé"`, AgentID: "jarvis", Author: "Jarvis"},
		{Role: RoleAssistant, Kind: KindTurnError, Content: `"boom"`, AgentID: "smith"},
	}); err != nil {
		t.Fatal(err)
	}
	msgs, err := s.LoadMessages(ctx, "zz-agents")
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 || msgs[0].AgentID != "jarvis" || msgs[0].Author != "Jarvis" || msgs[1].AgentID != "smith" {
		t.Errorf("loaded %+v", msgs)
	}
}

// What calls an agent is its mention, or its ID: a mention may not be another
// agent's ID, nor an ID another agent's mention, case aside, on create as on
// update. An agent may take its own ID as mention.
func TestAgentMentionIsNotAnotherID(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	const a, b, c = "zz-names-a", "zz-names-b", "zz-names-c"
	for _, id := range []string{a, b, c} {
		s.DeleteAgent(ctx, id)
		t.Cleanup(func() { s.DeleteAgent(ctx, id) })
	}

	if err := s.CreateAgent(ctx, Agent{ID: a, Name: "A", Mention: "ZZ-Names-Smith"}); err != nil {
		t.Fatal(err)
	}
	// b's mention would be a's ID.
	if err := s.CreateAgent(ctx, Agent{ID: b, Name: "B", Mention: "ZZ-NAMES-A"}); !errors.Is(err, ErrMentionTaken) {
		t.Errorf("create with another agent's ID as mention: %v, want ErrMentionTaken", err)
	}
	// c's ID would be a's mention.
	if err := s.CreateAgent(ctx, Agent{ID: "zz-names-smith", Name: "Smith"}); !errors.Is(err, ErrMentionTaken) {
		t.Errorf("create with another agent's mention as ID: %v, want ErrMentionTaken", err)
	}
	if got, _ := s.GetAgent(ctx, "zz-names-smith"); got != nil {
		s.DeleteAgent(ctx, "zz-names-smith")
		t.Error("the refused agent was written")
	}
	if inserted, err := s.InsertAgentIfAbsent(ctx, Agent{ID: b, Name: "B", Mention: a}); inserted || !errors.Is(err, ErrMentionTaken) {
		t.Errorf("seed with another agent's ID as mention: %v %v, want ErrMentionTaken", inserted, err)
	}

	if err := s.CreateAgent(ctx, Agent{ID: b, Name: "B", Mention: b}); err != nil {
		t.Fatalf("create with its own ID as mention: %v", err)
	}
	if err := s.CreateAgent(ctx, Agent{ID: c, Name: "C"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateAgent(ctx, Agent{ID: c, Name: "C", Mention: "Zz-Names-B"}, 1); !errors.Is(err, ErrMentionTaken) {
		t.Errorf("update to another agent's ID as mention: %v, want ErrMentionTaken", err)
	}
	if rev, err := s.UpdateAgent(ctx, Agent{ID: c, Name: "C", Mention: "zz-names-c2"}, 1); err != nil || rev != 2 {
		t.Errorf("update to a free mention: %d %v", rev, err)
	}
	if got, _ := s.GetAgent(ctx, c); got == nil || got.Mention != "zz-names-c2" {
		t.Errorf("agent c %+v", got)
	}
}

// Two agents written at once cannot take each other's names: the check and
// the write are serialized.
func TestAgentNamesClashUnderConcurrency(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	const a, b = "zz-race-a", "zz-race-b"
	for _, id := range []string{a, b} {
		s.DeleteAgent(ctx, id)
		t.Cleanup(func() { s.DeleteAgent(ctx, id) })
	}

	for range 20 {
		s.DeleteAgent(ctx, a)
		s.DeleteAgent(ctx, b)
		errs := make(chan error, 2)
		go func() { errs <- s.CreateAgent(ctx, Agent{ID: a, Name: "A", Mention: b}) }()
		go func() { errs <- s.CreateAgent(ctx, Agent{ID: b, Name: "B"}) }()
		failed := 0
		for range 2 {
			if err := <-errs; errors.Is(err, ErrMentionTaken) {
				failed++
			} else if err != nil {
				t.Fatal(err)
			}
		}
		if failed != 1 {
			t.Fatalf("%d writes refused, want exactly one", failed)
		}
	}
}

// A turn reads its session as it was when it started, plus what it wrote
// since: a person's message written meanwhile is not in it, nor another
// turn's whose key merely starts like its own.
func TestLoadConversation(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	const sid = "zz-conversation"
	cleanup := func() { s.DeleteMessagesBySession(ctx, sid) }
	cleanup()
	t.Cleanup(cleanup)

	if last, err := s.LastMessageID(ctx, sid); err != nil || last != 0 {
		t.Fatalf("empty session: last %d, %v", last, err)
	}
	text := func(s string) Message { return Message{Role: RoleUser, Content: `"` + s + `"`} }
	s.AppendMessage(ctx, sid, HumanMessageKey("q"), text("question"))
	snapshot, err := s.LastMessageID(ctx, sid)
	if err != nil || snapshot == 0 {
		t.Fatalf("last %d, %v", snapshot, err)
	}
	turn := TurnKey("run-1", 0)
	s.AppendMessages(ctx, sid, turn, 0, []Message{{Role: RoleAssistant, Content: `"searching"`}})
	s.AppendMessage(ctx, sid, HumanMessageKey("m"), text("meanwhile"))
	s.AppendMessages(ctx, sid, turn, 1, []Message{{Role: RoleAssistant, Content: `"found"`}})
	s.AppendMessages(ctx, sid, turn+"1", 0, []Message{{Role: RoleAssistant, Content: `"another turn"`}})

	got, err := s.LoadConversation(ctx, sid, snapshot, []string{turn})
	if err != nil {
		t.Fatal(err)
	}
	var seen []string
	for _, m := range got {
		seen = append(seen, m.Key+" "+m.Content)
	}
	want := []string{`msg:q "question"`, `run-1.0:0 "searching"`, `run-1.0:1 "found"`}
	if !reflect.DeepEqual(seen, want) {
		t.Errorf("loaded %q\nwant %q", seen, want)
	}

	// No turn: the snapshot alone. Everything once the bound covers it.
	if got, _ := s.LoadConversation(ctx, sid, snapshot, nil); len(got) != 1 {
		t.Errorf("without turns: %d messages, want the question", len(got))
	}
	last, _ := s.LastMessageID(ctx, sid)
	if got, _ := s.LoadConversation(ctx, sid, last, nil); len(got) != 5 || got[2].Key != "msg:m" {
		t.Errorf("up to the last: %+v", got)
	}
}
