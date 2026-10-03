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
	for _, f := range []struct{ id, by, purpose string }{{"zz-f-alice", "zz-alice", "Écrire l'export CSV"}, {"zz-f-bob", "zz-bob", ""}} {
		if err := s.CreateSession(ctx, Session{SessionID: f.id, CreatedBy: f.by, Channel: "web",
			ParentSessionID: "zz-parent", ForkedAtMessageID: all[1].ID, ForkedBy: f.by, ForkPurpose: f.purpose}); err != nil {
			t.Fatal(err)
		}
	}
	if f, _ := s.GetSession(ctx, "zz-f-alice"); f.ParentSessionID != "zz-parent" || f.ForkedAtMessageID != all[1].ID || f.ForkedBy != "zz-alice" ||
		f.ForkPurpose != "Écrire l'export CSV" {
		t.Errorf("fork %+v", f)
	}
	if f, _ := s.GetSession(ctx, "zz-f-bob"); f.ForkPurpose != "" {
		t.Errorf("a fork opened without a purpose: %+v", f)
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

// AppendMessage returns the stored message's ID, the one already stored when
// its key was written before: a retried write answers like the first.
func TestAppendMessage_ReturnsItsID(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	const sid = "zz-append-id"
	cleanup := func() { s.DeleteMessagesBySession(ctx, sid) }
	cleanup()
	t.Cleanup(cleanup)

	first, err := s.AppendMessage(ctx, sid, HumanMessageKey("a"), Message{Role: RoleUser, Content: `"a"`})
	if err != nil || first == 0 {
		t.Fatalf("first: %d, %v", first, err)
	}
	second, err := s.AppendMessage(ctx, sid, HumanMessageKey("b"), Message{Role: RoleUser, Content: `"b"`})
	if err != nil || second <= first {
		t.Fatalf("second: %d, %v; want after %d", second, err, first)
	}
	if last, _ := s.LastMessageID(ctx, sid); last != second {
		t.Errorf("last message %d, want %d", last, second)
	}
	again, err := s.AppendMessage(ctx, sid, HumanMessageKey("a"), Message{Role: RoleUser, Content: `"changed"`})
	if err != nil || again != first {
		t.Errorf("rewrite: %d, %v; want %d", again, err, first)
	}
	msgs, _ := s.LoadMessagesWithID(ctx, sid)
	if len(msgs) != 2 || msgs[0].Content != `"a"` {
		t.Errorf("after the rewrite: %+v, want the two messages, the first unchanged", msgs)
	}
}

// A turn reads its session up to the message it answers, what its own group
// wrote, and what the turns of earlier messages wrote even after that
// message; not a person's message stored after it, nor another turn's whose
// key merely starts like its own.
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
	question, err := s.AppendMessage(ctx, sid, HumanMessageKey("q"), text("question"))
	if err != nil || question == 0 {
		t.Fatalf("question %d, %v", question, err)
	}
	turn := TurnKey(TurnGroupKey("run-1", question), 0)
	s.AppendMessages(ctx, sid, turn, 0, []Message{{Role: RoleAssistant, Content: `"searching"`}})
	meanwhile, _ := s.AppendMessage(ctx, sid, HumanMessageKey("m"), text("meanwhile"))
	s.AppendMessages(ctx, sid, turn, 1, []Message{{Role: RoleAssistant, Content: `"found"`}})
	s.AppendMessages(ctx, sid, turn+"1", 0, []Message{{Role: RoleAssistant, Content: `"another turn"`}})

	keys := func(got []MessageWithID) []string {
		var seen []string
		for _, m := range got {
			seen = append(seen, m.Key+" "+m.Content)
		}
		return seen
	}
	got, err := s.LoadConversation(ctx, sid, question, []string{turn})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{`msg:q "question"`, turn + `:0 "searching"`, turn + `:1 "found"`}
	if !reflect.DeepEqual(keys(got), want) {
		t.Errorf("loaded %q\nwant %q", keys(got), want)
	}

	// No turn: the message alone.
	if got, _ := s.LoadConversation(ctx, sid, question, nil); len(got) != 1 {
		t.Errorf("without turns: %q, want the question", keys(got))
	}

	// The turn answering the message written meanwhile reads all that the
	// turns of the question wrote, after that message too. In ID order:
	// conversation.Order moves the message after them.
	next := TurnKey(TurnGroupKey("run-2", meanwhile), 0)
	s.AppendMessages(ctx, sid, next, 0, []Message{{Role: RoleAssistant, Content: `"answer"`}})
	got, _ = s.LoadConversation(ctx, sid, meanwhile, []string{next})
	want = []string{`msg:q "question"`, turn + `:0 "searching"`, `msg:m "meanwhile"`, turn + `:1 "found"`, turn + `1:0 "another turn"`, next + `:0 "answer"`}
	if !reflect.DeepEqual(keys(got), want) {
		t.Errorf("the next turn loaded %q\nwant %q", keys(got), want)
	}

	// A later message's turns are not read by an earlier one's: the question
	// read again stops before them.
	got, _ = s.LoadConversation(ctx, sid, question, []string{turn})
	want = []string{`msg:q "question"`, turn + `:0 "searching"`, turn + `:1 "found"`}
	if !reflect.DeepEqual(keys(got), want) {
		t.Errorf("the next turn loaded %q\nwant %q", keys(got), want)
	}
}

// clearMemory deletes the test's memory rows: nothing else deletes from
// memory.
func clearMemory(t *testing.T, s *PostgresStore, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if _, err := s.db.Exec("DELETE FROM memory WHERE scope = $1 AND scope_id = $2", string(MemoryScopeUser), id); err != nil {
			t.Fatal(err)
		}
	}
}

// A save names the version it replaces: the first from 0, each next from the
// one before. A save from a version another save replaced writes nothing and
// is refused with ErrMemoryConflict.
func TestSaveMemory_Versions(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	const id, other = "zz-memory-versions", "zz-memory-none"
	clearMemory(t, s, id, other)
	t.Cleanup(func() { clearMemory(t, s, id, other) })

	if m, err := s.LoadMemory(ctx, MemoryScopeUser, id); err != nil || m != (Memory{}) {
		t.Fatalf("never saved: %+v, %v", m, err)
	}
	if v, err := s.SaveMemory(ctx, MemoryScopeUser, id, "likes tea", 0); err != nil || v != 1 {
		t.Fatalf("first save: version %d, %v", v, err)
	}
	if v, err := s.SaveMemory(ctx, MemoryScopeUser, id, "likes tea and coffee", 1); err != nil || v != 2 {
		t.Fatalf("update: version %d, %v", v, err)
	}
	if m, _ := s.LoadMemory(ctx, MemoryScopeUser, id); m != (Memory{Content: "likes tea and coffee", Version: 2}) {
		t.Errorf("loaded %+v", m)
	}

	for _, expected := range []int64{0, 1, 3} {
		if _, err := s.SaveMemory(ctx, MemoryScopeUser, id, "stale", expected); !errors.Is(err, ErrMemoryConflict) {
			t.Errorf("save from version %d: %v", expected, err)
		}
		if m, _ := s.LoadMemory(ctx, MemoryScopeUser, id); m != (Memory{Content: "likes tea and coffee", Version: 2}) {
			t.Errorf("a save from version %d wrote: %+v", expected, m)
		}
	}

	// A version for a memory never saved: nothing to replace, nothing written.
	if _, err := s.SaveMemory(ctx, MemoryScopeUser, other, "blind", 4); !errors.Is(err, ErrMemoryConflict) {
		t.Errorf("save over a missing memory: %v", err)
	}
	if m, _ := s.LoadMemory(ctx, MemoryScopeUser, other); m != (Memory{}) {
		t.Errorf("a refused save created %+v", m)
	}
}

// Saves from the same version at once, first save or not: exactly one wins,
// the others are refused, and the memory is the winner's.
func TestSaveMemory_ConcurrentSavesOneWins(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	const id = "zz-memory-race"
	clearMemory(t, s, id)
	t.Cleanup(func() { clearMemory(t, s, id) })

	const writers = 8
	for round := range 5 {
		expected := int64(round) // round 0: the first save, an INSERT
		type result struct {
			version int64
			err     error
		}
		results := make(chan result, writers)
		for w := range writers {
			go func() {
				v, err := s.SaveMemory(ctx, MemoryScopeUser, id, fmt.Sprintf("round %d writer %d", round, w), expected)
				results <- result{v, err}
			}()
		}
		won := 0
		for range writers {
			r := <-results
			switch {
			case r.err == nil && r.version == expected+1:
				won++
			case errors.Is(r.err, ErrMemoryConflict):
			default:
				t.Fatalf("round %d: version %d, %v", round, r.version, r.err)
			}
		}
		if won != 1 {
			t.Fatalf("round %d: %d saves won, want exactly one", round, won)
		}
		if m, err := s.LoadMemory(ctx, MemoryScopeUser, id); err != nil || m.Version != expected+1 {
			t.Fatalf("round %d: memory %+v, %v; want version %d", round, m, err, expected+1)
		}
	}
}
