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
	// A nil allowlist is stored as [], which grants nothing; and its model
	// runs on the server's key unless said.
	if a.Tools == nil || len(a.Tools) != 0 || a.Revision != 1 || a.LLMOnMachine != LLMOnMachineNever {
		t.Errorf("created %+v", a)
	}

	rev, err := s.UpdateAgent(ctx, Agent{ID: id, Name: "Renamed", Tools: []string{"read_file"}, LLMOnMachine: LLMOnMachinePrefer}, 1)
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
	if a.Name != "Renamed" || !reflect.DeepEqual(a.Tools, []string{"read_file"}) || a.UpdatedAt.Before(a.CreatedAt) || a.LLMOnMachine != LLMOnMachinePrefer {
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
	if n, ok, err := s.SessionMembership(ctx, "zz-s1", "zz-bob"); n != 2 || !ok || err != nil {
		t.Errorf("bob's membership: %d members, member %v, %v", n, ok, err)
	}
	if n, ok, err := s.SessionMembership(ctx, "zz-s1", "zz-carol"); n != 2 || ok || err != nil {
		t.Errorf("a stranger's membership: %d members, member %v, %v", n, ok, err)
	}
	if n, ok, err := s.SessionMembership(ctx, "zz-none", "zz-bob"); n != 0 || ok || err != nil {
		t.Errorf("no session: %d members, member %v, %v", n, ok, err)
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

	// Its summary is recorded on it, once, in the same write.
	if f, _ := s.GetSession(ctx, "zz-f-alice"); f.SummaryMessageID != 0 {
		t.Errorf("a fork before its summary: %+v", f)
	}
	summary := Message{Role: RoleUser, Kind: KindForkSummary, Content: `"brief"`}
	id, err := s.AppendForkSummary(ctx, "zz-f-alice", summary)
	if err != nil || id == 0 {
		t.Fatalf("summary: %d, %v", id, err)
	}
	if again, err := s.AppendForkSummary(ctx, "zz-f-alice", summary); again != id || err != nil {
		t.Errorf("retry: %d, %v; want %d", again, err, id)
	}
	if f, _ := s.GetSession(ctx, "zz-f-alice"); f.SummaryMessageID != id {
		t.Errorf("fork after its summary: %+v", f)
	}
	if msgs, _ := s.LoadMessagesWithID(ctx, "zz-f-alice"); len(msgs) != 1 || msgs[0].ID != id || msgs[0].Key != ForkSummaryKey || msgs[0].Kind != KindForkSummary {
		t.Errorf("fork's messages %+v", msgs)
	}
	// A fork deleted while its summary was written gets none.
	if _, err := s.AppendForkSummary(ctx, "zz-f-gone", summary); !errors.Is(err, ErrForkGone) {
		t.Errorf("deleted fork: %v", err)
	}
	if msgs, _ := s.LoadMessagesWithID(ctx, "zz-f-gone"); len(msgs) != 0 {
		t.Errorf("a summary written for a deleted fork: %+v", msgs)
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
		{Role: RoleAssistant, Kind: KindTurnEnd, Content: `"boom"`, AgentID: "smith"},
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
	again, err := s.AppendMessage(ctx, sid, HumanMessageKey("a"), Message{Role: RoleUser, Content: `"changed"`})
	if err != nil || again != first {
		t.Errorf("rewrite: %d, %v; want %d", again, err, first)
	}
	msgs, _ := s.LoadMessagesWithID(ctx, sid)
	if len(msgs) != 2 || msgs[0].Content != `"a"` {
		t.Errorf("after the rewrite: %+v, want the two messages, the first unchanged", msgs)
	}
}

// A turn reads its session up to the message it answers, its own turn
// whole, and another participant's turn once it ended by that message; not
// a person's message stored after it, nor a turn still running.
func TestLoadConversation(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	const sid = "zz-conversation"
	cleanup := func() { s.DeleteMessagesBySession(ctx, sid) }
	cleanup()
	t.Cleanup(cleanup)

	text := func(s string) Message { return Message{Role: RoleUser, Content: `"` + s + `"`} }
	question, err := s.AppendMessage(ctx, sid, HumanMessageKey("q"), text("question"))
	if err != nil || question == 0 {
		t.Fatalf("question %d, %v", question, err)
	}
	jarvis := TurnKey(question, "jarvis")
	s.AppendMessages(ctx, sid, jarvis, 0, []Message{{Role: RoleAssistant, Content: `"searching"`}})
	meanwhile, _ := s.AppendMessage(ctx, sid, HumanMessageKey("m"), text("meanwhile"))
	s.AppendMessages(ctx, sid, jarvis, 1, []Message{{Role: RoleAssistant, Content: `"found"`}})

	keys := func(got []MessageWithID) []string {
		var seen []string
		for _, m := range got {
			seen = append(seen, m.Key+" "+m.Content)
		}
		return seen
	}
	load := func(scope TurnScope) []string {
		t.Helper()
		got, err := s.LoadConversation(ctx, sid, scope)
		if err != nil {
			t.Fatal(err)
		}
		return keys(got)
	}
	want := []string{`msg:q "question"`, jarvis + `:0 "searching"`, jarvis + `:1 "found"`}
	if got := load(ScopeOf(jarvis, nil)); !reflect.DeepEqual(got, want) {
		t.Errorf("jarvis loaded %q\nwant %q", got, want)
	}

	// Smith answers the message written meanwhile: Jarvis's turn runs, so
	// none of it is read.
	smith := TurnKey(meanwhile, "smith")
	want = []string{`msg:q "question"`, `msg:m "meanwhile"`}
	if got := load(ScopeOf(smith, nil)); !reflect.DeepEqual(got, want) {
		t.Errorf("smith, jarvis running, loaded %q\nwant %q", got, want)
	}

	// Jarvis's turn ends; Smith's next turn, on a later message, reads it
	// whole, never its end.
	if _, err := s.AppendTurnEnd(ctx, sid, jarvis, TurnEnd("jarvis", "")); err != nil {
		t.Fatal(err)
	}
	later, _ := s.AppendMessage(ctx, sid, HumanMessageKey("l"), text("later"))
	want = []string{`msg:q "question"`, jarvis + `:0 "searching"`, `msg:m "meanwhile"`, jarvis + `:1 "found"`, `msg:l "later"`}
	if got := load(ScopeOf(TurnKey(later, "smith"), nil)); !reflect.DeepEqual(got, want) {
		t.Errorf("smith, jarvis ended, loaded %q\nwant %q", got, want)
	}
	// Ended after the message Smith answers: still not read.
	if got := load(ScopeOf(smith, nil)); len(got) != 2 {
		t.Errorf("smith on the message before jarvis ended loaded %q", got)
	}
}

// A turn's end goes under its own key, after the turn's messages: a turn
// that stored message 0 does not absorb it, and a rewrite keeps the first.
// Whether a participant answered a message is whether its turn has an end.
func TestAppendTurnEnd(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	const sid = "zz-turn-end"
	cleanup := func() { s.DeleteMessagesBySession(ctx, sid) }
	cleanup()
	t.Cleanup(cleanup)

	question, _ := s.AppendMessage(ctx, sid, HumanMessageKey("q"), Message{Role: RoleUser, Content: `"q"`})
	turn := TurnKey(question, "jarvis")
	if done, err := s.HasTurnEnd(ctx, sid, turn); err != nil || done {
		t.Fatalf("before any end: %v, %v", done, err)
	}
	if err := s.AppendMessages(ctx, sid, turn, 0, []Message{{Role: RoleAssistant, ToolCalls: []ToolCall{{ID: "t1"}}}, {Role: RoleTool, ToolResult: &ToolResult{ToolCallID: "t1"}}}); err != nil {
		t.Fatal(err)
	}
	id, err := s.AppendTurnEnd(ctx, sid, turn, TurnEnd("jarvis", "call LLM: boom"))
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.AppendTurnEnd(ctx, sid, turn, TurnEnd("jarvis", ""))
	if err != nil || again != id {
		t.Errorf("rewrite: %d, %v; want %d", again, err, id)
	}
	msgs, _ := s.LoadMessagesWithID(ctx, sid)
	if len(msgs) != 4 || msgs[3].ID != id || msgs[3].Key != TurnEndKey(turn) || TurnEndError(msgs[3].Message) != "call LLM: boom" {
		t.Fatalf("messages %+v, want the turn's two then its end, with the first error", msgs)
	}
	if done, err := s.HasTurnEnd(ctx, sid, turn); err != nil || !done {
		t.Errorf("after the end: %v, %v", done, err)
	}
	if done, _ := s.HasTurnEnd(ctx, sid, TurnKey(question, "smith")); done {
		t.Error("another participant's turn has an end")
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

// A fork's report goes into its parent and is recorded on the fork at once;
// retried, it is not posted twice; started from a stale point, by someone who
// left the parent, or after the parent's deletion, it is refused.
func TestForkReports(t *testing.T) {
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
	s.AddSessionMember(ctx, "zz-parent", "zz-bob", "zz-alice")
	at, _ := s.AppendMessage(ctx, "zz-parent", "k0", Message{Role: RoleUser, Content: `"plan"`})
	if err := s.CreateSession(ctx, Session{SessionID: "zz-fork", CreatedBy: "zz-bob", Channel: "web",
		ParentSessionID: "zz-parent", ForkedAtMessageID: at, ForkedBy: "zz-bob"}); err != nil {
		t.Fatal(err)
	}
	report := func(from, upTo int64, text string) ForkReport {
		return ForkReport{ForkSessionID: "zz-fork", ParentSessionID: "zz-parent", ReporterID: "zz-bob", From: from, UpTo: upTo,
			Message: Message{Role: RoleUser, Kind: KindForkReport, Content: `"` + text + `"`, UserID: "zz-bob", Author: "Bob",
				Fork: &ForkRef{SessionID: "zz-fork", Title: "Export", UpToMessageID: upTo}}}
	}

	if f, _ := s.GetSession(ctx, "zz-fork"); f.LastReportedMessageID != 0 || f.LastReportID != 0 || f.LastReportedAt != nil {
		t.Errorf("a fork that never reported: %+v", f)
	}
	id, err := s.AppendForkReport(ctx, report(0, 40, "first"))
	if err != nil {
		t.Fatal(err)
	}
	f, _ := s.GetSession(ctx, "zz-fork")
	if f.LastReportedMessageID != 40 || f.LastReportID != id || f.LastReportedAt == nil {
		t.Errorf("fork after its report: %+v", f)
	}
	msgs, _ := s.LoadMessagesWithID(ctx, "zz-parent")
	if len(msgs) != 2 || msgs[1].ID != id || msgs[1].Key != ForkReportKey("zz-fork", 0, 40) || msgs[1].Kind != KindForkReport ||
		msgs[1].UserID != "zz-bob" || msgs[1].Fork == nil || *msgs[1].Fork != (ForkRef{SessionID: "zz-fork", Title: "Export", UpToMessageID: 40}) {
		t.Errorf("parent's messages %+v", msgs)
	}

	// A retry returns the report posted, and posts nothing.
	if again, err := s.AppendForkReport(ctx, report(0, 40, "first")); err != nil || again != id {
		t.Errorf("retry: %d, %v; want %d", again, err, id)
	}
	// A report started before the first was posted would cover it again.
	if _, err := s.AppendForkReport(ctx, report(0, 45, "overlap")); !errors.Is(err, ErrReportStale) {
		t.Errorf("stale report: %v", err)
	}
	second, err := s.AppendForkReport(ctx, report(40, 50, "second"))
	if err != nil || second <= id {
		t.Fatalf("second report: %d, %v", second, err)
	}
	if f, _ := s.GetSession(ctx, "zz-fork"); f.LastReportedMessageID != 50 || f.LastReportID != second {
		t.Errorf("fork after its second report: %+v", f)
	}
	if msgs, _ := s.LoadMessagesWithID(ctx, "zz-parent"); len(msgs) != 3 {
		t.Errorf("%d messages in the parent, want 3", len(msgs))
	}

	// Bob left the parent: he reports there no more.
	s.RemoveSessionMember(ctx, "zz-parent", "zz-bob")
	if _, err := s.AppendForkReport(ctx, report(50, 60, "third")); !errors.Is(err, ErrReportNotParentMember) {
		t.Errorf("a reporter who left the parent: %v", err)
	}
	s.AddSessionMember(ctx, "zz-parent", "zz-bob", "zz-alice")
	// Bob left the fork while its report was written: he no longer signs
	// what it says, even with other members left in it.
	s.AddSessionMember(ctx, "zz-fork", "zz-alice", "zz-bob")
	s.RemoveSessionMember(ctx, "zz-fork", "zz-bob")
	if _, err := s.AppendForkReport(ctx, report(50, 60, "third")); !errors.Is(err, ErrReportNotForkMember) {
		t.Errorf("a reporter who left the fork: %v", err)
	}
	s.AddSessionMember(ctx, "zz-fork", "zz-bob", "zz-alice")
	if msgs, _ := s.LoadMessagesWithID(ctx, "zz-parent"); len(msgs) != 3 {
		t.Errorf("%d messages in the parent after the refusals, want 3", len(msgs))
	}

	// The parent is deleted: the fork has nowhere to report, and the
	// parent's messages are gone with it.
	if err := s.DeleteSession(ctx, "zz-parent"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppendForkReport(ctx, report(50, 60, "third")); !errors.Is(err, ErrReportParentGone) {
		t.Errorf("parent deleted: %v", err)
	}
	var n int
	s.db.QueryRow("SELECT count(*) FROM messages WHERE session_id = 'zz-parent'").Scan(&n)
	if n != 0 {
		t.Errorf("%d messages left in the deleted parent", n)
	}

	// The fork is deleted: told apart from a parent gone.
	if err := s.DeleteSession(ctx, "zz-fork"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppendForkReport(ctx, report(50, 60, "third")); !errors.Is(err, ErrForkGone) {
		t.Errorf("fork deleted: %v", err)
	}
}

// A report posted while its parent is deleted: whichever goes first, the
// deletion takes every message of the parent with it, and the report is
// either refused or gone with them. Never a report left behind.
func TestForkReportRacesParentDeletion(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	cleanup := func() {
		s.db.Exec("DELETE FROM messages WHERE session_id LIKE 'zz-race-%'")
		s.db.Exec("DELETE FROM sessions WHERE session_id LIKE 'zz-race-f%'")
		s.db.Exec("DELETE FROM sessions WHERE session_id LIKE 'zz-race-%'")
		s.db.Exec("DELETE FROM users WHERE id = 'zz-race-alice'")
	}
	cleanup()
	t.Cleanup(cleanup)
	if err := s.CreateUser(ctx, User{ID: "zz-race-alice", Email: "zz-race-alice@example.com", Role: UserRoleStandard, PasswordHash: "h"}); err != nil {
		t.Fatal(err)
	}

	posted, refused := 0, 0
	for i := range 20 {
		parent, fork := fmt.Sprintf("zz-race-p%d", i), fmt.Sprintf("zz-race-f%d", i)
		if err := s.CreateSession(ctx, Session{SessionID: parent, CreatedBy: "zz-race-alice", Channel: "web"}); err != nil {
			t.Fatal(err)
		}
		at, _ := s.AppendMessage(ctx, parent, "k0", Message{Role: RoleUser, Content: `"plan"`})
		if err := s.CreateSession(ctx, Session{SessionID: fork, CreatedBy: "zz-race-alice", Channel: "web",
			ParentSessionID: parent, ForkedAtMessageID: at, ForkedBy: "zz-race-alice"}); err != nil {
			t.Fatal(err)
		}

		start := make(chan struct{})
		reportErr, deleteErr := make(chan error, 1), make(chan error, 1)
		go func() {
			<-start
			_, err := s.AppendForkReport(ctx, ForkReport{ForkSessionID: fork, ParentSessionID: parent, ReporterID: "zz-race-alice", UpTo: 10,
				Message: Message{Role: RoleUser, Kind: KindForkReport, Content: `"report"`, UserID: "zz-race-alice"}})
			reportErr <- err
		}()
		go func() {
			<-start
			deleteErr <- s.DeleteSession(ctx, parent)
		}()
		close(start)

		if err := <-deleteErr; err != nil {
			t.Fatalf("round %d: delete: %v", i, err)
		}
		switch err := <-reportErr; {
		case err == nil:
			posted++
		case errors.Is(err, ErrReportParentGone):
			refused++
		default:
			t.Fatalf("round %d: report: %v", i, err)
		}
		var n int
		s.db.QueryRow("SELECT count(*) FROM messages WHERE session_id = $1", parent).Scan(&n)
		if n != 0 {
			t.Fatalf("round %d: %d messages left in the deleted parent", i, n)
		}
		if f, _ := s.GetSession(ctx, fork); f == nil || f.ParentSessionID != "" {
			t.Fatalf("round %d: fork after its parent's deletion: %+v", i, f)
		}
	}
	t.Logf("%d reports posted then deleted, %d refused", posted, refused)
}

// A reporter who leaves the fork while its report is posted waits for it:
// the membership rows the report checked are held until it commits, and the
// report is signed by a member.
func TestForkReportHoldsItsReporterRows(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	cleanup := func() {
		s.db.Exec("DELETE FROM messages WHERE session_id LIKE 'zz-hold-%'")
		s.db.Exec("DELETE FROM sessions WHERE session_id = 'zz-hold-fork'")
		s.db.Exec("DELETE FROM sessions WHERE session_id LIKE 'zz-hold-%'")
		s.db.Exec("DELETE FROM users WHERE id = 'zz-hold-bob'")
	}
	cleanup()
	t.Cleanup(cleanup)
	if err := s.CreateUser(ctx, User{ID: "zz-hold-bob", Email: "zz-hold-bob@example.com", Role: UserRoleStandard, PasswordHash: "h"}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateSession(ctx, Session{SessionID: "zz-hold-parent", CreatedBy: "zz-hold-bob", Channel: "web"}); err != nil {
		t.Fatal(err)
	}
	at, _ := s.AppendMessage(ctx, "zz-hold-parent", "k0", Message{Role: RoleUser, Content: `"plan"`})
	if err := s.CreateSession(ctx, Session{SessionID: "zz-hold-fork", CreatedBy: "zz-hold-bob", Channel: "web",
		ParentSessionID: "zz-hold-parent", ForkedAtMessageID: at, ForkedBy: "zz-hold-bob"}); err != nil {
		t.Fatal(err)
	}

	// waiting is how many statements of this database wait on a lock.
	waiting := func(n int) bool {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			var got int
			s.db.QueryRow("SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'").Scan(&got)
			if got == n {
				return true
			}
			time.Sleep(20 * time.Millisecond)
		}
		return false
	}

	// The report is held after its checks: its message's key is being
	// written by another transaction, which the insert waits for.
	blocker, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback()
	if _, err := blocker.Exec("INSERT INTO messages (session_id, msg_key, data) VALUES ($1, $2, '{}')",
		"zz-hold-parent", ForkReportKey("zz-hold-fork", 0, 10)); err != nil {
		t.Fatal(err)
	}
	type result struct {
		id  int64
		err error
	}
	reported := make(chan result, 1)
	go func() {
		id, err := s.AppendForkReport(ctx, ForkReport{ForkSessionID: "zz-hold-fork", ParentSessionID: "zz-hold-parent", ReporterID: "zz-hold-bob", UpTo: 10,
			Message: Message{Role: RoleUser, Kind: KindForkReport, Content: `"report"`, UserID: "zz-hold-bob"}})
		reported <- result{id, err}
	}()
	if !waiting(1) {
		t.Fatal("the report never waited on its message's key")
	}
	left := make(chan error, 1)
	go func() { left <- s.RemoveSessionMember(ctx, "zz-hold-fork", "zz-hold-bob") }()
	if !waiting(2) {
		t.Fatal("leaving the fork did not wait for the report")
	}
	select {
	case err := <-left:
		t.Fatalf("the reporter left during the report: %v", err)
	default:
	}

	blocker.Rollback()
	r := <-reported
	if r.err != nil || r.id == 0 {
		t.Fatalf("report: %d, %v", r.id, r.err)
	}
	if err := <-left; err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.IsSessionMember(ctx, "zz-hold-fork", "zz-hold-bob"); ok {
		t.Error("the reporter is still a member of the fork after leaving")
	}
}
