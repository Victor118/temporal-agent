package store

import (
	"context"
	"errors"
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
