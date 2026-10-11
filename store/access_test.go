package store

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/victor/temporal-agent/access"
)

// accessGroup creates a group for the test, removed when it ends with
// their rules.
func accessGroup(t *testing.T, s *PostgresStore, id, name string, members ...string) {
	t.Helper()
	ctx := context.Background()
	clean := func() {
		s.db.Exec(`DELETE FROM access_rules WHERE id IN (SELECT rule_id FROM access_rule_groups WHERE group_id = $1)`, id)
		s.db.Exec(`DELETE FROM groups WHERE id = $1`, id)
	}
	clean()
	t.Cleanup(clean)
	if err := s.CreateGroup(ctx, Group{ID: id, Name: name}); err != nil {
		t.Fatal(err)
	}
	for _, m := range members {
		if err := s.AddGroupMember(ctx, id, m); err != nil {
			t.Fatal(err)
		}
	}
}

func TestGroups(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	machineUser(t, s, "zz-acc-alice")
	machineUser(t, s, "zz-acc-bob")
	accessGroup(t, s, "zz-g-sales", "zz Sales", "zz-acc-alice", "zz-acc-bob")

	if err := s.CreateGroup(ctx, Group{ID: "zz-g-other", Name: "ZZ SALES"}); !errors.Is(err, ErrGroupExists) {
		t.Errorf("a second group of the same name: %v", err)
	}
	if err := s.AddGroupMember(ctx, "zz-g-sales", "zz-acc-alice"); err != nil {
		t.Errorf("added twice: %v", err)
	}
	g, err := s.GetGroup(ctx, "zz-g-sales")
	if err != nil || g == nil || g.Name != "zz Sales" || !slices.Equal(g.Members, []string{"zz-acc-alice", "zz-acc-bob"}) {
		t.Fatalf("group %+v, %v", g, err)
	}
	if err := s.RemoveGroupMember(ctx, "zz-g-sales", "zz-acc-bob"); err != nil {
		t.Fatal(err)
	}
	if err := s.RenameGroup(ctx, "zz-g-sales", "zz Ventes"); err != nil {
		t.Fatal(err)
	}
	if g, _ = s.GetGroup(ctx, "zz-g-sales"); g.Name != "zz Ventes" || !slices.Equal(g.Members, []string{"zz-acc-alice"}) {
		t.Errorf("group %+v", g)
	}
	if err := s.RenameGroup(ctx, "zz-g-none", "x"); !errors.Is(err, ErrGroupNotFound) {
		t.Errorf("rename of no group: %v", err)
	}
	// A user deleted leaves the group.
	s.db.Exec(`DELETE FROM users WHERE id = 'zz-acc-alice'`)
	if g, _ = s.GetGroup(ctx, "zz-g-sales"); len(g.Members) != 0 {
		t.Errorf("members after the user's deletion: %v", g.Members)
	}
}

func TestAccessRules(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	machineUser(t, s, "zz-acc-carol")
	accessGroup(t, s, "zz-g-a", "zz A", "zz-acc-carol")
	accessGroup(t, s, "zz-g-b", "zz B")

	id, err := s.CreateAccessRule(ctx, access.Rule{Kind: access.KindMCPServer, Target: "zz-crm", Groups: []string{"zz-g-a", "zz-g-a"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.DeleteAccessRule(ctx, id) })
	r, err := s.GetAccessRule(ctx, id)
	if err != nil || r == nil || r.Kind != access.KindMCPServer || r.Target != "zz-crm" || !slices.Equal(r.Groups, []string{"zz-g-a"}) || r.Users != nil {
		t.Fatalf("rule %+v, %v", r, err)
	}
	if _, err := s.CreateAccessRule(ctx, access.Rule{Kind: "nonsense", Target: "x"}); err == nil {
		t.Error("a rule of an unknown kind created")
	}

	// The only holder of a rule is not deleted; the rules are named.
	err = s.DeleteGroup(ctx, "zz-g-a")
	var sole *SoleHolderError
	if !errors.As(err, &sole) || len(sole.Rules) != 1 || sole.Rules[0].ID != id {
		t.Fatalf("deleting the only holder: %v", err)
	}
	// With another holder, it goes, and leaves the rule.
	if err := s.UpdateAccessRule(ctx, access.Rule{ID: id, Kind: access.KindPattern, Target: "zz_*", Groups: []string{"zz-g-a", "zz-g-b"}, Users: []string{"zz-acc-carol"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteGroup(ctx, "zz-g-a"); err != nil {
		t.Fatalf("deleting a holder among others: %v", err)
	}
	r, _ = s.GetAccessRule(ctx, id)
	if r.Kind != access.KindPattern || r.Target != "zz_*" || !slices.Equal(r.Groups, []string{"zz-g-b"}) || !slices.Equal(r.Users, []string{"zz-acc-carol"}) {
		t.Errorf("rule after the update and the deletion %+v", r)
	}
	// A user deleted leaves the rule: none holds it any more.
	s.db.Exec(`DELETE FROM users WHERE id = 'zz-acc-carol'`)
	s.db.Exec(`DELETE FROM access_rule_groups WHERE rule_id = $1`, id)
	if r, _ = s.GetAccessRule(ctx, id); r.HasHolder() {
		t.Errorf("rule still held: %+v", r)
	}

	if err := s.UpdateAccessRule(ctx, access.Rule{ID: -1, Kind: access.KindPattern, Target: "x"}); !errors.Is(err, ErrRuleNotFound) {
		t.Errorf("update of no rule: %v", err)
	}
	if err := s.DeleteAccessRule(ctx, id); err != nil {
		t.Fatal(err)
	}
	if r, _ = s.GetAccessRule(ctx, id); r != nil {
		t.Errorf("deleted rule read back %+v", r)
	}
}

func TestAccessSnapshot(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	machineUser(t, s, "zz-acc-dan")
	machineUser(t, s, "zz-acc-eve")
	accessGroup(t, s, "zz-g-snap", "zz Snap", "zz-acc-dan")
	s.db.Exec(`UPDATE users SET role = 'admin', disabled_at = NOW() WHERE id = 'zz-acc-eve'`)
	id, err := s.CreateAccessRule(ctx, access.Rule{Kind: access.KindPattern, Target: "agent_zz", Groups: []string{"zz-g-snap"}, Users: []string{"zz-acc-eve"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.DeleteAccessRule(ctx, id) })

	snap, err := s.AccessSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if dan := snap.Users["zz-acc-dan"]; dan.Admin || dan.Disabled || !slices.Equal(dan.Groups, []string{"zz-g-snap"}) {
		t.Errorf("dan %+v", dan)
	}
	if eve := snap.Users["zz-acc-eve"]; !eve.Admin || !eve.Disabled || eve.Groups != nil {
		t.Errorf("eve %+v", eve)
	}
	i := slices.IndexFunc(snap.Rules, func(r access.Rule) bool { return r.ID == id })
	if i < 0 || !slices.Equal(snap.Rules[i].Users, []string{"zz-acc-eve"}) {
		t.Fatalf("rules %+v", snap.Rules)
	}
	if !snap.Allowed("zz-acc-dan", access.Agent("zz")) || snap.Allowed("zz-acc-eve", access.Agent("zz")) {
		t.Error("the snapshot decides wrong")
	}
}

// What the sweep records: a rule that targeted something, then nothing, is
// told once; targeting something again, it may be told again.
func TestAccessRules_UnmatchedAlertOnce(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	id, err := s.CreateAccessRule(ctx, access.Rule{Kind: access.KindMCPServer, Target: "zz-gone"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.DeleteAccessRule(ctx, id) })

	if ok, err := s.ClaimUnmatchedAlert(ctx, id); err != nil || ok {
		t.Errorf("a rule that never targeted anything told: %v %v", ok, err)
	}
	if err := s.SetAccessRulesMatched(ctx, []int64{id}); err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.ClaimUnmatchedAlert(ctx, id); !ok {
		t.Error("first alert not claimed")
	}
	if ok, _ := s.ClaimUnmatchedAlert(ctx, id); ok {
		t.Error("second alert claimed")
	}
	s.SetAccessRulesMatched(ctx, []int64{id})
	if ok, _ := s.ClaimUnmatchedAlert(ctx, id); !ok {
		t.Error("alert after targeting again not claimed")
	}
	// An update starts again: it has not targeted anything yet.
	s.UpdateAccessRule(ctx, access.Rule{ID: id, Kind: access.KindMCPServer, Target: "zz-other"})
	if r, _ := s.GetAccessRule(ctx, id); r.LastMatchedAt != nil || r.UnmatchedAlertedAt != nil {
		t.Errorf("update kept the sweep's marks: %+v", r)
	}
}

func TestListMCPServers(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	clean := func() { s.db.Exec(`DELETE FROM tools WHERE name LIKE 'zzsrv%'`) }
	clean()
	t.Cleanup(clean)
	for _, rec := range []ToolRecord{
		{Name: "zzsrv_a", MCPServer: "zzsrv"},
		{Name: "zzsrv_b", MCPServer: "zzsrv"},
		{Name: "zzsrv_builtin"},
	} {
		rec.TaskQueue, rec.InputSchema, rec.Kind, rec.SchemaHash = "q", []byte(`{}`), "mcp", "h"
		if err := s.UpsertTool(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}
	servers, err := s.ListMCPServers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(slices.DeleteFunc(slices.Clone(servers), func(n string) bool { return n != "zzsrv" })); n != 1 || slices.Contains(servers, "") {
		t.Errorf("servers %v", servers)
	}
}

// A rule write that makes a group its only holder while the group is being
// deleted: the deletion waits for it, and refuses; never a rule left with
// no holder.
func TestDeleteGroup_RaceWithARuleWrite(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	machineUser(t, s, "zz-acc-race")
	accessGroup(t, s, "zz-g-race", "zz Race")
	id, err := s.CreateAccessRule(ctx, access.Rule{Kind: access.KindPattern, Target: "zz_race", Users: []string{"zz-acc-race"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.DeleteAccessRule(ctx, id) })

	// The write, not committed yet: the group becomes the only holder.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM access_rule_users WHERE rule_id = $1`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO access_rule_groups (rule_id, group_id) VALUES ($1, 'zz-g-race')`, id); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.DeleteGroup(ctx, "zz-g-race") }()
	select {
	case err := <-done:
		t.Fatalf("the deletion did not wait for the rule write: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var sole *SoleHolderError
	if err := <-done; !errors.As(err, &sole) {
		t.Errorf("deletion after the write: %v, want the rule named", err)
	}
	if r, _ := s.GetAccessRule(ctx, id); r == nil || !r.HasHolder() {
		t.Errorf("rule %+v left without holder", r)
	}
	if err := s.DeleteGroup(ctx, "zz-g-none"); !errors.Is(err, ErrGroupNotFound) {
		t.Errorf("no such group: %v", err)
	}
}
