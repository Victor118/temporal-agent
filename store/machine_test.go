package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
)

// A token's rotation in two steps: the machine gets the next token, keeps
// the one it presented until it confirms it wrote the next one, and every
// token replaced for good is retired.
func TestRotateTokens(t *testing.T) {
	for _, c := range []struct {
		name                     string
		current, prev, presented string
		wantCurrent, wantPrev    string
		wantRetire               []string
		wantOK                   bool
	}{
		{"current, nothing pending", "a", "", "a", "n", "a", nil, true},
		{"current, its confirmation lost", "b", "a", "b", "n", "b", []string{"a"}, true},
		{"pending: the next one never written", "b", "a", "a", "n", "a", []string{"b"}, true},
		{"neither", "b", "a", "x", "b", "a", nil, false},
		{"nothing presented", "b", "", "", "b", "", nil, false},
		{"revoked machine", "", "", "", "", "", nil, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			cur, prev, retire, ok := rotateTokens(c.current, c.prev, c.presented, "n")
			if cur != c.wantCurrent || prev != c.wantPrev || !slices.Equal(retire, c.wantRetire) || ok != c.wantOK {
				t.Errorf("got %q %q %v %v", cur, prev, retire, ok)
			}
		})
	}
}

func TestConfirmToken(t *testing.T) {
	if prev, retire, ok := confirmToken("b", "a", "b"); prev != "" || !slices.Equal(retire, []string{"a"}) || !ok {
		t.Errorf("confirm: %q %v %v", prev, retire, ok)
	}
	if prev, retire, ok := confirmToken("b", "", "b"); prev != "" || retire != nil || !ok {
		t.Errorf("confirm twice: %q %v %v", prev, retire, ok)
	}
	if prev, retire, ok := confirmToken("b", "a", "a"); prev != "a" || retire != nil || ok {
		t.Errorf("confirm with the old token: %q %v %v", prev, retire, ok)
	}
}

func TestChooseMachine(t *testing.T) {
	up := func(id string, prio, max, open int) machineChoice {
		return machineChoice{ID: id, Priority: prio, Max: max, Open: open, Online: true, Can: true}
	}
	for _, c := range []struct {
		name    string
		choices []machineChoice
		want    string
	}{
		{"none", nil, ""},
		{"highest priority first", []machineChoice{up("a", 0, 4, 0), up("b", 1, 4, 3)}, "b"},
		{"then the least busy", []machineChoice{up("a", 0, 4, 2), up("b", 0, 4, 1)}, "b"},
		{"then by ID", []machineChoice{up("b", 0, 2, 0), up("a", 0, 2, 0)}, "a"},
		{"full is skipped", []machineChoice{up("a", 5, 1, 1), up("b", 0, 1, 0)}, "b"},
		{"all full", []machineChoice{up("a", 0, 1, 1), up("b", 0, 2, 2)}, ""},
		{"offline, paused, without the capability", []machineChoice{
			{ID: "a", Max: 1, Can: true},
			{ID: "b", Max: 1, Online: true, Paused: true, Can: true},
			{ID: "c", Max: 1, Online: true},
		}, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := chooseMachine(c.choices); got != c.want {
				t.Errorf("chose %q, want %q", got, c.want)
			}
		})
	}
}

// machineUser creates a user for the machine tests, removed with its
// machines and enrollments when the test ends.
func machineUser(t *testing.T, s *PostgresStore, id string) {
	t.Helper()
	clean := func() { s.db.Exec("DELETE FROM users WHERE id = $1", id) }
	clean()
	t.Cleanup(clean)
	if err := s.CreateUser(context.Background(), User{ID: id, Email: id + "@example.com", Role: UserRoleStandard, PasswordHash: "h"}); err != nil {
		t.Fatal(err)
	}
}

// enrollMachine enrolls a machine for userID through an enrollment token.
func enrollMachine(t *testing.T, s *PostgresStore, userID, id, tokenHash string, caps []string, maxDirectives int) Machine {
	t.Helper()
	ctx := context.Background()
	secret := "secret-" + id
	if err := s.CreateMachineEnrollment(ctx, MachineEnrollment{ID: "e-" + id, Kind: EnrollmentToken, SecretHash: secret,
		ExpiresAt: time.Now().Add(time.Minute), ApprovedBy: userID}); err != nil {
		t.Fatal(err)
	}
	m, err := s.RedeemMachineEnrollment(ctx, EnrollmentToken, secret,
		MachineInfo{Name: id, OS: "linux", Capabilities: caps, MaxDirectives: maxDirectives}, Machine{ID: id}, tokenHash, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestMachineEnrollment_Device(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	machineUser(t, s, "zz-mach-alice")
	now := time.Now()
	req := MachineEnrollment{ID: "zz-mach-e1", Kind: EnrollmentDevice, SecretHash: "zz-mach-secret", UserCode: "ZZ2345",
		Info: MachineInfo{Name: "maison", OS: "linux", Capabilities: []string{"echo"}, MaxDirectives: 3}, ClientAddr: "192.0.2.1",
		ExpiresAt: now.Add(10 * time.Minute)}
	t.Cleanup(func() { s.db.Exec("DELETE FROM machine_enrollments WHERE id LIKE 'zz-mach-%'") })
	s.db.Exec("DELETE FROM machine_enrollments WHERE id LIKE 'zz-mach-%' OR user_code = 'ZZ2345'")
	if err := s.CreateMachineEnrollment(ctx, req); err != nil {
		t.Fatal(err)
	}
	other := req
	other.ID, other.SecretHash = "zz-mach-e2", "zz-mach-other"
	if err := s.CreateMachineEnrollment(ctx, other); !errors.Is(err, ErrUserCodeTaken) {
		t.Errorf("same user code: %v", err)
	}
	if n, err := s.CountPendingDeviceRequests(ctx, now); err != nil || n < 1 {
		t.Errorf("pending: %d %v", n, err)
	}

	tok := MachineInfo{Name: "ignored"}
	if _, err := s.RedeemMachineEnrollment(ctx, EnrollmentDevice, req.SecretHash, tok, Machine{ID: "zz-mach-m"}, "zz-mach-h", now); !errors.Is(err, ErrEnrollmentPending) {
		t.Errorf("before approval: %v", err)
	}
	found, err := s.FindDeviceRequest(ctx, "ZZ2345", now)
	if err != nil || found == nil || found.Info.Name != "maison" || found.ClientAddr != "192.0.2.1" || !slices.Equal(found.Info.Capabilities, []string{"echo"}) {
		t.Fatalf("find: %+v %v", found, err)
	}
	if err := s.ApproveDeviceRequest(ctx, found.ID, "AAAAAA", "zz-mach-alice", now); !errors.Is(err, ErrEnrollmentUnknown) {
		t.Errorf("approve with another code: %v", err)
	}
	if err := s.ApproveDeviceRequest(ctx, found.ID, "ZZ2345", "zz-mach-alice", now); err != nil {
		t.Fatal(err)
	}
	if err := s.ApproveDeviceRequest(ctx, found.ID, "ZZ2345", "zz-mach-alice", now); !errors.Is(err, ErrEnrollmentUnknown) {
		t.Errorf("approved twice: %v", err)
	}
	if again, _ := s.FindDeviceRequest(ctx, "ZZ2345", now); again != nil {
		t.Error("an approved request is still found by its code")
	}
	m, err := s.RedeemMachineEnrollment(ctx, EnrollmentDevice, req.SecretHash, tok, Machine{ID: "zz-mach-m"}, "zz-mach-h", now)
	if err != nil || m.UserID != "zz-mach-alice" || m.Name != "maison" || m.MaxDirectives != 3 {
		t.Fatalf("redeem: %+v %v", m, err)
	}
	if _, err := s.RedeemMachineEnrollment(ctx, EnrollmentDevice, req.SecretHash, tok, Machine{ID: "zz-mach-m2"}, "zz-mach-h2", now); !errors.Is(err, ErrEnrollmentUnknown) {
		t.Errorf("redeemed twice: %v", err)
	}

	// Expired, before or after an approval.
	late := MachineEnrollment{ID: "zz-mach-e3", Kind: EnrollmentToken, SecretHash: "zz-mach-late", ApprovedBy: "zz-mach-alice",
		ExpiresAt: now.Add(time.Minute)}
	if err := s.CreateMachineEnrollment(ctx, late); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RedeemMachineEnrollment(ctx, EnrollmentToken, late.SecretHash, tok, Machine{ID: "zz-mach-m3"}, "zz-mach-h3", now.Add(2*time.Minute)); !errors.Is(err, ErrEnrollmentExpired) {
		t.Errorf("expired: %v", err)
	}
	if err := s.DeleteExpiredEnrollments(ctx, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RedeemMachineEnrollment(ctx, EnrollmentToken, late.SecretHash, tok, Machine{ID: "zz-mach-m3"}, "zz-mach-h3", now); !errors.Is(err, ErrEnrollmentUnknown) {
		t.Errorf("deleted: %v", err)
	}
}

func TestMachineToken_RotationAndRevocation(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	machineUser(t, s, "zz-mach-bob")
	enrollMachine(t, s, "zz-mach-bob", "zz-mach-rot", "zz-t1", []string{"echo"}, 1)

	use := func(hash string) TokenUse {
		t.Helper()
		m, u, err := s.MachineByToken(ctx, hash)
		if err != nil {
			t.Fatal(err)
		}
		if u != TokenUnknown && (m == nil || m.ID != "zz-mach-rot") {
			t.Fatalf("%s: machine %+v", hash, m)
		}
		return u
	}
	if use("zz-t1") != TokenCurrent || use("nope") != TokenUnknown {
		t.Fatal("before rotation")
	}
	if err := s.RotateMachineToken(ctx, "zz-mach-rot", "zz-t1", "zz-t2"); err != nil {
		t.Fatal(err)
	}
	if use("zz-t1") != TokenPending || use("zz-t2") != TokenCurrent {
		t.Fatal("rotation started")
	}
	if err := s.ConfirmMachineToken(ctx, "zz-mach-rot", "zz-t1"); !errors.Is(err, ErrTokenRefused) {
		t.Errorf("confirm with the old token: %v", err)
	}
	if err := s.ConfirmMachineToken(ctx, "zz-mach-rot", "zz-t2"); err != nil {
		t.Fatal(err)
	}
	if use("zz-t1") != TokenRetired || use("zz-t2") != TokenCurrent {
		t.Fatal("rotation confirmed")
	}
	if err := s.RotateMachineToken(ctx, "zz-mach-rot", "zz-t1", "zz-t3"); !errors.Is(err, ErrTokenRefused) {
		t.Errorf("rotation from a retired token: %v", err)
	}

	// A crash before the next token is written: the pending one connects
	// again, and the next token, never written, is retired.
	if err := s.RotateMachineToken(ctx, "zz-mach-rot", "zz-t2", "zz-t3"); err != nil {
		t.Fatal(err)
	}
	if err := s.RotateMachineToken(ctx, "zz-mach-rot", "zz-t2", "zz-t4"); err != nil {
		t.Fatal(err)
	}
	if use("zz-t3") != TokenRetired || use("zz-t2") != TokenPending || use("zz-t4") != TokenCurrent {
		t.Fatal("rotation after a crash")
	}

	// Connected, with a directive open: revoking closes it and refuses the
	// tokens.
	if _, err := s.MachineConnected(ctx, "zz-mach-rot", "gw", "192.0.2.9", MachineInfo{OS: "linux", Capabilities: []string{"echo"}, MaxDirectives: 2}); err != nil {
		t.Fatal(err)
	}
	d, _, err := s.PickMachine(ctx, pick("zz-mach-bob", "zz-rev-run", "c1", time.Now().Add(-time.Minute)))
	if err != nil {
		t.Fatal(err)
	}
	closed, err := s.RevokeMachine(ctx, "zz-mach-rot", "test")
	if err != nil || len(closed) != 1 || closed[0].ID != d.ID || closed[0].State != DirectiveRevoked {
		t.Fatalf("revoke: %+v %v", closed, err)
	}
	if again, err := s.RevokeMachine(ctx, "zz-mach-rot", "test"); err != nil || len(again) != 0 {
		t.Errorf("revoked twice: %+v %v", again, err)
	}
	if use("zz-t4") != TokenRetired || use("zz-t2") != TokenRetired {
		t.Error("tokens of a revoked machine")
	}
	m, _ := s.GetMachine(ctx, "zz-mach-rot")
	if m == nil || m.RevokedAt == nil || m.RevokedReason != "test" || m.ConnectedTo != "" {
		t.Errorf("revoked machine: %+v", m)
	}
	if _, err := s.MachineConnected(ctx, "zz-mach-rot", "gw", "", MachineInfo{MaxDirectives: 1}); !errors.Is(err, ErrMachineNotFound) {
		t.Errorf("a revoked machine connects: %v", err)
	}
}

func pick(userID, run, call string, seenAfter time.Time) PickRequest {
	now := time.Now()
	return PickRequest{DirectiveID: fmt.Sprintf("zz-d-%s-%s", run, call), UserID: userID, Capabilities: []string{"echo"}, Kind: "echo",
		Input: json.RawMessage(`{"text":"hi"}`), WorkflowID: "wf", RunID: run, CallKey: call,
		HandoffBy: now.Add(time.Minute), Deadline: now.Add(time.Hour), SeenAfter: seenAfter}
}

func TestPickMachine(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	machineUser(t, s, "zz-mach-carol")
	seen := time.Now().Add(-time.Minute)
	if _, _, err := s.PickMachine(ctx, pick("zz-mach-carol", "zz-r1", "c", seen)); !errors.Is(err, ErrNoMachine) {
		t.Fatalf("no machine: %v", err)
	}
	enrollMachine(t, s, "zz-mach-carol", "zz-mach-low", "zz-low", []string{"echo"}, 1)
	enrollMachine(t, s, "zz-mach-carol", "zz-mach-high", "zz-high", []string{"echo"}, 1)
	if _, _, err := s.PickMachine(ctx, pick("zz-mach-carol", "zz-r1", "c", seen)); !errors.Is(err, ErrNoMachine) {
		t.Fatalf("machines offline: %v", err)
	}
	for _, id := range []string{"zz-mach-low", "zz-mach-high"} {
		if _, err := s.MachineConnected(ctx, id, "gw", "", MachineInfo{Capabilities: []string{"echo"}, MaxDirectives: 1}); err != nil {
			t.Fatal(err)
		}
	}
	s.db.Exec("UPDATE machines SET priority = 5 WHERE id = 'zz-mach-high'")
	if _, _, err := s.PickMachine(ctx, PickRequest{UserID: "zz-mach-carol", Capabilities: []string{"claude-code"}, RunID: "zz-r0", CallKey: "c", SeenAfter: seen}); !errors.Is(err, ErrNoMachine) {
		t.Fatalf("no machine with the capability: %v", err)
	}
	d1, m1, err := s.PickMachine(ctx, pick("zz-mach-carol", "zz-r1", "c", seen))
	if err != nil || m1.ID != "zz-mach-high" || d1.State != DirectiveReserved || d1.MachineID != m1.ID {
		t.Fatalf("first pick: %+v %+v %v", d1, m1, err)
	}
	// Made again: the same directive, not a second one.
	again, m, err := s.PickMachine(ctx, pick("zz-mach-carol", "zz-r1", "c", seen))
	if err != nil || again.ID != d1.ID || m.ID != m1.ID {
		t.Fatalf("pick again: %+v %v", again, err)
	}
	d2, m2, err := s.PickMachine(ctx, pick("zz-mach-carol", "zz-r2", "c", seen))
	if err != nil || m2.ID != "zz-mach-low" {
		t.Fatalf("second pick: %+v %+v %v", d2, m2, err)
	}
	if _, _, err := s.PickMachine(ctx, pick("zz-mach-carol", "zz-r3", "c", seen)); !errors.Is(err, ErrNoMachine) {
		t.Fatalf("both full: %v", err)
	}

	// Started, sent once, ended once.
	started, err := s.StartDirective(ctx, d1.ID, []byte("tok"), "5", time.Now().Add(time.Hour))
	if err != nil || started.State != DirectiveRunning || string(started.TaskToken) != "tok" {
		t.Fatalf("start: %+v %v", started, err)
	}
	if _, err := s.StartDirective(ctx, d1.ID, []byte("tok"), "5", time.Now().Add(time.Hour)); err != nil {
		t.Errorf("start again: %v", err)
	}
	if sent, err := s.MarkDirectiveSent(ctx, d1.ID, "conn-1"); !sent || err != nil {
		t.Errorf("sent: %v %v", sent, err)
	}
	if sent, _ := s.MarkDirectiveSent(ctx, d1.ID, "conn-2"); sent {
		t.Error("sent twice")
	}
	if open, err := s.OpenDirectives(ctx, "zz-mach-high"); err != nil || len(open) != 1 || open[0].SentConn != "conn-1" {
		t.Errorf("open: %+v %v", open, err)
	}
	if err := s.SaveDirectiveResult(ctx, d1.ID, json.RawMessage(`{"text":"hi"}`)); err != nil {
		t.Fatal(err)
	}
	pending, err := s.PendingDirectiveResults(ctx)
	if err != nil || !slices.ContainsFunc(pending, func(d Directive) bool { return d.ID == d1.ID }) ||
		slices.ContainsFunc(pending, func(d Directive) bool { return d.ID == d2.ID }) {
		t.Errorf("pending results: %+v %v", pending, err)
	}
	if ok, err := s.CloseDirective(ctx, d1.ID, DirectiveCompleted, ""); !ok || err != nil {
		t.Fatalf("close: %v %v", ok, err)
	}
	if ok, _ := s.CloseDirective(ctx, d1.ID, DirectiveFailed, "late"); ok {
		t.Error("closed twice")
	}
	if pending, _ := s.PendingDirectiveResults(ctx); slices.ContainsFunc(pending, func(d Directive) bool { return d.ID == d1.ID }) {
		t.Error("a closed directive's result is still pending")
	}
	if _, err := s.StartDirective(ctx, d1.ID, []byte("tok2"), "5", time.Now()); !errors.Is(err, ErrDirectiveClosed) {
		t.Errorf("start a closed one: %v", err)
	}
	if err := s.SaveDirectiveResult(ctx, d1.ID, json.RawMessage(`{}`)); !errors.Is(err, ErrDirectiveClosed) {
		t.Errorf("result of a closed one: %v", err)
	}
	got, _ := s.GetDirective(ctx, d1.ID)
	if got == nil || got.State != DirectiveCompleted || string(got.Result) != `{"text": "hi"}` || got.ClosedAt == nil {
		t.Errorf("closed directive: %+v", got)
	}
	ms, err := s.ListMachines(ctx, "zz-mach-carol")
	if err != nil || len(ms) != 2 {
		t.Fatalf("list: %+v %v", ms, err)
	}
	for _, m := range ms {
		if want := map[string]int{"zz-mach-high": 0, "zz-mach-low": 1}[m.ID]; m.OpenDirectives != want {
			t.Errorf("%s: %d open, want %d", m.ID, m.OpenDirectives, want)
		}
	}

	// Paused, or not heard from lately: not chosen.
	s.db.Exec("UPDATE machines SET paused = TRUE WHERE id = 'zz-mach-high'")
	if _, _, err := s.PickMachine(ctx, pick("zz-mach-carol", "zz-r4", "c", seen)); !errors.Is(err, ErrNoMachine) {
		t.Errorf("paused: %v", err)
	}
	s.db.Exec("UPDATE machines SET paused = FALSE WHERE id = 'zz-mach-high'")
	if _, _, err := s.PickMachine(ctx, pick("zz-mach-carol", "zz-r4", "c", time.Now().Add(time.Minute))); !errors.Is(err, ErrNoMachine) {
		t.Errorf("not seen lately: %v", err)
	}
	if err := s.TouchMachines(ctx, []string{"zz-mach-high"}); err != nil {
		t.Fatal(err)
	}
	if err := s.ResetMachineConnections(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.PickMachine(ctx, pick("zz-mach-carol", "zz-r4", "c", seen)); !errors.Is(err, ErrNoMachine) {
		t.Errorf("after a gateway's restart: %v", err)
	}

	// The sweep: a reservation past its handoff, a run past its deadline.
	if _, err := s.MachineConnected(ctx, "zz-mach-high", "gw", "", MachineInfo{Capabilities: []string{"echo"}, MaxDirectives: 2}); err != nil {
		t.Fatal(err)
	}
	r5, _, err := s.PickMachine(ctx, pick("zz-mach-carol", "zz-r5", "c", seen))
	if err != nil {
		t.Fatal(err)
	}
	r6, _, err := s.PickMachine(ctx, pick("zz-mach-carol", "zz-r6", "c", seen))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.StartDirective(ctx, r6.ID, []byte("t6"), "5", time.Now().Add(30*time.Second)); err != nil {
		t.Fatal(err)
	}
	swept, err := s.SweepDirectives(ctx, time.Now().Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	states := map[string]string{}
	for _, d := range swept {
		states[d.ID] = d.State
	}
	if states[r5.ID] != DirectiveOrphaned || states[r6.ID] != DirectiveExpired || states[d2.ID] != DirectiveOrphaned {
		t.Errorf("swept: %v", states)
	}
}

// Two picks at once for a machine's last slot: one gets it.
func TestPickMachine_ConcurrentPicksKeepTheCap(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	machineUser(t, s, "zz-mach-dave")
	enrollMachine(t, s, "zz-mach-dave", "zz-mach-one", "zz-one", []string{"echo"}, 2)
	if _, err := s.MachineConnected(ctx, "zz-mach-one", "gw", "", MachineInfo{Capabilities: []string{"echo"}, MaxDirectives: 2}); err != nil {
		t.Fatal(err)
	}
	seen := time.Now().Add(-time.Minute)
	if _, _, err := s.PickMachine(ctx, pick("zz-mach-dave", "zz-c0", "c", seen)); err != nil {
		t.Fatal(err)
	}
	for round := range 5 {
		var wg sync.WaitGroup
		var mu sync.Mutex
		var got, none int
		for i := range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _, err := s.PickMachine(ctx, pick("zz-mach-dave", fmt.Sprintf("zz-c%d-%d", round, i), "c", seen))
				mu.Lock()
				defer mu.Unlock()
				switch {
				case err == nil:
					got++
				case errors.Is(err, ErrNoMachine):
					none++
				default:
					t.Error(err)
				}
			}()
		}
		wg.Wait()
		if got != 1 || none != 7 {
			t.Fatalf("round %d: %d picked, %d refused", round, got, none)
		}
		// Free the slot for the next round.
		s.db.Exec(`UPDATE machine_directives SET state = 'completed' WHERE machine_id = 'zz-mach-one' AND run_id <> 'zz-c0'`)
	}
}

func TestMachineSettingsAndStatus(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	machineUser(t, s, "zz-mach-erin")
	enrollMachine(t, s, "zz-mach-erin", "zz-mach-set", "zz-set", []string{"echo"}, 2)
	if _, err := s.MachineConnected(ctx, "zz-mach-set", "gw", "", MachineInfo{Capabilities: []string{"echo"}, MaxDirectives: 2, ClaudeCode: "absent"}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateMachineStatus(ctx, "zz-mach-set", []string{"echo", "claude-code"}, "ok"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetMachinePaused(ctx, "someone-else", "zz-mach-set", true); !errors.Is(err, ErrMachineNotFound) {
		t.Errorf("paused by another user: %v", err)
	}
	if err := s.SetMachinePriority(ctx, "zz-mach-erin", "zz-mach-set", 7); err != nil {
		t.Fatal(err)
	}
	seen := time.Now().Add(-time.Minute)
	req := pick("zz-mach-erin", "zz-set-run", "c", seen)
	req.Capabilities, req.Kind = []string{"claude-code"}, "analyze_repo"
	req.SessionID, req.Participant, req.Agent = "sess-1", "jarvis", "Jarvis"
	req.TurnKey, req.CallID, req.AgentID = "m7.jarvis", "call-1", "jarvis"
	// Every capability is needed: this machine does not push.
	pushing := req
	pushing.Capabilities, pushing.RunID = []string{"claude-code", "git-push"}, "zz-set-push"
	if _, _, err := s.PickMachine(ctx, pushing); !errors.Is(err, ErrNoMachine) {
		t.Errorf("a machine without git-push picked: %v", err)
	}
	d, _, err := s.PickMachine(ctx, req)
	if err != nil || d.SessionID != "sess-1" || d.Participant != "jarvis" || d.Agent != "Jarvis" ||
		d.TurnKey != "m7.jarvis" || d.CallID != "call-1" || d.AgentID != "jarvis" {
		t.Fatalf("pick: %+v %v", d, err)
	}
	ms, err := s.ListMachines(ctx, "zz-mach-erin")
	if err != nil || len(ms) != 1 {
		t.Fatalf("list: %+v %v", ms, err)
	}
	m := ms[0]
	if m.ClaudeCode != "ok" || !m.Can("claude-code") || m.Priority != 7 || m.OpenDirectives != 1 || !slices.Equal(m.OpenKinds, []string{"analyze_repo"}) {
		t.Errorf("machine %+v", m)
	}
	if err := s.SetMachinePaused(ctx, "zz-mach-erin", "zz-mach-set", true); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.PickMachine(ctx, pick("zz-mach-erin", "zz-set-run2", "c", seen)); !errors.Is(err, ErrNoMachine) {
		t.Errorf("a paused machine was picked: %v", err)
	}
}
