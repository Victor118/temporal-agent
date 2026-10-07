package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// llmMachine enrolls and connects a machine of userID's offering its model,
// maxLLM calls at once, and one coding run.
func llmMachine(t *testing.T, s *PostgresStore, userID, id string, maxLLM, priority int) {
	t.Helper()
	ctx := context.Background()
	enrollMachine(t, s, userID, id, "h-"+id, []string{"llm", "claude-code"}, 1)
	if _, err := s.MachineConnected(ctx, id, "gw", "", MachineInfo{Capabilities: []string{"llm", "claude-code"}, MaxDirectives: 1,
		MaxLLM: maxLLM, LLMProvider: "anthropic", LLMModel: "claude-x", LLMState: "ok"}); err != nil {
		t.Fatal(err)
	}
	if priority != 0 {
		if err := s.SetMachinePriority(ctx, userID, id, priority); err != nil {
			t.Fatal(err)
		}
	}
}

func llmCall(userID, machineID, run, key string, seen time.Time) LLMDirectiveRequest {
	return LLMDirectiveRequest{DirectiveID: run + "-" + key, MachineID: machineID, UserID: userID,
		Input: json.RawMessage(`{"memory_version":3}`), WorkflowID: "wf-" + run, RunID: run, ActivityID: "1", CallKey: key,
		TaskToken: []byte("tok-" + key), Deadline: time.Now().Add(3 * time.Minute), SeenAfter: seen,
		SessionID: "sess", TurnKey: "m1.jarvis", AgentID: "jarvis"}
}

// A turn's model: the user's machine with its model, the highest priority,
// under its cap of calls, which the coding runs do not share; a machine set
// aside until it connects again; the ones the turn excluded.
func TestLLMMachines(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	machineUser(t, s, "zz-llm-alice")
	seen := time.Now().Add(-time.Minute)

	if _, err := s.ChooseLLMMachine(ctx, "zz-llm-alice", nil, seen); !errors.Is(err, ErrNoMachine) {
		t.Errorf("no machine: %v", err)
	}
	llmMachine(t, s, "zz-llm-alice", "zz-llm-low", 2, 0)
	llmMachine(t, s, "zz-llm-alice", "zz-llm-high", 1, 5)
	// One without its model is never chosen for it.
	enrollMachine(t, s, "zz-llm-alice", "zz-llm-none", "h-none", []string{"claude-code"}, 1)
	if _, err := s.MachineConnected(ctx, "zz-llm-none", "gw", "", MachineInfo{Capabilities: []string{"claude-code"}, MaxDirectives: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetMachinePriority(ctx, "zz-llm-alice", "zz-llm-none", 10); err != nil {
		t.Fatal(err)
	}

	m, err := s.ChooseLLMMachine(ctx, "zz-llm-alice", nil, seen)
	if err != nil || m.ID != "zz-llm-high" || m.LLMModel != "claude-x" || m.MaxLLM != 1 || m.LLMState != "ok" {
		t.Fatalf("chose %+v %v", m, err)
	}
	if m, err := s.ChooseLLMMachine(ctx, "zz-llm-alice", []string{"zz-llm-high"}, seen); err != nil || m.ID != "zz-llm-low" {
		t.Errorf("high excluded: %s %v", m.ID, err)
	}

	// A call: running at once, its input kept, under the cap.
	d, dm, err := s.CreateLLMDirective(ctx, llmCall("zz-llm-alice", "zz-llm-high", "zz-llm-r1", "llm:0:1", seen))
	if err != nil || d.State != DirectiveRunning || d.Kind != "llm" || string(d.TaskToken) != "tok-llm:0:1" || dm.ID != "zz-llm-high" || d.SentConn != "" {
		t.Fatalf("create: %+v %v", d, err)
	}
	var in map[string]int
	if json.Unmarshal(d.Input, &in); in["memory_version"] != 3 {
		t.Errorf("input %s", d.Input)
	}
	// The same key again: an attempt is never made twice.
	if _, _, err := s.CreateLLMDirective(ctx, llmCall("zz-llm-alice", "zz-llm-high", "zz-llm-r1", "llm:0:1", seen)); !errors.Is(err, ErrDirectiveClosed) {
		t.Errorf("same key: %v", err)
	}
	// Its cap reached: another chosen first; a call is busy, not refused.
	if m, err := s.ChooseLLMMachine(ctx, "zz-llm-alice", nil, seen); err != nil || m.ID != "zz-llm-low" {
		t.Errorf("high full: %s %v", m.ID, err)
	}
	if _, _, err := s.CreateLLMDirective(ctx, llmCall("zz-llm-alice", "zz-llm-high", "zz-llm-r1", "llm:0:2", seen)); !errors.Is(err, ErrMachineBusy) ||
		errors.Is(err, ErrMachineUnavailable) {
		t.Errorf("over the cap: %v", err)
	}
	// With no other, the full one is chosen all the same: its calls wait.
	if m, err := s.ChooseLLMMachine(ctx, "zz-llm-alice", []string{"zz-llm-low"}, seen); err != nil || m.ID != "zz-llm-high" {
		t.Errorf("only a full one: %s %v", m.ID, err)
	}
	// The coding runs have their own cap: a call to the model takes none of
	// it (high runs one at a time, and has a call open)…
	pr := pick("zz-llm-alice", "zz-llm-run", "c", seen)
	pr.Capabilities, pr.Kind = []string{"claude-code"}, "analyze_repo"
	if _, pm, err := s.PickMachine(ctx, pr); err != nil || pm.ID != "zz-llm-none" {
		t.Errorf("a run: %s %v", pm.ID, err)
	}
	pr = pick("zz-llm-alice", "zz-llm-run2", "c", seen)
	pr.Capabilities, pr.Kind = []string{"claude-code"}, "analyze_repo"
	if _, pm, err := s.PickMachine(ctx, pr); err != nil || pm.ID != "zz-llm-high" {
		t.Errorf("a run beside a call: %s %v", pm.ID, err)
	}
	// …nor does a run take the calls' cap.
	if _, _, err := s.CreateLLMDirective(ctx, llmCall("zz-llm-alice", "zz-llm-low", "zz-llm-r2", "llm:0:1", seen)); err != nil {
		t.Errorf("a call: %v", err)
	}

	// Another user's machine is not theirs to call.
	machineUser(t, s, "zz-llm-bob")
	if _, _, err := s.CreateLLMDirective(ctx, llmCall("zz-llm-bob", "zz-llm-low", "zz-llm-r3", "llm:0:1", seen)); !errors.Is(err, ErrMachineUnavailable) {
		t.Errorf("another's machine: %v", err)
	}

	// Set aside: not chosen, no call, until it connects again.
	s.db.Exec(`UPDATE machine_directives SET state = 'completed' WHERE machine_id LIKE 'zz-llm-%'`)
	if err := s.SetMachineAside(ctx, "zz-llm-high"); err != nil {
		t.Fatal(err)
	}
	if m, err := s.ChooseLLMMachine(ctx, "zz-llm-alice", nil, seen); err != nil || m.ID != "zz-llm-low" {
		t.Errorf("high set aside: %s %v", m.ID, err)
	}
	if _, _, err := s.CreateLLMDirective(ctx, llmCall("zz-llm-alice", "zz-llm-high", "zz-llm-r4", "llm:0:1", seen)); !errors.Is(err, ErrMachineUnavailable) {
		t.Errorf("a call to a machine set aside: %v", err)
	}
	// Bounded in time: an aside older than the online window is over.
	got, _ := s.GetMachine(ctx, "zz-llm-high")
	if !got.AsideForLLM(seen) || got.AsideForLLM(time.Now().Add(time.Second)) {
		t.Errorf("aside within the window %v, past it %v", got.AsideForLLM(seen), got.AsideForLLM(time.Now().Add(time.Second)))
	}
	// Lifted by a heartbeat of its gateway.
	if err := s.ClearMachineAside(ctx, "zz-llm-high"); err != nil {
		t.Fatal(err)
	}
	if m, err := s.ChooseLLMMachine(ctx, "zz-llm-alice", nil, seen); err != nil || m.ID != "zz-llm-high" {
		t.Errorf("back after a heartbeat: %s %v", m.ID, err)
	}
	// And by its next connection.
	if err := s.SetMachineAside(ctx, "zz-llm-high"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if _, err := s.MachineConnected(ctx, "zz-llm-high", "gw", "", MachineInfo{Capabilities: []string{"llm"}, MaxDirectives: 1, MaxLLM: 1, LLMState: "ok"}); err != nil {
		t.Fatal(err)
	}
	if m, err := s.ChooseLLMMachine(ctx, "zz-llm-alice", nil, seen); err != nil || m.ID != "zz-llm-high" {
		t.Errorf("back after its connection: %s %v", m.ID, err)
	}

	// Its model withdrawn (its key refused): not chosen.
	if err := s.UpdateMachineStatus(ctx, "zz-llm-high", []string{"claude-code"}, "", "refused"); err != nil {
		t.Fatal(err)
	}
	if m, err := s.ChooseLLMMachine(ctx, "zz-llm-alice", nil, seen); err != nil || m.ID != "zz-llm-low" {
		t.Errorf("model withdrawn: %s %v", m.ID, err)
	}
	got, _ = s.GetMachine(ctx, "zz-llm-high")
	if got.LLMState != "refused" || got.Can("llm") {
		t.Errorf("after the refusal: %+v", got)
	}
}
