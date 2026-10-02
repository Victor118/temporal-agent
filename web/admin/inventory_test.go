package admin

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/victor/temporal-agent/skill"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/taskqueue"
)

func testInputs() Inputs {
	fresh := []taskqueue.Poller{{Identity: "1@w", LastAccess: time.Now()}}
	return Inputs{
		Agents: []store.Agent{
			{ID: "boss", Name: "Boss", Tools: []string{"read_file", "agent_*"}, Skills: []string{"present", "ghost"}},
			{ID: "coder", Name: "Coder", Tools: []string{"read_file", "exec", "nope_*"}},
			{ID: "root", Name: "Root", Tools: []string{"*"}},
			{ID: "mute", Name: "Mute", Tools: []string{}},
		},
		Tools: []store.ToolRecord{
			{Name: "exec", Kind: "activity", TaskQueue: "tools", Sensitive: true},
			{Name: "orphan", Kind: "activity", TaskQueue: "gone"},
			{Name: "read_file", Kind: "activity", TaskQueue: "tools"},
		},
		Queues: map[string]taskqueue.Status{
			"agent": {Queue: "agent", Workflow: fresh},
			"tools": {Queue: "tools", Activity: fresh},
			"gone":  {Queue: "gone"},
		},
		Skills:         []skill.Skill{{Name: "present"}},
		DefaultAgentID: "boss",
		WorkflowQueue:  "agent",
	}
}

func toolNames(av *AgentView) []string {
	var n []string
	for _, t := range av.Tools {
		n = append(n, t.Name)
	}
	return n
}

func hasAlert(inv *Inventory, level, fragment string) bool {
	for _, a := range inv.Alerts {
		if a.Level == level && strings.Contains(a.Message, fragment) {
			return true
		}
	}
	return false
}

func TestBuildInventory_EffectiveTools(t *testing.T) {
	inv := BuildInventory(testInputs())
	cases := map[string][]string{
		"boss":  {"agent_coder", "agent_mute", "agent_root", "read_file"},
		"coder": {"exec", "read_file"},
		"root":  {"agent_boss", "agent_coder", "agent_mute", "exec", "orphan", "read_file"},
		"mute":  nil,
	}
	for id, want := range cases {
		if got := toolNames(inv.Agent(id)); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: tools = %v, want %v", id, got, want)
		}
	}
	if via := inv.Agent("root").Tools[0].Via; via != "*" {
		t.Errorf("root exec via = %q, want *", via)
	}
	if got := inv.Tool("exec").Agents; !reflect.DeepEqual(got, []string{"coder", "root"}) {
		t.Errorf("exec agents = %v", got)
	}
}

func TestBuildInventory_Availability(t *testing.T) {
	in := testInputs()
	in.Queues["unknown"] = taskqueue.Status{Queue: "unknown", Err: errors.New("timeout")}
	in.Tools = append(in.Tools, store.ToolRecord{Name: "z", Kind: "activity", TaskQueue: "unknown"})
	// A workflow tool needs workflow pollers: activity pollers on its queue
	// don't serve it.
	in.Tools = append(in.Tools, store.ToolRecord{Name: "zz", Kind: "workflow", TaskQueue: "tools"})
	inv := BuildInventory(in)

	for name, want := range map[string]string{
		"read_file":   "up",
		"agent_coder": "up", // runs on the workflow queue
		"orphan":      "down",
		"z":           "unknown",
		"zz":          "down",
	} {
		if got := inv.Tool(name).Availability; got != want {
			t.Errorf("%s: availability = %s, want %s", name, got, want)
		}
	}
	if !hasAlert(inv, LevelDanger, "Le tool orphan est publié sur la queue gone") {
		t.Error("missing alert for a tool on an unserved queue")
	}
	if hasAlert(inv, LevelDanger, "Le tool z ") {
		t.Error("an unknown status must not be reported as down")
	}
}

func TestBuildInventory_Delegation(t *testing.T) {
	inv := BuildInventory(testInputs())
	boss := inv.Agent("boss")

	extras := map[string][]string{}
	var targets []string
	for _, d := range boss.DelegatesTo {
		targets = append(targets, d.AgentID)
		for _, tv := range d.Extra {
			extras[d.AgentID] = append(extras[d.AgentID], tv.Name)
		}
	}
	if !reflect.DeepEqual(targets, []string{"coder", "mute", "root"}) {
		t.Errorf("boss delegates to %v", targets)
	}
	// What each call can end up using that boss cannot, transitively: root
	// may itself call coder, and calling back boss adds nothing.
	want := map[string][]string{"coder": {"exec"}, "root": {"exec", "orphan"}}
	if !reflect.DeepEqual(extras, want) {
		t.Errorf("extras = %v, want %v", extras, want)
	}
	if !hasAlert(inv, LevelWarning, "L'agent boss peut déléguer à coder et atteindre ainsi exec") {
		t.Error("missing escalation alert")
	}
	if got := inv.Agent("coder").CalledBy; !reflect.DeepEqual(got, []string{"boss", "root"}) {
		t.Errorf("coder called by %v", got)
	}
	if inv.Agent("coder").DelegatesTo != nil {
		t.Error("an agent granted no agent tool delegates to nobody")
	}
	if got := inv.Tool("agent_coder").Agents; !reflect.DeepEqual(got, []string{"boss", "root"}) {
		t.Errorf("agent_coder callable by %v", got)
	}
	// "agent_*" matches every other agent's tool, not boss's own.
	for _, g := range boss.Globs {
		if g.Pattern == "agent_*" && !reflect.DeepEqual(g.Matches, []string{"agent_coder", "agent_root", "agent_mute"}) {
			t.Errorf("agent_* matches %v", g.Matches)
		}
	}
}

func TestBuildInventory_Alerts(t *testing.T) {
	in := testInputs()
	in.Agents = append(in.Agents, store.Agent{ID: "typo", Name: "Typo", Tools: []string{"read_[file"}})
	inv := BuildInventory(in)

	for _, c := range []struct{ level, fragment string }{
		{LevelDanger, "L'agent root a accès à tous les tools"},
		{LevelWarning, "L'agent mute n'a aucun tool"},
		{LevelWarning, `Le motif "nope_*" de l'agent coder`},
		{LevelDanger, `L'agent typo a un motif d'allowlist invalide`},
		{LevelWarning, "Le skill ghost"},
		// orphan is reachable through root's "*": nothing is unused here.
	} {
		if !hasAlert(inv, c.level, c.fragment) {
			t.Errorf("missing %s alert %q", c.level, c.fragment)
		}
	}
	if inv.Alerts[0].Level != LevelDanger {
		t.Error("danger alerts must come first")
	}

	if hasAlert(inv, LevelWarning, "accessible à aucun agent") {
		t.Error("every tool is reachable through root")
	}
	in.Agents = in.Agents[:2] // drop root: exec stays reachable via coder only
	if !hasAlert(BuildInventory(in), LevelWarning, "Le tool orphan n'est accessible à aucun agent") {
		t.Error("missing alert for an unused tool")
	}

	in.DefaultAgentID = "nobody"
	if !hasAlert(BuildInventory(in), LevelDanger, `"nobody"`) {
		t.Error("missing alert for an absent default agent")
	}
}

func TestBuildInventory_Skills(t *testing.T) {
	inv := BuildInventory(testInputs())
	if got := inv.Skill("present").Agents; !reflect.DeepEqual(got, []string{"boss"}) {
		t.Errorf("present used by %v", got)
	}
	if len(inv.MissingSkills) != 1 || inv.MissingSkills[0].Name != "ghost" {
		t.Errorf("missing skills = %+v", inv.MissingSkills)
	}
	refs := inv.Agent("boss").Skills
	if !refs[0].Present || refs[1].Present {
		t.Errorf("boss skill refs = %+v", refs)
	}
}

func TestBuildInventory_Queues(t *testing.T) {
	in := testInputs()
	in.ActivityQueues = []store.ActivityQueueEntry{{ActivityName: "CallLLM", TaskQueue: "llm"}}
	inv := BuildInventory(in)

	var names []string
	for _, q := range inv.Queues {
		names = append(names, q.Name)
	}
	// Workflow queue first, then by name; activity_queues targets included.
	if !reflect.DeepEqual(names, []string{"agent", "gone", "llm", "tools"}) {
		t.Errorf("queues = %v", names)
	}
	for _, q := range inv.Queues {
		if q.Name == "llm" && (q.Known || !reflect.DeepEqual(q.ActivityRoutes, []string{"CallLLM"})) {
			t.Errorf("llm queue = %+v", q)
		}
	}
}
