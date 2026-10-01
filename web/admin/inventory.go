package admin

import (
	"fmt"
	"path"
	"slices"
	"sort"
	"strings"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/skill"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/taskqueue"
)

// sensitiveTools can change the outside world: run commands, write files, push
// code, send mail. Display only — the allowlist is what grants or denies them.
var sensitiveTools = map[string]bool{
	"exec":              true,
	"write_file":        true,
	"edit_file":         true,
	"implement_feature": true,
	"send_email":        true,
}

// Inputs is everything the dashboard reads, gathered by the handlers.
type Inputs struct {
	Agents         []store.Agent
	Tools          []store.ToolRecord
	Queues         map[string]taskqueue.Status // missing queue = status unknown
	Skills         []skill.Skill
	SkillsSource   string // where the server loaded skills from; "" = nowhere
	Sessions       map[string]int
	ActivityQueues []store.ActivityQueueEntry
	DefaultAgentID string
	WorkflowQueue  string
}

// Inventory is the dashboard's view of the configuration, with the
// inconsistencies found in it.
type Inventory struct {
	Agents        []*AgentView
	Tools         []*ToolView
	Queues        []*QueueView
	Skills        []*SkillView
	MissingSkills []*MissingSkill
	SkillsSource  string
	Alerts        []Alert

	agents map[string]*AgentView
	tools  map[string]*ToolView
	skills map[string]*SkillView
}

type AgentView struct {
	store.Agent
	IsDefault bool
	Sessions  int
	AllTools  bool // the allowlist holds "*"
	Globs     []GlobView
	// Tools is what the agent is offered, as activity.Catalog computes it for
	// the workers.
	Tools          []AgentTool
	SensitiveCount int
	DelegatesTo    []Delegation
	CalledBy       []string
	Skills         []SkillRef
}

// GlobView is one allowlist pattern and the published tools it matches.
type GlobView struct {
	Pattern string
	Invalid bool
	Matches []string
}

type AgentTool struct {
	*ToolView
	Via string // first glob of the allowlist that matches the tool
}

// Delegation is a spawn_session target, with the tools the target can use
// that the delegating agent cannot: what delegation adds to its reach.
type Delegation struct {
	AgentID        string
	Extra          []*ToolView
	ExtraSensitive []string
}

type SkillRef struct {
	Name    string
	Present bool
}

type ToolView struct {
	store.ToolRecord
	Sensitive bool
	Agents    []string
	// Availability: "up", "down" or "unknown", from the pollers of the type the
	// tool needs on its queue.
	Availability string
}

type QueueView struct {
	Name            string
	IsWorkflowQueue bool
	Status          taskqueue.Status
	Known           bool // Temporal answered
	Tools           []string
	ActivityRoutes  []string // activities routed here by activity_queues
}

type SkillView struct {
	skill.Skill
	Agents []string
}

// MissingSkill is a skill an agent references that the server did not load.
type MissingSkill struct {
	Name   string
	Agents []string
}

// Alert levels.
const (
	LevelDanger  = "danger"
	LevelWarning = "warning"
)

type Alert struct {
	Level   string
	Message string
	Link    string
}

// BuildInventory computes the dashboard views from the raw configuration.
// It is pure: no I/O, so every rule here is testable on its own.
func BuildInventory(in Inputs) *Inventory {
	inv := &Inventory{
		SkillsSource: in.SkillsSource,
		agents:       make(map[string]*AgentView),
		tools:        make(map[string]*ToolView),
		skills:       make(map[string]*SkillView),
	}

	for _, t := range in.Tools {
		tv := &ToolView{ToolRecord: t, Sensitive: sensitiveTools[t.Name]}
		tv.Availability = availability(t, in.Queues)
		inv.Tools = append(inv.Tools, tv)
		inv.tools[t.Name] = tv
	}
	for _, s := range in.Skills {
		sv := &SkillView{Skill: s}
		inv.Skills = append(inv.Skills, sv)
		inv.skills[s.Name] = sv
	}

	// The effective tools are computed by activity.Catalog itself, never
	// re-derived here.
	catalog, entries := newCatalog(in.Agents, in.Tools)

	missing := make(map[string]*MissingSkill)
	for _, a := range in.Agents {
		av := &AgentView{
			Agent:     a,
			IsDefault: a.ID == in.DefaultAgentID,
			Sessions:  in.Sessions[a.ID],
			AllTools:  slices.Contains(a.Tools, "*"),
		}
		for _, g := range a.Tools {
			av.Globs = append(av.Globs, globView(g, in.Tools))
		}
		for _, def := range catalog.AllowedTools(a.ID).Tools {
			tv := inv.tools[def.Name]
			if tv == nil {
				continue
			}
			tv.Agents = append(tv.Agents, a.ID)
			av.Tools = append(av.Tools, AgentTool{ToolView: tv, Via: firstMatch(a.Tools, def.Name)})
			if tv.Sensitive {
				av.SensitiveCount++
			}
		}
		for _, name := range a.Skills {
			sv, ok := inv.skills[name]
			av.Skills = append(av.Skills, SkillRef{Name: name, Present: ok})
			if ok {
				sv.Agents = append(sv.Agents, a.ID)
				continue
			}
			if missing[name] == nil {
				missing[name] = &MissingSkill{Name: name}
				inv.MissingSkills = append(inv.MissingSkills, missing[name])
			}
			missing[name].Agents = append(missing[name].Agents, a.ID)
		}
		inv.Agents = append(inv.Agents, av)
		inv.agents[a.ID] = av
	}

	for _, av := range inv.Agents {
		if !av.hasTool(activity.SpawnToolName) {
			continue
		}
		for _, id := range activity.DelegatableAgentIDs(entries, av.ID) {
			target := inv.agents[id]
			d := Delegation{AgentID: id}
			for _, t := range target.Tools {
				if !av.hasTool(t.Name) {
					d.Extra = append(d.Extra, t.ToolView)
					if t.Sensitive {
						d.ExtraSensitive = append(d.ExtraSensitive, t.Name)
					}
				}
			}
			av.DelegatesTo = append(av.DelegatesTo, d)
			target.CalledBy = append(target.CalledBy, av.ID)
		}
	}

	inv.Queues = buildQueues(in)
	inv.Alerts = buildAlerts(inv, in)
	return inv
}

// newCatalog builds, from the same rows, the catalog the workers build.
func newCatalog(agents []store.Agent, tools []store.ToolRecord) (*activity.Catalog, []activity.AgentCatalogEntry) {
	entries := make([]activity.AgentCatalogEntry, len(agents))
	for i, a := range agents {
		entries[i] = activity.AgentCatalogEntry{ID: a.ID, Name: a.Name, Description: a.Description, Skills: a.Skills, Tools: a.Tools}
	}
	catalog := activity.NewCatalog()
	catalog.SetAgents(entries)
	catalog.SetTools(tools)
	return catalog, entries
}

func (av *AgentView) hasTool(name string) bool {
	for _, t := range av.Tools {
		if t.Name == name {
			return true
		}
	}
	return false
}

func globView(pattern string, tools []store.ToolRecord) GlobView {
	gv := GlobView{Pattern: pattern}
	if _, err := path.Match(pattern, ""); err != nil {
		gv.Invalid = true
		return gv
	}
	for _, t := range tools {
		if ok, _ := path.Match(pattern, t.Name); ok {
			gv.Matches = append(gv.Matches, t.Name)
		}
	}
	return gv
}

func firstMatch(globs []string, name string) string {
	for _, g := range globs {
		if ok, _ := path.Match(g, name); ok {
			return g
		}
	}
	return ""
}

// availability tells whether a worker takes the tool's tasks: a workflow tool
// needs workflow pollers on its queue, any other kind activity pollers.
func availability(t store.ToolRecord, queues map[string]taskqueue.Status) string {
	st, ok := queues[t.TaskQueue]
	if !ok || st.Err != nil {
		return "unknown"
	}
	served := st.ActivityServed()
	if t.Kind == "workflow" {
		served = st.WorkflowServed()
	}
	if served {
		return "up"
	}
	return "down"
}

// buildQueues lists every queue the configuration names: the workflow queue,
// the queues tools are published on, and the activity_queues targets.
func buildQueues(in Inputs) []*QueueView {
	byName := make(map[string]*QueueView)
	get := func(name string) *QueueView {
		if q, ok := byName[name]; ok {
			return q
		}
		q := &QueueView{Name: name, IsWorkflowQueue: name == in.WorkflowQueue}
		if st, ok := in.Queues[name]; ok {
			q.Status = st
			q.Known = st.Err == nil
		}
		byName[name] = q
		return q
	}

	get(in.WorkflowQueue)
	for _, t := range in.Tools {
		q := get(t.TaskQueue)
		q.Tools = append(q.Tools, t.Name)
	}
	for _, e := range in.ActivityQueues {
		q := get(e.TaskQueue)
		q.ActivityRoutes = append(q.ActivityRoutes, e.ActivityName)
	}

	queues := make([]*QueueView, 0, len(byName))
	for _, q := range byName {
		queues = append(queues, q)
	}
	// Workflow queue first, then by name.
	sort.Slice(queues, func(i, j int) bool {
		if queues[i].IsWorkflowQueue != queues[j].IsWorkflowQueue {
			return queues[i].IsWorkflowQueue
		}
		return queues[i].Name < queues[j].Name
	})
	return queues
}

func buildAlerts(inv *Inventory, in Inputs) []Alert {
	var alerts []Alert
	add := func(level, link, format string, args ...any) {
		alerts = append(alerts, Alert{Level: level, Link: link, Message: fmt.Sprintf(format, args...)})
	}

	if in.DefaultAgentID != "" && inv.agents[in.DefaultAgentID] == nil {
		add(LevelDanger, "/admin/agents", "L'agent par défaut %q (DEFAULT_AGENT_ID) n'existe pas : une session sans agent_id prend le premier agent venu.", in.DefaultAgentID)
	}

	for _, av := range inv.Agents {
		link := "/admin/agents/" + av.ID
		if av.AllTools {
			add(LevelDanger, link, "L'agent %s a accès à tous les tools (\"*\"), dont %d sensible%s.", av.ID, av.SensitiveCount, plural(av.SensitiveCount))
		}
		if len(av.Agent.Tools) == 0 {
			add(LevelWarning, link, "L'agent %s n'a aucun tool : son allowlist est vide.", av.ID)
		}
		for _, g := range av.Globs {
			switch {
			case g.Invalid:
				add(LevelDanger, link, "L'agent %s a un motif d'allowlist invalide : %q.", av.ID, g.Pattern)
			case len(g.Matches) == 0:
				add(LevelWarning, link, "Le motif %q de l'agent %s ne correspond à aucun tool publié.", g.Pattern, av.ID)
			}
		}
		for _, d := range av.DelegatesTo {
			if len(d.ExtraSensitive) > 0 {
				add(LevelWarning, link, "L'agent %s peut déléguer à %s et atteindre ainsi %s, qu'il n'a pas lui-même.", av.ID, d.AgentID, strings.Join(d.ExtraSensitive, ", "))
			}
		}
	}

	// One alert for all unused tools: listed one by one, they drown the rest
	// whenever an allowlist is still empty.
	var unused []string
	for _, tv := range inv.Tools {
		if len(tv.Agents) == 0 {
			unused = append(unused, tv.Name)
		}
	}
	switch len(unused) {
	case 0:
	case 1:
		add(LevelWarning, "/admin/tools/"+unused[0], "Le tool %s n'est accessible à aucun agent.", unused[0])
	default:
		add(LevelWarning, "/admin/tools", "%d tools ne sont accessibles à aucun agent : %s.", len(unused), strings.Join(unused, ", "))
	}

	for _, tv := range inv.Tools {
		link := "/admin/tools/" + tv.Name
		if tv.Availability == "down" {
			add(LevelDanger, link, "Le tool %s est publié sur la queue %s, que plus aucun worker ne sert : ses appels échoueront.", tv.Name, tv.TaskQueue)
		}
	}

	for _, ms := range inv.MissingSkills {
		add(LevelWarning, "/admin/skills", "Le skill %s, référencé par %s, n'est pas chargé par le serveur.", ms.Name, strings.Join(ms.Agents, ", "))
	}

	// Most severe first, stable within a level.
	sort.SliceStable(alerts, func(i, j int) bool {
		return alerts[i].Level == LevelDanger && alerts[j].Level != LevelDanger
	})
	return alerts
}

func plural(n int) string {
	if n > 1 {
		return "s"
	}
	return ""
}

func (inv *Inventory) Agent(id string) *AgentView   { return inv.agents[id] }
func (inv *Inventory) Tool(name string) *ToolView   { return inv.tools[name] }
func (inv *Inventory) Skill(name string) *SkillView { return inv.skills[name] }

// ToolsByQueue groups the tools by the queue they are published on, in queue
// order.
func (inv *Inventory) ToolsByQueue() []QueueTools {
	var groups []QueueTools
	for _, q := range inv.Queues {
		if len(q.Tools) == 0 {
			continue
		}
		g := QueueTools{Queue: q}
		for _, name := range q.Tools {
			g.Tools = append(g.Tools, inv.tools[name])
		}
		groups = append(groups, g)
	}
	return groups
}

type QueueTools struct {
	Queue *QueueView
	Tools []*ToolView
}

// DangerCount is the number of danger-level alerts.
func (inv *Inventory) DangerCount() int {
	n := 0
	for _, a := range inv.Alerts {
		if a.Level == LevelDanger {
			n++
		}
	}
	return n
}
