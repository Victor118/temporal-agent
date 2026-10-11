// Package admin serves the back-office: a read-only view of the agents, the
// tools they can reach, the queues serving those tools, and the skills.
package admin

import (
	"cmp"
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/victor/temporal-agent/access"
	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/auth"
	"github.com/victor/temporal-agent/skill"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/taskqueue"
)

//go:embed static
var staticFS embed.FS

// Store is what the back-office reads and edits: the configuration (agents,
// the tools workers publish, activity routes, skills version) and the users.
type Store interface {
	ListAgents(ctx context.Context) ([]store.Agent, error)
	GetAgent(ctx context.Context, agentID string) (*store.Agent, error)
	CreateAgent(ctx context.Context, agent store.Agent) error
	UpdateAgent(ctx context.Context, agent store.Agent, expectedRevision int64) (int64, error)
	DeleteAgent(ctx context.Context, agentID string) error
	CountSessionsByAgent(ctx context.Context) (map[string]int, error)
	ListTools(ctx context.Context) ([]store.ToolRecord, error)
	ListActivityQueues(ctx context.Context) ([]store.ActivityQueueEntry, error)
	IncrementSkillsVersion(ctx context.Context) (int64, error)

	ListUsers(ctx context.Context) ([]store.User, error)
	GetUser(ctx context.Context, id string) (*store.User, error)
	GetUserByEmail(ctx context.Context, email string) (*store.User, error)
	CreateUser(ctx context.Context, u store.User) error
	UpdateUser(ctx context.Context, u store.User) error
	SetUserPassword(ctx context.Context, id, passwordHash string) error
	SetUserDisabled(ctx context.Context, id string, disabled bool) error

	// Who may use which tool (access.go).
	ListGroups(ctx context.Context) ([]store.Group, error)
	CreateGroup(ctx context.Context, g store.Group) error
	RenameGroup(ctx context.Context, id, name string) error
	DeleteGroup(ctx context.Context, id string) error
	AddGroupMember(ctx context.Context, groupID, userID string) error
	RemoveGroupMember(ctx context.Context, groupID, userID string) error
	ListAccessRules(ctx context.Context) ([]store.AccessRule, error)
	GetAccessRule(ctx context.Context, id int64) (*store.AccessRule, error)
	CreateAccessRule(ctx context.Context, r access.Rule) (int64, error)
	UpdateAccessRule(ctx context.Context, r access.Rule) error
	DeleteAccessRule(ctx context.Context, id int64) error
	ListMCPServers(ctx context.Context) ([]string, error)
}

// Config wires the back-office to the server's state.
type Config struct {
	Store        Store
	Temporal     taskqueue.Describer  // asked who polls each queue
	Skills       func() []skill.Skill // the skills the server currently holds
	SkillsSource string               // where they come from, "" = not loaded
	// SkillsReloadable: the skills come from a repo that the server and the
	// workers reload when skills_version moves.
	SkillsReloadable bool
	DefaultAgentID   string
	WorkflowQueue    string
	Auth             *auth.Service // logs users in; the back-office admits admins only
}

type Admin struct {
	cfg    Config
	prober *queueProber
	pages  *renderer
}

func New(cfg Config) *Admin {
	return &Admin{cfg: cfg, prober: newQueueProber(cfg.Temporal), pages: newRenderer()}
}

// Routes returns the back-office router, to be mounted under /admin.
func (a *Admin) Routes() http.Handler {
	r := chi.NewRouter()
	r.Use(auth.SameOrigin)

	static, _ := fs.Sub(staticFS, "static")
	r.Handle("/static/*", http.StripPrefix("/admin/static/", shortCache(http.FileServer(http.FS(static)))))
	r.Get("/login", a.loginPage)
	r.Post("/login", a.login)

	r.Group(func(r chi.Router) {
		r.Use(a.requireLogin)
		r.Post("/logout", a.logout)

		r.Get("/", a.overview)
		r.Get("/agents", a.agents)
		r.Get("/agents/new", a.newAgentForm)
		r.Post("/agents", a.createAgent)
		r.Get("/agents/{id}", a.agent)
		r.Get("/agents/{id}/edit", a.editAgentForm)
		r.Post("/agents/{id}", a.updateAgent)
		r.Post("/agents/{id}/delete", a.deleteAgent)
		r.Get("/agents/{id}/prompt", a.agentPrompt)
		r.Get("/allowlist/preview", a.allowlistPreview)
		r.Get("/tools", a.tools)
		r.Get("/tools/{name}", a.tool)
		r.Get("/queues", a.queues)
		r.Get("/queues/status", a.queuesStatus)
		r.Get("/skills", a.skills)
		r.Post("/skills/reload", a.reloadSkills)
		r.Get("/skills/{name}", a.skill)
		r.Get("/users", a.users)
		r.Get("/users/new", a.newUserForm)
		r.Post("/users", a.createUser)
		r.Get("/users/{id}/edit", a.editUserForm)
		r.Post("/users/{id}", a.updateUser)
		r.Post("/users/{id}/password", a.resetPassword)
		r.Post("/users/{id}/disable", a.disableUser)
		r.Post("/users/{id}/enable", a.enableUser)
		r.Get("/access", a.accessPage)
		r.Get("/access/preview", a.rulePreview)
		r.Get("/access/rules/new", a.newRuleForm)
		r.Post("/access/rules", a.createRule)
		r.Get("/access/rules/{id}", a.editRuleForm)
		r.Post("/access/rules/{id}", a.updateRule)
		r.Post("/access/rules/{id}/delete", a.deleteRule)
		r.Post("/access/groups", a.createGroup)
		r.Get("/access/groups/{id}", a.groupPage)
		r.Post("/access/groups/{id}", a.renameGroup)
		r.Post("/access/groups/{id}/members", a.addGroupMember)
		r.Post("/access/groups/{id}/members/{userID}/remove", a.removeGroupMember)
		r.Post("/access/groups/{id}/delete", a.deleteGroup)
	})
	return r
}

// shortCache bounds how long a browser keeps a stale stylesheet after a
// deploy: embedded files carry no modification time to revalidate against.
func shortCache(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "private, max-age=300")
		next.ServeHTTP(w, r)
	})
}

// snapshot is one consistent read of the configuration: the raw inputs, and
// the inventory built from them.
type snapshot struct {
	in  Inputs
	inv *Inventory
}

func (a *Admin) load(ctx context.Context) (*snapshot, error) {
	agents, err := a.cfg.Store.ListAgents(ctx)
	if err != nil {
		return nil, fmt.Errorf("list agents: %w", err)
	}
	tools, err := a.cfg.Store.ListTools(ctx)
	if err != nil {
		return nil, fmt.Errorf("list tools: %w", err)
	}
	sessions, err := a.cfg.Store.CountSessionsByAgent(ctx)
	if err != nil {
		return nil, fmt.Errorf("count sessions: %w", err)
	}
	routes, err := a.cfg.Store.ListActivityQueues(ctx)
	if err != nil {
		return nil, fmt.Errorf("list activity queues: %w", err)
	}
	rules, err := a.cfg.Store.ListAccessRules(ctx)
	if err != nil {
		return nil, fmt.Errorf("list access rules: %w", err)
	}
	groups, err := a.cfg.Store.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("list groups: %w", err)
	}
	users, err := a.cfg.Store.ListUsers(ctx)
	if err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}
	servers, err := a.cfg.Store.ListMCPServers(ctx)
	if err != nil {
		return nil, fmt.Errorf("list MCP servers: %w", err)
	}
	var skills []skill.Skill
	if a.cfg.Skills != nil {
		skills = a.cfg.Skills()
	}

	names := []string{a.cfg.WorkflowQueue}
	for _, t := range tools {
		names = append(names, t.TaskQueue)
	}
	for _, e := range routes {
		names = append(names, e.TaskQueue)
	}

	in := Inputs{
		Agents:         agents,
		Tools:          tools,
		Queues:         a.prober.statuses(ctx, names),
		Skills:         skills,
		SkillsSource:   a.cfg.SkillsSource,
		Sessions:       sessions,
		ActivityQueues: routes,
		DefaultAgentID: a.cfg.DefaultAgentID,
		WorkflowQueue:  a.cfg.WorkflowQueue,
		Rules:          rules,
		Groups:         groups,
		Users:          users,
		MCPServers:     servers,
	}
	return &snapshot{in: in, inv: BuildInventory(in)}, nil
}

// page loads the snapshot and renders a full page, or reports the failure.
func (a *Admin) page(w http.ResponseWriter, r *http.Request, name, nav string, data func(*Inventory) (any, bool)) {
	snap, err := a.load(r.Context())
	if err != nil {
		log.Printf("admin: %v", err)
		http.Error(w, "Lecture de la configuration impossible : "+err.Error(), http.StatusInternalServerError)
		return
	}
	d, ok := data(snap.inv)
	if !ok {
		http.NotFound(w, r)
		return
	}
	a.pages.page(w, name, pageData{Nav: nav, Inv: snap.inv, Data: d, Flash: cmp.Or(flash(r), accessFlash(r)), Me: auth.UserFrom(r.Context())})
}

func (a *Admin) overview(w http.ResponseWriter, r *http.Request) {
	a.page(w, r, "overview", "overview", func(*Inventory) (any, bool) { return nil, true })
}

func (a *Admin) agents(w http.ResponseWriter, r *http.Request) {
	a.page(w, r, "agents", "agents", func(*Inventory) (any, bool) { return nil, true })
}

func (a *Admin) agent(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	a.page(w, r, "agent", "agents", func(inv *Inventory) (any, bool) {
		av := inv.Agent(id)
		return av, av != nil
	})
}

func (a *Admin) tools(w http.ResponseWriter, r *http.Request) {
	a.page(w, r, "tools", "tools", func(*Inventory) (any, bool) { return nil, true })
}

func (a *Admin) tool(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	a.page(w, r, "tool", "tools", func(inv *Inventory) (any, bool) {
		tv := inv.Tool(name)
		return tv, tv != nil
	})
}

func (a *Admin) queues(w http.ResponseWriter, r *http.Request) {
	a.page(w, r, "queues", "queues", func(*Inventory) (any, bool) { return nil, true })
}

func (a *Admin) skills(w http.ResponseWriter, r *http.Request) {
	a.page(w, r, "skills", "skills", func(*Inventory) (any, bool) { return a.cfg.SkillsReloadable, true })
}

func (a *Admin) skill(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	a.page(w, r, "skill", "skills", func(inv *Inventory) (any, bool) {
		sv := inv.Skill(name)
		return sv, sv != nil
	})
}

// queuesStatus renders the queue cards alone, for the queues page to refresh
// itself.
func (a *Admin) queuesStatus(w http.ResponseWriter, r *http.Request) {
	snap, err := a.load(r.Context())
	if err != nil {
		log.Printf("admin: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	a.pages.fragment(w, "queues", "queue_cards", pageData{Inv: snap.inv})
}

// agentPrompt renders the system prompt the agent would get, built as the
// workers' LLM calls build it, from the server's skills, for its whole list
// of tools (a turn's user may have fewer: docs/design/permissions.md).
func (a *Admin) agentPrompt(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	snap, err := a.load(r.Context())
	if err != nil {
		log.Printf("admin: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if snap.inv.Agent(id) == nil {
		http.NotFound(w, r)
		return
	}

	catalog := newCatalog(snap.in.Agents, snap.in.Tools)
	var tools []string
	for _, def := range catalog.AllowedTools(id).Tools {
		tools = append(tools, def.Name)
	}
	prompt := activity.NewPrompts(snap.in.Skills, catalog).AgentPrompt(id, tools)
	a.pages.fragment(w, "agent", "prompt", pageData{Inv: snap.inv, Data: prompt})
}
