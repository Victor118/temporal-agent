// Package admin serves the back-office: a read-only view of the agents, the
// tools they can reach, the queues serving those tools, and the skills.
package admin

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
	"go.temporal.io/sdk/client"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/skill"
	"github.com/victor/temporal-agent/store"
)

//go:embed static
var staticFS embed.FS

// Config wires the back-office to the server's state.
type Config struct {
	Store        store.Store
	Temporal     client.Client
	Skills       func() []skill.Skill // the skills the server currently holds
	SkillsSource string               // where they come from, "" = not loaded
	// SkillsReloadable: the skills come from a repo that the server and the
	// workers reload when skills_version moves.
	SkillsReloadable bool
	DefaultAgentID   string
	WorkflowQueue    string
	AdminKey         string // the back-office password; "" = closed
}

type Admin struct {
	cfg      Config
	prober   *queueProber
	pages    *renderer
	sessions *sessionStore
}

func New(cfg Config) *Admin {
	return &Admin{cfg: cfg, prober: newQueueProber(cfg.Temporal), pages: newRenderer(), sessions: newSessionStore()}
}

// Routes returns the back-office router, to be mounted under /admin.
func (a *Admin) Routes() http.Handler {
	r := chi.NewRouter()
	r.Use(sameOrigin)

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
	a.pages.page(w, name, pageData{Nav: nav, Inv: snap.inv, Data: d, Flash: flash(r)})
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

// agentPrompt renders the system prompt the agent would get, built by the same
// activity the workers run, from the server's skills.
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
	out, err := activity.NewSkillActivities(snap.in.Skills, catalog).
		LoadSkillsForAgent(r.Context(), activity.LoadSkillsForAgentInput{AgentID: id})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	a.pages.fragment(w, "agent", "prompt", pageData{Inv: snap.inv, Data: out.SystemPrompt})
}
