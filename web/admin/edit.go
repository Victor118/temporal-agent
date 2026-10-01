package admin

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/store"
)

// catalogRefresh is how often the workers reload agents from the DB: an edit
// reaches the turns that start after at most this long.
const catalogRefresh = 30 * time.Second

// previewID stands in for an agent whose ID is not typed yet.
const previewID = "__preview__"

// agentForm is the create/edit form, holding what the user typed so a refused
// submission comes back intact.
type agentForm struct {
	New         bool
	ID          string
	Name        string
	Description string
	Skills      string // one per line
	Tools       string // one glob per line
	Revision    int64
	Error       string
	IsDefault   bool
	// Known skills, offered as suggestions.
	AvailableSkills []string
	// Preview is the agent as the allowlist being typed would make it.
	Preview *AgentView
}

func formFromAgent(a store.Agent) agentForm {
	return agentForm{
		ID:          a.ID,
		Name:        a.Name,
		Description: a.Description,
		Skills:      strings.Join(a.Skills, "\n"),
		Tools:       strings.Join(a.Tools, "\n"),
		Revision:    a.Revision,
	}
}

func formFromRequest(r *http.Request) agentForm {
	rev, _ := strconv.ParseInt(r.FormValue("revision"), 10, 64)
	return agentForm{
		ID:          strings.TrimSpace(r.FormValue("id")),
		Name:        strings.TrimSpace(r.FormValue("name")),
		Description: strings.TrimSpace(r.FormValue("description")),
		Skills:      r.FormValue("skills"),
		Tools:       r.FormValue("tools"),
		Revision:    rev,
	}
}

// agent turns the form into an agent, validated like a seed entry.
func (f agentForm) agent() (store.Agent, error) {
	def := config.AgentDefinition{
		ID:          f.ID,
		Name:        f.Name,
		Description: f.Description,
		Skills:      lines(f.Skills),
		Tools:       lines(f.Tools),
	}
	if err := def.Validate(); err != nil {
		return store.Agent{}, err
	}
	return store.Agent{ID: def.ID, Name: def.Name, Description: def.Description, Skills: def.Skills, Tools: def.Tools}, nil
}

// lines splits a textarea into its non-empty, trimmed, distinct lines. It never
// returns nil: an empty allowlist is stored as [], which grants nothing.
func lines(s string) []string {
	out := []string{}
	for _, l := range strings.Split(s, "\n") {
		l = strings.TrimSpace(l)
		if l != "" && !slices.Contains(out, l) {
			out = append(out, l)
		}
	}
	return out
}

// renderForm shows the form, with the preview of what it would grant.
func (a *Admin) renderForm(w http.ResponseWriter, r *http.Request, f agentForm) {
	snap, err := a.load(r.Context())
	if err != nil {
		log.Printf("admin: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for _, s := range snap.in.Skills {
		f.AvailableSkills = append(f.AvailableSkills, s.Name)
	}
	f.IsDefault = !f.New && f.ID == a.cfg.DefaultAgentID
	f.Preview = preview(snap.in, f)
	a.pages.page(w, "agent_edit", pageData{Nav: "agents", Inv: snap.inv, Data: f})
}

// preview builds the inventory as if the form were saved, so what it shows
// comes from the same computation as the dashboard itself.
func preview(in Inputs, f agentForm) *AgentView {
	id := f.ID
	if id == "" {
		id = previewID
	}
	candidate := store.Agent{ID: id, Name: f.Name, Skills: lines(f.Skills), Tools: lines(f.Tools)}

	agents := make([]store.Agent, 0, len(in.Agents)+1)
	replaced := false
	for _, ag := range in.Agents {
		if ag.ID == id {
			agents = append(agents, candidate)
			replaced = true
			continue
		}
		agents = append(agents, ag)
	}
	if !replaced {
		agents = append(agents, candidate)
	}
	in.Agents = agents
	return BuildInventory(in).Agent(id)
}

func (a *Admin) newAgentForm(w http.ResponseWriter, r *http.Request) {
	a.renderForm(w, r, agentForm{New: true})
}

func (a *Admin) editAgentForm(w http.ResponseWriter, r *http.Request) {
	ag, err := a.cfg.Store.GetAgent(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if ag == nil {
		http.NotFound(w, r)
		return
	}
	a.renderForm(w, r, formFromAgent(*ag))
}

func (a *Admin) createAgent(w http.ResponseWriter, r *http.Request) {
	f := formFromRequest(r)
	f.New = true
	ag, err := f.agent()
	if err == nil {
		err = a.cfg.Store.CreateAgent(r.Context(), ag)
	}
	switch {
	case errors.Is(err, store.ErrAgentExists):
		f.Error = fmt.Sprintf("L'identifiant %q est déjà pris.", f.ID)
	case err != nil:
		f.Error = err.Error()
	default:
		log.Printf("admin: agent %q created, tools %v", ag.ID, ag.Tools)
		navigate(w, r, savedURL(ag.ID))
		return
	}
	a.renderForm(w, r, f)
}

func (a *Admin) updateAgent(w http.ResponseWriter, r *http.Request) {
	f := formFromRequest(r)
	f.ID = chi.URLParam(r, "id") // the ID is not editable
	ag, err := f.agent()
	if err == nil {
		_, err = a.cfg.Store.UpdateAgent(r.Context(), ag, f.Revision)
	}
	switch {
	case errors.Is(err, store.ErrAgentNotFound):
		http.NotFound(w, r)
		return
	case errors.Is(err, store.ErrAgentConflict):
		// The revision is kept stale on purpose: resubmitting as is would
		// overwrite the other change, so it keeps being refused.
		f.Error = "Cet agent a été modifié ailleurs depuis l'ouverture du formulaire. Recharge la page pour repartir de la version actuelle, puis refais tes changements."
	case err != nil:
		f.Error = err.Error()
	default:
		log.Printf("admin: agent %q updated, tools %v", ag.ID, ag.Tools)
		navigate(w, r, savedURL(ag.ID))
		return
	}
	a.renderForm(w, r, f)
}

func (a *Admin) deleteAgent(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == a.cfg.DefaultAgentID {
		http.Error(w, "L'agent par défaut (DEFAULT_AGENT_ID) ne peut pas être supprimé.", http.StatusConflict)
		return
	}
	switch err := a.cfg.Store.DeleteAgent(r.Context(), id); {
	case errors.Is(err, store.ErrAgentNotFound):
		http.NotFound(w, r)
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	default:
		log.Printf("admin: agent %q deleted", id)
		navigate(w, r, "/admin/agents?deleted="+url.QueryEscape(id))
	}
}

// allowlistPreview re-renders the preview while the allowlist is typed.
func (a *Admin) allowlistPreview(w http.ResponseWriter, r *http.Request) {
	snap, err := a.load(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	a.pages.execute(w, "agent_edit", "allowlist_preview", preview(snap.in, formFromRequest(r)))
}

func (a *Admin) reloadSkills(w http.ResponseWriter, r *http.Request) {
	if !a.cfg.SkillsReloadable {
		http.Error(w, "Pas de dépôt de skills configuré (SKILLS_REPO).", http.StatusConflict)
		return
	}
	v, err := a.cfg.Store.IncrementSkillsVersion(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("admin: skills version bumped to %d", v)
	navigate(w, r, "/admin/skills?reloaded="+strconv.FormatInt(v, 10))
}

func savedURL(id string) string {
	return "/admin/agents/" + url.PathEscape(id) + "?saved=" + strconv.FormatInt(time.Now().Unix(), 10)
}

// flash describes the action that led to this page, from its query string.
func flash(r *http.Request) string {
	q := r.URL.Query()
	if s := q.Get("saved"); s != "" {
		if at, err := strconv.ParseInt(s, 10, 64); err == nil {
			t := time.Unix(at, 0)
			return fmt.Sprintf("Enregistré à %s. Les workers relisent les agents toutes les %d s : appliqué aux tours qui démarrent après %s au plus tard.",
				t.Format("15:04:05"), int(catalogRefresh.Seconds()), t.Add(catalogRefresh).Format("15:04:05"))
		}
	}
	if id := q.Get("deleted"); id != "" {
		return fmt.Sprintf("Agent %s supprimé. Ses sessions encore ouvertes n'ont plus accès à aucun tool.", id)
	}
	if v := q.Get("reloaded"); v != "" {
		return fmt.Sprintf("Version des skills passée à %s : le serveur et les workers rechargent le dépôt d'ici %d s.", v, int(catalogRefresh.Seconds()))
	}
	return ""
}
