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

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/auth"
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
	Mention     string // empty = the ID
	Description string
	// LLMOnMachine is where its turns' model runs (store.LLMOnMachine*).
	LLMOnMachine string
	Skills       string // one per line
	// The allowlist, split in two: published tools checked by name, and
	// everything else (patterns, names of tools not published right now), one
	// per line. Saving joins them back.
	Picked    []string
	Globs     string
	Revision  int64
	Error     string
	IsDefault bool
	// Known skills, offered as suggestions.
	AvailableSkills []string
	Picker          []PickerGroup
	Preview         previewData
}

// PickerGroup is one queue's tools in the allowlist picker.
type PickerGroup struct {
	Queue string
	Tools []PickerTool
}

type PickerTool struct {
	*ToolView
	Checked bool
	Via     string // the advanced pattern that already grants the tool
}

// previewData is what the allowlist being edited grants.
type previewData struct {
	Agent  *AgentView
	Picker []PickerGroup
	// OOB is set on a live refresh: the response then also updates the "via"
	// badges of the picker, out of band, without re-rendering its checkboxes.
	OOB bool
}

// formFromAgent fills the form from a stored agent. An entry naming a
// published tool becomes a checkbox; anything else stays text, so that saving
// never drops a pattern or a tool whose worker happens to be down.
func formFromAgent(a store.Agent, tools []store.ToolRecord, agents []store.Agent) agentForm {
	published := make(map[string]bool, len(tools))
	for _, t := range tools {
		published[t.Name] = true
	}
	for _, o := range agents {
		if o.ID != a.ID {
			published[activity.AgentToolName(o.ID)] = true
		}
	}
	f := agentForm{
		ID:           a.ID,
		Name:         a.Name,
		Mention:      a.Mention,
		Description:  a.Description,
		LLMOnMachine: a.LLMOn(),
		Skills:       strings.Join(a.Skills, "\n"),
		Revision:     a.Revision,
	}
	var globs []string
	for _, g := range a.Tools {
		if published[g] {
			f.Picked = append(f.Picked, g)
		} else {
			globs = append(globs, g)
		}
	}
	f.Globs = strings.Join(globs, "\n")
	return f
}

func formFromRequest(r *http.Request) agentForm {
	r.ParseForm()
	rev, _ := strconv.ParseInt(r.FormValue("revision"), 10, 64)
	return agentForm{
		ID:           strings.TrimSpace(r.FormValue("id")),
		Name:         strings.TrimSpace(r.FormValue("name")),
		Mention:      strings.TrimPrefix(strings.TrimSpace(r.FormValue("mention")), "@"),
		Description:  strings.TrimSpace(r.FormValue("description")),
		LLMOnMachine: r.FormValue("llm_on_machine"),
		Skills:       r.FormValue("skills"),
		Picked:       r.Form["tool"],
		Globs:        r.FormValue("globs"),
		Revision:     rev,
	}
}

// allowlist is what the form grants: the checked tools, then the patterns.
func (f agentForm) allowlist() []string {
	return lines(strings.Join(f.Picked, "\n") + "\n" + f.Globs)
}

// agent turns the form into an agent, validated like a seed entry.
func (f agentForm) agent() (store.Agent, error) {
	def := config.AgentDefinition{
		ID:           f.ID,
		Name:         f.Name,
		Mention:      f.Mention,
		Description:  f.Description,
		Skills:       lines(f.Skills),
		Tools:        f.allowlist(),
		LLMOnMachine: f.LLMOnMachine,
	}
	if err := def.Validate(); err != nil {
		return store.Agent{}, err
	}
	return store.Agent{ID: def.ID, Name: def.Name, Mention: def.Mention, Description: def.Description, Skills: def.Skills, Tools: def.Tools,
		LLMOnMachine: def.LLMOnMachine}, nil
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
	f.Picker = buildPicker(snap.inv, f)
	f.Preview = previewData{Agent: preview(snap.in, f), Picker: f.Picker}
	a.pages.page(w, "agent_edit", pageData{Nav: "agents", Inv: snap.inv, Data: f, Me: auth.UserFrom(r.Context())})
}

// buildPicker lists the published tools by queue, checked as in the form, and
// marked when one of the form's patterns already grants them.
func buildPicker(inv *Inventory, f agentForm) []PickerGroup {
	globs := lines(f.Globs)
	var groups []PickerGroup
	for _, qt := range inv.ToolsByQueue() {
		g := PickerGroup{Queue: qt.Queue.Name}
		for _, tv := range qt.Tools {
			g.Tools = append(g.Tools, PickerTool{
				ToolView: tv,
				Checked:  slices.Contains(f.Picked, tv.Name),
				Via:      firstMatch(globs, tv.Name),
			})
		}
		groups = append(groups, g)
	}

	// The agents this one may delegate to: every other agent, as a tool.
	agents := PickerGroup{Queue: "Agents (délégation)"}
	for _, tv := range inv.AgentTools {
		if tv.Target == f.ID {
			continue
		}
		agents.Tools = append(agents.Tools, PickerTool{
			ToolView: tv,
			Checked:  slices.Contains(f.Picked, tv.Name),
			Via:      firstMatch(globs, tv.Name),
		})
	}
	if len(agents.Tools) > 0 {
		groups = append(groups, agents)
	}
	return groups
}

// preview builds the inventory as if the form were saved, so what it shows
// comes from the same computation as the dashboard itself.
func preview(in Inputs, f agentForm) *AgentView {
	id := f.ID
	if id == "" {
		id = previewID
	}
	candidate := store.Agent{ID: id, Name: f.Name, Skills: lines(f.Skills), Tools: f.allowlist()}

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
	tools, err := a.cfg.Store.ListTools(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	agents, err := a.cfg.Store.ListAgents(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	a.renderForm(w, r, formFromAgent(*ag, tools, agents))
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
	case errors.Is(err, store.ErrMentionTaken):
		f.Error = mentionTaken(f)
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
	case errors.Is(err, store.ErrMentionTaken):
		f.Error = mentionTaken(f)
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
	f := formFromRequest(r)
	a.pages.execute(w, "agent_edit", "allowlist_preview", previewData{
		Agent:  preview(snap.in, f),
		Picker: buildPicker(snap.inv, f),
		OOB:    true,
	})
}

func (a *Admin) reloadSkills(w http.ResponseWriter, r *http.Request) {
	if !a.cfg.SkillsReloadable {
		http.Error(w, "Pas de source de skills configurée (SKILLS_REPO ou SKILLS_DIR).", http.StatusConflict)
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
	if msg, ok := userFlashes[q.Get("user")]; ok {
		return msg
	}
	if v := q.Get("reloaded"); v != "" {
		return fmt.Sprintf("Version des skills passée à %s : le serveur et les workers rechargent le dépôt d'ici %d s.", v, int(catalogRefresh.Seconds()))
	}
	return ""
}

// mentionTaken explains ErrMentionTaken: what calls an agent is its mention,
// or its ID, and neither may be another agent's mention or ID.
func mentionTaken(f agentForm) string {
	if f.Mention == "" {
		return fmt.Sprintf("L'identifiant %q est déjà la mention d'un autre agent.", f.ID)
	}
	return fmt.Sprintf("La mention @%s ou l'identifiant %q est déjà la mention ou l'identifiant d'un autre agent.", f.Mention, f.ID)
}
