package admin

import (
	"cmp"
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
	"github.com/google/uuid"

	"github.com/victor/temporal-agent/access"
	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/auth"
	"github.com/victor/temporal-agent/store"
)

// Who may use which tool (docs/design/permissions.md §5): the groups, the
// rules and what they close, decided by access.Allowed as the workers
// decide it.

// RuleView is a rule as the back-office shows it.
type RuleView struct {
	store.AccessRule
	Groups []*GroupView
	Users  []store.User
	// Targets are the published tools and the agents it targets now.
	Targets []*ToolView
	// Removes are the users it leaves without any agent: they had one
	// under the other rules, and have none with it.
	Removes []store.User
}

// Label names what the rule targets.
func (r *RuleView) Label() string {
	if r.Kind == access.KindMCPServer {
		return "serveur " + r.Target
	}
	return r.Target
}

// TargetsNothing: no published tool, no agent.
func (r *RuleView) TargetsNothing() bool { return len(r.Targets) == 0 }

// Lost: it targeted something, and targets nothing any more (a server
// renamed): what it closed would be open again under another name.
func (r *RuleView) Lost() bool { return r.TargetsNothing() && r.LastMatchedAt != nil }

// Holders names who keeps access through it: its groups, then its users;
// "personne" for none (the admins only).
func (r *RuleView) Holders() string {
	var names []string
	for _, g := range r.Groups {
		names = append(names, g.Name)
	}
	for _, u := range r.Users {
		names = append(names, u.Name())
	}
	if len(names) == 0 {
		return "personne"
	}
	return strings.Join(names, ", ")
}

// GroupView is a group with its members and the rules naming it.
type GroupView struct {
	store.Group
	Users []store.User
	Rules []*RuleView
	// SoleOf are the rules it holds alone: deleting it is refused.
	SoleOf []*RuleView
}

// Who says who may use a tool the rules target: "" when none does (open to
// all), else each rule's holders, every rule required.
func (tv *ToolView) Who() string {
	switch len(tv.Rules) {
	case 0:
		return ""
	case 1:
		return tv.Rules[0].Holders()
	}
	parts := make([]string, len(tv.Rules))
	for i, r := range tv.Rules {
		parts[i] = "(" + r.Holders() + ")"
	}
	return strings.Join(parts, " et ")
}

// UserAccess is what a user may use: by agent, whether it answers them, and
// its tools open and closed to them.
type UserAccess struct {
	Agents []UserAgent
}

type UserAgent struct {
	Agent  *AgentView
	Open   bool
	Tools  []*ToolView
	Closed []*ToolView
}

// ref is a tool as the rules see it.
func (tv *ToolView) ref() access.ToolRef {
	return access.ToolRef{Name: tv.Name, MCPServer: tv.MCPServer}
}

// buildAccess fills the inventory's rules and groups, and each tool's rules.
func (inv *Inventory) buildAccess(in Inputs) {
	inv.groups = map[string]*GroupView{}
	users := map[string]store.User{}
	inv.access = access.Snapshot{Users: map[string]access.User{}}
	for _, u := range in.Users {
		users[u.ID] = u
		inv.access.Users[u.ID] = access.User{ID: u.ID, Admin: u.IsAdmin(), Disabled: u.DisabledAt != nil}
	}
	for _, g := range in.Groups {
		gv := &GroupView{Group: g}
		for _, id := range g.Members {
			if u, ok := users[id]; ok {
				gv.Users = append(gv.Users, u)
			}
			au := inv.access.Users[id]
			au.Groups = append(au.Groups, g.ID)
			inv.access.Users[id] = au
		}
		inv.Groups = append(inv.Groups, gv)
		inv.groups[g.ID] = gv
	}

	tools := slices.Concat(inv.Tools, inv.AgentTools)
	var agents []access.ToolRef
	for _, tv := range inv.AgentTools {
		agents = append(agents, tv.ref())
	}
	var allUsers []access.User
	for _, u := range in.Users {
		allUsers = append(allUsers, inv.access.Users[u.ID])
	}
	for _, r := range in.Rules {
		inv.access.Rules = append(inv.access.Rules, r.Rule)
	}
	for i, r := range in.Rules {
		rv := &RuleView{AccessRule: r}
		for _, id := range r.Groups {
			if gv := inv.groups[id]; gv != nil {
				rv.Groups = append(rv.Groups, gv)
				gv.Rules = append(gv.Rules, rv)
				if len(r.Groups) == 1 && len(r.Users) == 0 {
					gv.SoleOf = append(gv.SoleOf, rv)
				}
			}
		}
		for _, id := range r.Users {
			if u, ok := users[id]; ok {
				rv.Users = append(rv.Users, u)
			}
		}
		for _, tv := range tools {
			if r.Matches(tv.ref()) {
				rv.Targets = append(rv.Targets, tv)
				tv.Rules = append(tv.Rules, rv)
			}
		}
		others := slices.Delete(slices.Clone(inv.access.Rules), i, i+1)
		for _, u := range access.Removes(r.Rule, others, allUsers, agents) {
			rv.Removes = append(rv.Removes, users[u.ID])
		}
		inv.Rules = append(inv.Rules, rv)
		inv.rules[r.ID] = rv
	}
	for _, av := range inv.Agents {
		av.Self = inv.tools[activity.AgentToolName(av.ID)]
	}
}

// SensitiveOpen are the sensitive tools no rule targets: open to everyone
// who may call an agent that has them. Said, never changed.
func (inv *Inventory) SensitiveOpen() []*ToolView {
	var open []*ToolView
	for _, tv := range inv.Tools {
		if tv.Sensitive && len(tv.Rules) == 0 {
			open = append(open, tv)
		}
	}
	return open
}

// UserAccess is what userID may use, agent by agent, as the workers decide
// it (access.Allowed).
func (inv *Inventory) UserAccess(userID string) UserAccess {
	var ua UserAccess
	for _, av := range inv.Agents {
		ag := UserAgent{Agent: av, Open: av.Self == nil || inv.access.Allowed(userID, av.Self.ref())}
		for _, t := range av.Tools {
			if inv.access.Allowed(userID, t.ref()) {
				ag.Tools = append(ag.Tools, t.ToolView)
			} else {
				ag.Closed = append(ag.Closed, t.ToolView)
			}
		}
		ua.Agents = append(ua.Agents, ag)
	}
	return ua
}

// UserGroups are the groups userID is a member of.
func (inv *Inventory) UserGroups(userID string) []*GroupView {
	var groups []*GroupView
	for _, g := range inv.Groups {
		if slices.Contains(g.Members, userID) {
			groups = append(groups, g)
		}
	}
	return groups
}

func (inv *Inventory) Rule(id int64) *RuleView     { return inv.rules[id] }
func (inv *Inventory) Group(id string) *GroupView  { return inv.groups[id] }
func (inv *Inventory) MCPServers() []string        { return inv.mcpServers }
func (inv *Inventory) AccessUsers() []store.User   { return inv.users }
func (inv *Inventory) AccessUser(id string) string { return userName(inv.users, id) }

func userName(users []store.User, id string) string {
	for _, u := range users {
		if u.ID == id {
			return u.Name()
		}
	}
	return id
}

// accessAlerts are the rules that close something nobody (but the admins)
// may use, and those that target nothing, or nothing any more.
func accessAlerts(inv *Inventory, add func(level, link, format string, args ...any)) {
	for _, rv := range inv.Rules {
		link := fmt.Sprintf("/admin/access/rules/%d", rv.ID)
		if !rv.HasHolder() {
			add(LevelDanger, link, "La règle sur %s n'a aucun détenteur : ce qu'elle vise est fermé à tous sauf aux admins.", rv.Label())
		}
		switch {
		case rv.Lost():
			add(LevelDanger, link, "La règle sur %s ne vise plus aucun outil : un serveur MCP renommé ? Ce qu'elle fermait serait ouvert à tous sous son nouveau nom. Refais ses règles.", rv.Label())
		case rv.TargetsNothing():
			add(LevelWarning, link, "La règle sur %s ne vise aucun outil publié ni aucun agent aujourd'hui.", rv.Label())
		}
	}
}

// --- Handlers ---

// accessFlash says when a change of the rules applies, after it is saved.
func accessFlash(r *http.Request) string {
	q := r.URL.Query()
	s := q.Get("access")
	if s == "" {
		return ""
	}
	at, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return ""
	}
	t := time.Unix(at, 0)
	return fmt.Sprintf("Enregistré à %s. Les workers relisent les droits toutes les %d s : appliqué aux tours qui démarrent après %s au plus tard ; un tour en cours garde ses outils.",
		t.Format("15:04:05"), int(catalogRefresh.Seconds()), t.Add(catalogRefresh).Format("15:04:05"))
}

func accessSavedURL(path string) string {
	return path + "?access=" + strconv.FormatInt(time.Now().Unix(), 10)
}

func (a *Admin) accessPage(w http.ResponseWriter, r *http.Request) {
	a.page(w, r, "access", "access", func(*Inventory) (any, bool) { return groupForm{}, true })
}

// ruleForm is the create/edit form of a rule, holding what was typed.
type ruleForm struct {
	New     bool
	ID      int64
	Kind    string
	Server  string // the target of a server rule
	Pattern string // the target of a pattern rule
	Groups  []string
	Users   []string
	Error   string
	Preview *RuleView
}

func (f ruleForm) rule() access.Rule {
	target := f.Pattern
	if f.Kind == access.KindMCPServer {
		target = f.Server
	}
	return access.Rule{ID: f.ID, Kind: f.Kind, Target: strings.TrimSpace(target), Groups: f.Groups, Users: f.Users}
}

func (f ruleForm) check() error {
	r := f.rule()
	switch {
	case !access.ValidKind(r.Kind):
		return errors.New("cible inconnue : un serveur MCP ou un motif")
	case r.Target == "" && r.Kind == access.KindMCPServer:
		return errors.New("choisis un serveur MCP")
	case r.Target == "":
		return errors.New("le motif est vide")
	case !access.ValidTarget(r.Kind, r.Target):
		return fmt.Errorf("motif invalide : %q", r.Target)
	}
	return nil
}

func ruleFormFromRequest(r *http.Request) ruleForm {
	r.ParseForm()
	return ruleForm{
		Kind:    r.FormValue("kind"),
		Server:  r.FormValue("server"),
		Pattern: strings.TrimSpace(r.FormValue("pattern")),
		Groups:  r.Form["group"],
		Users:   r.Form["user"],
	}
}

// previewRule builds the inventory as if the form were saved: what the rule
// targets and takes away come from the same computation as the pages.
func previewRule(in Inputs, f ruleForm) *RuleView {
	candidate := store.AccessRule{Rule: f.rule()}
	if candidate.ID == 0 {
		candidate.ID = -1
	}
	rules := slices.DeleteFunc(slices.Clone(in.Rules), func(r store.AccessRule) bool { return r.ID == candidate.ID })
	in.Rules = append(rules, candidate)
	return BuildInventory(in).Rule(candidate.ID)
}

func (a *Admin) renderRuleForm(w http.ResponseWriter, r *http.Request, f ruleForm) {
	snap, err := a.load(r.Context())
	if err != nil {
		log.Printf("admin: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if f.Kind == "" {
		f.Kind = access.KindMCPServer
	}
	f.Preview = previewRule(snap.in, f)
	a.pages.page(w, "rule_edit", pageData{Nav: "access", Inv: snap.inv, Data: f, Flash: accessFlash(r), Me: auth.UserFrom(r.Context())})
}

func (a *Admin) newRuleForm(w http.ResponseWriter, r *http.Request) {
	a.renderRuleForm(w, r, ruleForm{New: true, Kind: cmp.Or(r.URL.Query().Get("kind"), access.KindMCPServer), Pattern: r.URL.Query().Get("pattern")})
}

func (a *Admin) editRuleForm(w http.ResponseWriter, r *http.Request) {
	id, _ := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	rule, err := a.cfg.Store.GetAccessRule(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if rule == nil {
		http.NotFound(w, r)
		return
	}
	f := ruleForm{ID: rule.ID, Kind: rule.Kind, Groups: rule.Groups, Users: rule.Users}
	if rule.Kind == access.KindMCPServer {
		f.Server = rule.Target
	} else {
		f.Pattern = rule.Target
	}
	a.renderRuleForm(w, r, f)
}

func (a *Admin) createRule(w http.ResponseWriter, r *http.Request) {
	f := ruleFormFromRequest(r)
	f.New = true
	err := f.check()
	var id int64
	if err == nil {
		id, err = a.cfg.Store.CreateAccessRule(r.Context(), f.rule())
	}
	if err != nil {
		f.Error = err.Error()
		a.renderRuleForm(w, r, f)
		return
	}
	log.Printf("admin: access rule %d (%s %s) created by %s, groups %v, users %v", id, f.Kind, f.rule().Target, auth.UserFrom(r.Context()).ID, f.Groups, f.Users)
	navigate(w, r, accessSavedURL(fmt.Sprintf("/admin/access/rules/%d", id)))
}

func (a *Admin) updateRule(w http.ResponseWriter, r *http.Request) {
	f := ruleFormFromRequest(r)
	f.ID, _ = strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	err := f.check()
	if err == nil {
		err = a.cfg.Store.UpdateAccessRule(r.Context(), f.rule())
	}
	switch {
	case errors.Is(err, store.ErrRuleNotFound):
		http.NotFound(w, r)
	case err != nil:
		f.Error = err.Error()
		a.renderRuleForm(w, r, f)
	default:
		log.Printf("admin: access rule %d (%s %s) updated by %s, groups %v, users %v", f.ID, f.Kind, f.rule().Target, auth.UserFrom(r.Context()).ID, f.Groups, f.Users)
		navigate(w, r, accessSavedURL(fmt.Sprintf("/admin/access/rules/%d", f.ID)))
	}
}

func (a *Admin) deleteRule(w http.ResponseWriter, r *http.Request) {
	id, _ := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	switch err := a.cfg.Store.DeleteAccessRule(r.Context(), id); {
	case errors.Is(err, store.ErrRuleNotFound):
		http.NotFound(w, r)
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	default:
		log.Printf("admin: access rule %d deleted by %s", id, auth.UserFrom(r.Context()).ID)
		navigate(w, r, accessSavedURL("/admin/access"))
	}
}

// rulePreview re-renders what the rule being edited targets and takes away.
func (a *Admin) rulePreview(w http.ResponseWriter, r *http.Request) {
	snap, err := a.load(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	f := ruleFormFromRequest(r)
	f.ID, _ = strconv.ParseInt(r.FormValue("id"), 10, 64)
	a.pages.execute(w, "rule_edit", "rule_preview", previewRule(snap.in, f))
}

// groupForm is the group creation form on the access page, or a group's
// page, with what went wrong.
type groupForm struct {
	Group *GroupView
	Name  string
	Error string
}

func (a *Admin) renderGroup(w http.ResponseWriter, r *http.Request, id string, f groupForm) {
	a.page(w, r, "group", "access", func(inv *Inventory) (any, bool) {
		f.Group = inv.Group(id)
		return f, f.Group != nil
	})
}

func (a *Admin) groupPage(w http.ResponseWriter, r *http.Request) {
	a.renderGroup(w, r, chi.URLParam(r, "id"), groupForm{})
}

func (a *Admin) createGroup(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimSpace(r.FormValue("name"))
	id := uuid.NewString()
	err := errors.New("le nom est vide")
	if name != "" {
		err = a.cfg.Store.CreateGroup(r.Context(), store.Group{ID: id, Name: name})
	}
	if err != nil {
		if errors.Is(err, store.ErrGroupExists) {
			err = errors.New("un groupe porte déjà ce nom")
		}
		a.page(w, r, "access", "access", func(*Inventory) (any, bool) { return groupForm{Name: name, Error: err.Error()}, true })
		return
	}
	log.Printf("admin: group %s (%s) created by %s", id, name, auth.UserFrom(r.Context()).ID)
	navigate(w, r, "/admin/access/groups/"+url.PathEscape(id))
}

func (a *Admin) renameGroup(w http.ResponseWriter, r *http.Request) {
	id, name := chi.URLParam(r, "id"), strings.TrimSpace(r.FormValue("name"))
	err := errors.New("le nom est vide")
	if name != "" {
		err = a.cfg.Store.RenameGroup(r.Context(), id, name)
	}
	switch {
	case errors.Is(err, store.ErrGroupNotFound):
		http.NotFound(w, r)
	case errors.Is(err, store.ErrGroupExists):
		a.renderGroup(w, r, id, groupForm{Error: "un groupe porte déjà ce nom"})
	case err != nil:
		a.renderGroup(w, r, id, groupForm{Error: err.Error()})
	default:
		navigate(w, r, accessSavedURL("/admin/access/groups/"+url.PathEscape(id)))
	}
}

func (a *Admin) addGroupMember(w http.ResponseWriter, r *http.Request) {
	id, email := chi.URLParam(r, "id"), strings.TrimSpace(r.FormValue("email"))
	u, err := a.cfg.Store.GetUserByEmail(r.Context(), email)
	if err == nil && u == nil {
		err = fmt.Errorf("aucun utilisateur %q", email)
	}
	if err == nil {
		err = a.cfg.Store.AddGroupMember(r.Context(), id, u.ID)
	}
	if err != nil {
		a.renderGroup(w, r, id, groupForm{Error: err.Error()})
		return
	}
	log.Printf("admin: user %s added to group %s by %s", u.ID, id, auth.UserFrom(r.Context()).ID)
	navigate(w, r, accessSavedURL("/admin/access/groups/"+url.PathEscape(id)))
}

func (a *Admin) removeGroupMember(w http.ResponseWriter, r *http.Request) {
	id, userID := chi.URLParam(r, "id"), chi.URLParam(r, "userID")
	if err := a.cfg.Store.RemoveGroupMember(r.Context(), id, userID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("admin: user %s taken out of group %s by %s", userID, id, auth.UserFrom(r.Context()).ID)
	navigate(w, r, accessSavedURL("/admin/access/groups/"+url.PathEscape(id)))
}

func (a *Admin) deleteGroup(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	err := a.cfg.Store.DeleteGroup(r.Context(), id)
	var sole *store.SoleHolderError
	switch {
	case errors.As(err, &sole):
		targets := make([]string, len(sole.Rules))
		for i, rule := range sole.Rules {
			targets[i] = (&RuleView{AccessRule: rule}).Label()
		}
		a.renderGroup(w, r, id, groupForm{Error: "Ce groupe est le seul détenteur des règles sur " + strings.Join(targets, ", ") +
			" : supprimé, il les fermerait à tous sauf aux admins. Modifie-les d'abord."})
	case errors.Is(err, store.ErrGroupNotFound):
		http.NotFound(w, r)
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	default:
		log.Printf("admin: group %s deleted by %s", id, auth.UserFrom(r.Context()).ID)
		navigate(w, r, accessSavedURL("/admin/access"))
	}
}
