package admin

import (
	"context"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/victor/temporal-agent/access"
	"github.com/victor/temporal-agent/store"
)

// Groups and rules in memory, with the store's refusal of a group that
// alone holds a rule.

func (f *fakeStore) ListGroups(context.Context) ([]store.Group, error) { return f.groups, nil }
func (f *fakeStore) CreateGroup(_ context.Context, g store.Group) error {
	for _, e := range f.groups {
		if strings.EqualFold(e.Name, g.Name) {
			return store.ErrGroupExists
		}
	}
	f.groups = append(f.groups, g)
	return nil
}
func (f *fakeStore) group(id string) *store.Group {
	for i := range f.groups {
		if f.groups[i].ID == id {
			return &f.groups[i]
		}
	}
	return nil
}
func (f *fakeStore) RenameGroup(_ context.Context, id, name string) error {
	g := f.group(id)
	if g == nil {
		return store.ErrGroupNotFound
	}
	g.Name = name
	return nil
}
func (f *fakeStore) DeleteGroup(_ context.Context, id string) error {
	var sole []store.AccessRule
	for _, r := range f.rules {
		if slices.Equal(r.Groups, []string{id}) && len(r.Users) == 0 {
			sole = append(sole, r)
		}
	}
	if len(sole) > 0 {
		return &store.SoleHolderError{Rules: sole}
	}
	f.groups = slices.DeleteFunc(f.groups, func(g store.Group) bool { return g.ID == id })
	return nil
}
func (f *fakeStore) AddGroupMember(_ context.Context, groupID, userID string) error {
	if g := f.group(groupID); g != nil && !slices.Contains(g.Members, userID) {
		g.Members = append(g.Members, userID)
	}
	return nil
}
func (f *fakeStore) RemoveGroupMember(_ context.Context, groupID, userID string) error {
	if g := f.group(groupID); g != nil {
		g.Members = slices.DeleteFunc(g.Members, func(m string) bool { return m == userID })
	}
	return nil
}
func (f *fakeStore) ListAccessRules(context.Context) ([]store.AccessRule, error) { return f.rules, nil }
func (f *fakeStore) GetAccessRule(_ context.Context, id int64) (*store.AccessRule, error) {
	for _, r := range f.rules {
		if r.ID == id {
			return &r, nil
		}
	}
	return nil, nil
}
func (f *fakeStore) CreateAccessRule(_ context.Context, r access.Rule) (int64, error) {
	r.ID = int64(len(f.rules) + 1)
	f.rules = append(f.rules, store.AccessRule{Rule: r})
	return r.ID, nil
}
func (f *fakeStore) UpdateAccessRule(_ context.Context, r access.Rule) error {
	for i := range f.rules {
		if f.rules[i].ID == r.ID {
			f.rules[i] = store.AccessRule{Rule: r}
			return nil
		}
	}
	return store.ErrRuleNotFound
}
func (f *fakeStore) DeleteAccessRule(_ context.Context, id int64) error {
	f.rules = slices.DeleteFunc(f.rules, func(r store.AccessRule) bool { return r.ID == id })
	return nil
}
func (f *fakeStore) ListMCPServers(context.Context) ([]string, error) {
	var servers []string
	for _, t := range f.tools {
		if t.MCPServer != "" && !slices.Contains(servers, t.MCPServer) {
			servers = append(servers, t.MCPServer)
		}
	}
	return servers, nil
}

// accessInputs is an installation with a CRM server, a commercial agent and
// a sales group.
func accessInputs() Inputs {
	at := time.Now()
	return Inputs{
		Agents: []store.Agent{
			{ID: "default", Name: "Jarvis", Tools: []string{"*"}},
			{ID: "commercial", Name: "Commercial", Tools: []string{"crm_*", "exec"}},
		},
		Tools: []store.ToolRecord{
			{Name: "crm_search", MCPServer: "crm", TaskQueue: "tools"},
			{Name: "crm_update_contact", MCPServer: "crm", TaskQueue: "tools"},
			{Name: "exec", TaskQueue: "tools", Sensitive: true},
			{Name: "send_email", TaskQueue: "tools", Sensitive: true},
		},
		Users: []store.User{
			{ID: "u-alice", Email: "alice@example.com", Role: store.UserRoleStandard},
			{ID: "u-bob", Email: "bob@example.com", Role: store.UserRoleStandard},
			{ID: "u-root", Email: "root@example.com", Role: store.UserRoleAdmin},
		},
		Groups: []store.Group{{ID: "g-sales", Name: "Commercial", Members: []string{"u-alice"}}, {ID: "g-mgr", Name: "Managers"}},
		Rules: []store.AccessRule{
			{Rule: access.Rule{ID: 1, Kind: access.KindMCPServer, Target: "crm", Groups: []string{"g-sales"}}},
			{Rule: access.Rule{ID: 2, Kind: access.KindPattern, Target: "crm_update_*", Groups: []string{"g-mgr"}}},
			{Rule: access.Rule{ID: 3, Kind: access.KindPattern, Target: "agent_commercial", Users: []string{"u-alice"}}},
			{Rule: access.Rule{ID: 4, Kind: access.KindMCPServer, Target: "sales"}, LastMatchedAt: &at},
		},
		WorkflowQueue: "agent",
	}
}

// The inventory says who may use each tool, what each rule targets, the
// agents a user may use and with which tools, and the rules to look at.
func TestInventory_Access(t *testing.T) {
	inv := BuildInventory(accessInputs())

	if got := inv.Tool("crm_update_contact").Who(); got != "(Commercial) et (Managers)" {
		t.Errorf("crm_update_contact: %q", got)
	}
	if got := inv.Tool("crm_search").Who(); got != "Commercial" {
		t.Errorf("crm_search: %q", got)
	}
	if inv.Tool("exec").Who() != "" || len(inv.SensitiveOpen()) != 2 {
		t.Errorf("exec %q, sensitive open %d", inv.Tool("exec").Who(), len(inv.SensitiveOpen()))
	}
	if self := inv.Agent("commercial").Self; self == nil || self.Who() != "alice@example.com" {
		t.Errorf("the agent's own rule: %+v", self)
	}
	var targets []string
	for _, tv := range inv.Rule(1).Targets {
		targets = append(targets, tv.Name)
	}
	if !slices.Equal(targets, []string{"crm_search", "crm_update_contact"}) {
		t.Errorf("rule 1 targets %v", targets)
	}
	if !inv.Rule(4).Lost() || inv.Rule(4).HasHolder() || inv.Rule(1).Lost() {
		t.Errorf("rule 4 lost %v, held %v", inv.Rule(4).Lost(), inv.Rule(4).HasHolder())
	}
	if g := inv.Group("g-sales"); len(g.SoleOf) != 1 || g.SoleOf[0].ID != 1 {
		t.Errorf("sales holds alone %+v", g.SoleOf)
	}
	alerts := 0
	for _, a := range inv.Alerts {
		if strings.Contains(a.Message, "serveur sales") {
			alerts++
		}
	}
	if alerts != 2 {
		t.Errorf("alerts %+v, want the rule on sales held by nobody and targeting nothing any more", inv.Alerts)
	}

	ua := inv.UserAccess("u-bob")
	for _, ag := range ua.Agents {
		var open, closed []string
		for _, tv := range ag.Tools {
			open = append(open, tv.Name)
		}
		for _, tv := range ag.Closed {
			closed = append(closed, tv.Name)
		}
		switch ag.Agent.ID {
		case "commercial":
			if ag.Open || !slices.Equal(open, []string{"exec"}) || !slices.Equal(closed, []string{"crm_search", "crm_update_contact"}) {
				t.Errorf("bob's commercial: open %v, tools %v, closed %v", ag.Open, open, closed)
			}
		case "default":
			if !ag.Open || !slices.Contains(closed, "agent_commercial") || slices.Contains(open, "crm_search") {
				t.Errorf("bob's default: open %v, tools %v, closed %v", ag.Open, open, closed)
			}
		}
	}
}

// A wide rule says, before it is saved, who it leaves without any agent.
func TestPreviewRule_SaysWhatItTakes(t *testing.T) {
	in := accessInputs()
	rv := previewRule(in, ruleForm{Kind: access.KindPattern, Pattern: "agent_*", Groups: []string{"g-sales"}})
	if len(rv.Removes) != 1 || rv.Removes[0].ID != "u-bob" {
		t.Errorf("removes %+v", rv.Removes)
	}
	if len(rv.Targets) != 2 {
		t.Errorf("targets %d", len(rv.Targets))
	}
	// Editing a rule replaces it in the preview.
	rv = previewRule(in, ruleForm{ID: 3, Kind: access.KindPattern, Pattern: "agent_commercial", Users: []string{"u-alice", "u-bob"}})
	if len(rv.Removes) != 0 || rv.Holders() != "alice@example.com, bob@example.com" {
		t.Errorf("edited rule %+v", rv)
	}
}

// The pages: rules created and edited, a group created, its members, and
// its deletion refused while it alone holds a rule; a user's page shows
// what they may use.
func TestAccessPages(t *testing.T) {
	a, st := newTestAdmin(t)
	h := handler(a)
	c := login(t, h)

	if w := do(h, http.MethodGet, "/admin/access", nil, c); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "Aucune règle") {
		t.Fatalf("access page: %d %s", w.Code, w.Body)
	}
	w := do(h, http.MethodPost, "/admin/access/groups", url.Values{"name": {"Dev"}}, c)
	if w.Code != http.StatusSeeOther || len(st.groups) != 1 {
		t.Fatalf("create group: %d %s", w.Code, w.Body)
	}
	gid := st.groups[0].ID
	if w := do(h, http.MethodPost, "/admin/access/groups/"+gid+"/members", url.Values{"email": {"bob@example.com"}}, c); w.Code != http.StatusSeeOther || !slices.Equal(st.groups[0].Members, []string{"u-bob"}) {
		t.Fatalf("add member: %d %v", w.Code, st.groups[0].Members)
	}

	// A bad pattern is refused, the form kept.
	if w := do(h, http.MethodPost, "/admin/access/rules", url.Values{"kind": {"pattern"}, "pattern": {"["}, "group": {gid}}, c); w.Code != http.StatusOK || len(st.rules) != 0 || !strings.Contains(w.Body.String(), "motif invalide") {
		t.Errorf("bad pattern: %d, rules %v", w.Code, st.rules)
	}
	w = do(h, http.MethodPost, "/admin/access/rules", url.Values{"kind": {"pattern"}, "pattern": {"exec"}, "group": {gid}}, c)
	if w.Code != http.StatusSeeOther || len(st.rules) != 1 || st.rules[0].Target != "exec" || !strings.HasPrefix(w.Header().Get("Location"), "/admin/access/rules/1?access=") {
		t.Fatalf("create rule: %d %s, rules %v", w.Code, w.Header().Get("Location"), st.rules)
	}
	if w := do(h, http.MethodGet, w.Header().Get("Location"), nil, c); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "relisent les droits toutes les 30 s") {
		t.Errorf("rule page: %d", w.Code)
	}

	// The group alone holds the rule: not deleted, the rule named.
	if w := do(h, http.MethodPost, "/admin/access/groups/"+gid+"/delete", nil, c); w.Code != http.StatusOK || len(st.groups) != 1 || !strings.Contains(w.Body.String(), "seul détenteur des règles sur exec") {
		t.Errorf("delete the only holder: %d, groups %v", w.Code, st.groups)
	}

	// Bob's page: exec open to him through the group, the coder answers him.
	w = do(h, http.MethodGet, "/admin/users/u-bob/edit", nil, c)
	if body := w.Body.String(); w.Code != http.StatusOK || !strings.Contains(body, "Ce qu'il peut utiliser") || !strings.Contains(body, `<span class="chip sens">exec</span>`) {
		t.Errorf("user page: %d %s", w.Code, body)
	}
	// The agent's page says who may use exec.
	if w := do(h, http.MethodGet, "/admin/agents/coder", nil, c); !strings.Contains(w.Body.String(), "<td>Dev</td>") {
		t.Errorf("agent page does not say who uses exec: %s", w.Body)
	}

	// Edited, then deleted.
	if w := do(h, http.MethodPost, "/admin/access/rules/1", url.Values{"kind": {"pattern"}, "pattern": {"exec"}, "user": {"u-bob"}}, c); w.Code != http.StatusSeeOther || !slices.Equal(st.rules[0].Users, []string{"u-bob"}) || st.rules[0].Groups != nil {
		t.Errorf("update rule: %d %+v", w.Code, st.rules)
	}
	if w := do(h, http.MethodGet, "/admin/access/preview", url.Values{"kind": {"pattern"}, "pattern": {"agent_*"}, "user": {"u-bob"}}, c); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "agent_coder") {
		t.Errorf("preview: %d %s", w.Code, w.Body)
	}
	if w := do(h, http.MethodPost, "/admin/access/rules/1/delete", nil, c); w.Code != http.StatusSeeOther || len(st.rules) != 0 {
		t.Errorf("delete rule: %d %v", w.Code, st.rules)
	}
	if w := do(h, http.MethodPost, "/admin/access/groups/"+gid+"/delete", nil, c); w.Code != http.StatusSeeOther || len(st.groups) != 0 {
		t.Errorf("delete group: %d %v", w.Code, st.groups)
	}
}
