package access

import (
	"testing"
)

var (
	crmSearch = FromMCP("crm", "crm_search")
	crmUpdate = FromMCP("crm", "crm_update_contact")
	webFetch  = BuiltIn("web_fetch")
	webSearch = FromMCP("web", "web_search")
	execTool  = BuiltIn("exec")
)

func TestAllowed_OpenWithoutRules(t *testing.T) {
	alice := &User{ID: "alice"}
	for _, tool := range []Tool{crmSearch, webFetch, execTool, Agent("commercial")} {
		if !Allowed(alice, tool, nil) {
			t.Errorf("%s closed without any rule", tool.Name)
		}
		if !Allowed(nil, tool, nil) {
			t.Errorf("%s closed to no user without any rule", tool.Name)
		}
	}
}

func TestAllowed_ServerIsNotAPrefix(t *testing.T) {
	rules := []Rule{{Kind: KindMCPServer, Target: "web", Groups: []string{"g-research"}}}
	bob := &User{ID: "bob"}
	if Allowed(bob, webSearch, rules) {
		t.Error("the web server's tool open to bob")
	}
	if !Allowed(bob, webFetch, rules) {
		t.Error("web_fetch, a built-in tool, closed by the rule on the server web")
	}
	// A pattern is a name: web_* takes both.
	rules = []Rule{{Kind: KindPattern, Target: "web_*", Users: []string{"alice"}}}
	if Allowed(bob, webFetch, rules) || Allowed(bob, webSearch, rules) {
		t.Error("web_* left a web_ tool open to bob")
	}
}

// A tool is from its MCP server; a built-in tool and an agent from none,
// whatever a server is called.
func TestSource(t *testing.T) {
	if server, ok := crmSearch.Source.MCPServer(); !ok || server != "crm" {
		t.Errorf("crm_search from %q (%v)", server, ok)
	}
	if _, ok := execTool.Source.MCPServer(); ok {
		t.Error("a built-in tool from a server")
	}
	rules := []Rule{{Kind: KindMCPServer, Target: "agent", Users: []string{"alice"}}}
	if !Allowed(&User{ID: "bob"}, Agent("x"), rules) {
		t.Error("an agent closed by a rule on a server")
	}
}

func TestAllowed_EveryRuleIsALock(t *testing.T) {
	rules := []Rule{
		{Kind: KindMCPServer, Target: "crm", Groups: []string{"sales"}},
		{Kind: KindPattern, Target: "crm_update_*", Groups: []string{"managers"}},
	}
	seller := &User{ID: "s", Groups: []string{"sales"}}
	manager := &User{ID: "m", Groups: []string{"managers"}}
	both := &User{ID: "b", Groups: []string{"managers", "sales"}}
	cases := []struct {
		u    *User
		tool Tool
		want bool
	}{
		{seller, crmSearch, true},
		{seller, crmUpdate, false},
		{manager, crmSearch, false},
		{manager, crmUpdate, false},
		{both, crmUpdate, true},
		{seller, execTool, true},
	}
	for _, c := range cases {
		if got := Allowed(c.u, c.tool, rules); got != c.want {
			t.Errorf("%s on %s: %v, want %v", c.u.ID, c.tool.Name, got, c.want)
		}
	}
}

func TestAllowed_AdminsAndDisabledUsers(t *testing.T) {
	rules := []Rule{{Kind: KindPattern, Target: "exec", Users: []string{"alice"}}}
	if !Allowed(&User{ID: "root", Admin: true}, execTool, rules) {
		t.Error("an admin refused")
	}
	if Allowed(&User{ID: "root", Admin: true, Disabled: true}, execTool, rules) {
		t.Error("a disabled admin allowed")
	}
	if Allowed(&User{ID: "alice", Disabled: true}, webFetch, rules) {
		t.Error("a disabled user allowed a tool no rule targets")
	}
}

func TestAllowed_HoldersDirectAndByGroup(t *testing.T) {
	rules := []Rule{{Kind: KindPattern, Target: "agent_commercial", Users: []string{"victor"}, Groups: []string{"sales"}}}
	tool := Agent("commercial")
	if !Allowed(&User{ID: "victor"}, tool, rules) {
		t.Error("a named user refused")
	}
	if !Allowed(&User{ID: "alice", Groups: []string{"sales"}}, tool, rules) {
		t.Error("a member of a group refused")
	}
	if Allowed(&User{ID: "bob", Groups: []string{"other"}}, tool, rules) {
		t.Error("someone else allowed")
	}
	if !Allowed(&User{ID: "bob"}, Agent("default"), rules) {
		t.Error("another agent closed")
	}
}

func TestAllowed_RuleWithoutHolder(t *testing.T) {
	rules := []Rule{{Kind: KindPattern, Target: "exec"}}
	if rules[0].HasHolder() {
		t.Error("a rule without groups nor users has a holder")
	}
	if Allowed(&User{ID: "alice", Groups: []string{"g"}}, execTool, rules) {
		t.Error("a rule without holder let a user through")
	}
	if !Allowed(&User{ID: "root", Admin: true}, execTool, rules) {
		t.Error("a rule without holder refused an admin")
	}
}

func TestAllowed_StarAndNoUser(t *testing.T) {
	rules := []Rule{{Kind: KindPattern, Target: "*", Groups: []string{"staff"}}}
	if Allowed(&User{ID: "bob"}, webFetch, rules) || Allowed(&User{ID: "bob"}, Agent("x"), rules) {
		t.Error("* left a tool open")
	}
	if !Allowed(&User{ID: "s", Groups: []string{"staff"}}, Agent("x"), rules) {
		t.Error("* refused its group")
	}
	// Without a user: only what no rule targets.
	rules = []Rule{{Kind: KindPattern, Target: "exec", Groups: []string{"staff"}}}
	if Allowed(nil, execTool, rules) {
		t.Error("no user allowed a targeted tool")
	}
	if !Allowed(nil, webFetch, rules) {
		t.Error("no user refused a tool no rule targets")
	}
}

func TestSnapshot_UnknownUser(t *testing.T) {
	s := Snapshot{
		Rules: []Rule{{Kind: KindPattern, Target: "exec", Users: []string{"new"}}},
		Users: map[string]User{"bob": {ID: "bob", Disabled: true}},
	}
	if !s.Allowed("new", execTool) {
		t.Error("a user the snapshot does not know yet lost what names them")
	}
	if s.Allowed("bob", webFetch) {
		t.Error("a disabled user allowed")
	}
	if s.Allowed("", execTool) || !s.Allowed("", webFetch) {
		t.Error("no user: only the tools no rule targets")
	}
}

func TestMatching(t *testing.T) {
	rules := []Rule{
		{ID: 1, Kind: KindMCPServer, Target: "crm"},
		{ID: 2, Kind: KindPattern, Target: "crm_update_*"},
		{ID: 3, Kind: KindPattern, Target: "exec"},
		{ID: 4, Kind: "nonsense", Target: "*"},
	}
	got := Matching(crmUpdate, rules)
	if len(got) != 2 || got[0].ID != 1 || got[1].ID != 2 {
		t.Errorf("matching %v", got)
	}
	if len(Matching(webFetch, rules)) != 0 {
		t.Error("a rule of an unknown kind matched")
	}
}

func TestValidTarget(t *testing.T) {
	if ValidTarget(KindPattern, "[") || ValidTarget(KindPattern, "") || ValidTarget(KindMCPServer, "") {
		t.Error("an invalid target accepted")
	}
	if !ValidTarget(KindPattern, "crm_*") || !ValidTarget(KindMCPServer, "crm") {
		t.Error("a valid target refused")
	}
}

func TestRemoves(t *testing.T) {
	agents := []Tool{Agent("default"), Agent("commercial")}
	users := []User{
		{ID: "alice", Groups: []string{"sales"}},
		{ID: "bob"},
		{ID: "root", Admin: true},
		{ID: "gone", Disabled: true},
	}
	// agent_* to sales: bob loses every agent; alice keeps hers, the admin
	// all, and the disabled user had none.
	left := Removes(Rule{Kind: KindPattern, Target: "agent_*", Groups: []string{"sales"}}, nil, users, agents)
	if len(left) != 1 || left[0].ID != "bob" {
		t.Errorf("left without agent: %v", left)
	}
	// One agent: everyone keeps the other.
	if left := Removes(Rule{Kind: KindPattern, Target: "agent_commercial", Groups: []string{"sales"}}, nil, users, agents); len(left) != 0 {
		t.Errorf("left without agent: %v", left)
	}
	// The other one closed already: bob had only commercial left.
	others := []Rule{{Kind: KindPattern, Target: "agent_default", Users: []string{"alice"}}}
	left = Removes(Rule{Kind: KindPattern, Target: "agent_commercial", Groups: []string{"sales"}}, others, users, agents)
	if len(left) != 1 || left[0].ID != "bob" {
		t.Errorf("left without agent: %v", left)
	}
}
