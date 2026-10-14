// Package access decides who may use which tool, and so which agent
// (docs/design/permissions.md). Everything is open by default; a rule
// closes what it targets, a server's tools or the tools a name pattern
// matches, and says who keeps access: groups and users. A tool several
// rules target requires each of them: adding a rule never opens anything.
//
// The package is pure, with no store and no Temporal: the workers' catalog,
// the server and the back-office decide with the same functions.
package access

import (
	"path"
	"slices"
)

// The kinds of target a rule has.
const (
	// KindMCPServer: every tool of an MCP server, by its name in the
	// workers' configuration, those it adds later included.
	KindMCPServer = "mcp_server"
	// KindPattern: the tools whose name matches a glob, the globs of the
	// agents' tool lists (tool.MatchAny). An agent is the tool agent_<id>.
	KindPattern = "pattern"
)

// ValidKind reports a rule's kind.
func ValidKind(kind string) bool { return kind == KindMCPServer || kind == KindPattern }

// Rule closes the tools it targets to everyone but its holders: the members
// of its groups and its users (admins aside).
type Rule struct {
	ID     int64    `json:"id"`
	Kind   string   `json:"kind"`
	Target string   `json:"target"`
	Groups []string `json:"groups,omitempty"` // group IDs
	Users  []string `json:"users,omitempty"`  // user IDs
}

// User is who a turn answers, as the rules see them.
type User struct {
	ID       string   `json:"id"`
	Admin    bool     `json:"admin,omitempty"`
	Disabled bool     `json:"disabled,omitempty"`
	Groups   []string `json:"groups,omitempty"` // group IDs
}

// Tool is a tool as the rules see it: its name, which a pattern matches,
// and its source, which a rule on a server matches.
type Tool struct {
	Name   string
	Source Source
}

// Source is where a tool comes from: the platform (built-in), an agent (the
// tool that delegates to it), or an MCP server. The zero Source is built-in.
type Source struct {
	kind   sourceKind
	server string
}

type sourceKind int

const (
	sourceBuiltIn sourceKind = iota
	sourceAgent
	sourceMCP
)

// MCPServer is the MCP server t comes from, and whether it comes from one.
func (s Source) MCPServer() (string, bool) { return s.server, s.kind == sourceMCP }

// AgentToolPrefix names the tool that delegates to an agent, agent_<id>
// (activity.AgentToolPrefix): a rule on an agent is a rule on that tool.
const AgentToolPrefix = "agent_"

// BuiltIn is a tool the platform provides.
func BuiltIn(name string) Tool { return Tool{Name: name} }

// Agent is the tool of agentID.
func Agent(agentID string) Tool {
	return Tool{Name: AgentToolPrefix + agentID, Source: Source{kind: sourceAgent}}
}

// FromMCP is the tool name of server.
func FromMCP(server, name string) Tool {
	return Tool{Name: name, Source: Source{kind: sourceMCP, server: server}}
}

// ValidTarget reports whether the target an admin typed is well formed: a
// pattern is a valid glob, a server's name any non-empty name.
func ValidTarget(kind, target string) bool {
	if target == "" {
		return false
	}
	if kind == KindPattern {
		return validGlob(target)
	}
	return true
}

// validGlob reports whether glob is well written: wildcards * ? [...] as
// path.Match reads them ("crm_*" is valid, "[" is not). Go has no function
// that only checks a glob: path.Match tries it on any name, here an empty
// one whose match does not matter, and returns ErrBadPattern, whatever the
// name, for a malformed glob.
func validGlob(glob string) bool {
	_, err := path.Match(glob, "")
	return err == nil
}

// Matches reports whether r targets t. A server is matched by the server a
// tool comes from, never by its name's prefix: a server named "web" gives
// tools "web_…", like web_fetch, which is not its own.
func (r Rule) Matches(t Tool) bool {
	switch r.Kind {
	case KindMCPServer:
		server, ok := t.Source.MCPServer()
		return ok && server == r.Target
	case KindPattern:
		ok, _ := path.Match(r.Target, t.Name)
		return ok
	}
	return false
}

// Holds reports whether u keeps access through r: named, or a member of
// one of its groups. Admins and disabled users are Allowed's business.
func (r Rule) Holds(u User) bool {
	if slices.Contains(r.Users, u.ID) {
		return true
	}
	for _, g := range u.Groups {
		if slices.Contains(r.Groups, g) {
			return true
		}
	}
	return false
}

// HasHolder reports whether r names any group or user: a rule without
// closes its tools to everyone but the admins.
func (r Rule) HasHolder() bool { return len(r.Groups) > 0 || len(r.Users) > 0 }

// Matching returns the rules that target t, in order.
func Matching(t Tool, rules []Rule) []Rule {
	var matching []Rule
	for _, r := range rules {
		if r.Matches(t) {
			matching = append(matching, r)
		}
	}
	return matching
}

// Targets returns the tools of tools that r targets, in order.
func Targets(r Rule, tools []Tool) []Tool {
	var targeted []Tool
	for _, t := range tools {
		if r.Matches(t) {
			targeted = append(targeted, t)
		}
	}
	return targeted
}

// Allowed decides whether u may use t:
//   - no user (nil): only a tool no rule targets;
//   - a disabled user: nothing, admin or not;
//   - an admin: everything;
//   - anyone else: a tool every rule targeting it lets them keep.
func Allowed(u *User, t Tool, rules []Rule) bool {
	if u == nil {
		return len(Matching(t, rules)) == 0
	}
	if u.Disabled {
		return false
	}
	if u.Admin {
		return true
	}
	for _, r := range rules {
		if r.Matches(t) && !r.Holds(*u) {
			return false
		}
	}
	return true
}

// Snapshot is what the rules need, read at once: the rules, and every user
// by ID with their role, state and groups.
type Snapshot struct {
	Rules []Rule          `json:"rules,omitempty"`
	Users map[string]User `json:"users,omitempty"`
}

// User is userID as the rules see them: nil without a user; a user the
// snapshot does not know yet (created since it was read) has no group and
// no role.
func (s Snapshot) User(userID string) *User {
	if userID == "" {
		return nil
	}
	if u, ok := s.Users[userID]; ok {
		return &u
	}
	return &User{ID: userID}
}

// Allowed is Allowed for userID.
func (s Snapshot) Allowed(userID string, t Tool) bool {
	return Allowed(s.User(userID), t, s.Rules)
}

// Removes returns the users a rule would leave without any agent: those who
// may use at least one of agents under the other rules, and none with rule
// added (or changed: others hold every rule but this one). A wide rule
// ("agent_*", "*") given to a small group takes every agent from the rest.
func Removes(rule Rule, others []Rule, users []User, agents []Tool) []User {
	with := append(slices.Clone(others), rule)
	var left []User
	for _, u := range users {
		before, after := false, false
		for _, a := range agents {
			before = before || Allowed(&u, a, others)
			after = after || Allowed(&u, a, with)
		}
		if before && !after {
			left = append(left, u)
		}
	}
	return left
}
