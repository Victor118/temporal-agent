package chat

import (
	"strconv"
	"strings"
	"time"
)

// MachinesPage is "Mes machines": a user's machines, and how to add one.
type MachinesPage struct {
	Me       Person
	Machines []MachineRow
	// Server is the address to join, for the commands the page shows.
	Server string
	Flash  string
	Error  string
	// EnrollmentToken is shown once, right after its creation.
	EnrollmentToken string
	TokenExpires    time.Time
}

// MachineRow is one machine.
type MachineRow struct {
	ID             string
	Name           string
	OS             string
	Capabilities   []string
	Online         bool
	OpenDirectives int
	MaxDirectives  int
	AgentVersion   string
	LastAddr       string
	CreatedAt      time.Time
	SeenAt         time.Time // zero: never connected
	Revoked        bool
	RevokedReason  string
	Paused         bool
	Priority       int
	// ClaudeCode is the state of its claude CLI: "ok", "logged_out",
	// "absent"; "" = never said.
	ClaudeCode string
	// OpenKinds are the kinds of its open directives.
	OpenKinds []string
}

// ClaudeCodeText says whether the machine can run Claude Code.
func (m MachineRow) ClaudeCodeText() string {
	switch m.ClaudeCode {
	case "ok":
		return "Claude Code : connecté"
	case "logged_out":
		return "Claude Code : pas connecté (sur la machine : claude puis /login)"
	case "absent":
		return "Claude Code : absent"
	}
	return ""
}

// kindNames are the directives' kinds, in words.
var kindNames = map[string]string{"analyze_repo": "analyse de dépôt", "echo": "echo"}

// OpenText lists its open directives by kind: "2 analyses de dépôt, 1 echo".
func (m MachineRow) OpenText() string {
	if len(m.OpenKinds) == 0 {
		return "aucune"
	}
	counts := map[string]int{}
	var order []string
	for _, k := range m.OpenKinds {
		if counts[k] == 0 {
			order = append(order, k)
		}
		counts[k]++
	}
	parts := make([]string, len(order))
	for i, k := range order {
		name := kindNames[k]
		if name == "" {
			name = k
		}
		if counts[k] > 1 {
			parts[i] = strconv.Itoa(counts[k]) + " × " + name
		} else {
			parts[i] = name
		}
	}
	return strings.Join(parts, ", ")
}

// CapabilitiesText lists what the machine runs.
func (m MachineRow) CapabilitiesText() string {
	if len(m.Capabilities) == 0 {
		return "aucune"
	}
	return strings.Join(m.Capabilities, ", ")
}

// Seen says when the gateway last heard from it.
func (m MachineRow) Seen() string { return since(m.SeenAt) }

// Enrolled says when it was enrolled.
func (m MachineRow) Enrolled() string { return since(m.CreatedAt) }

// since is ago, as a phrase: "il y a 3 minutes", "à l'instant"; "" for a
// zero time.
func since(t time.Time) string {
	switch s := ago(t); s {
	case "", "à l'instant":
		return s
	default:
		return "il y a " + s
	}
}

// MachineActivatePage is where a user types the code a machine shows, then
// approves what it designates.
type MachineActivatePage struct {
	Me      Person
	Code    string
	Error   string
	Request *MachineRequest
}

// MachineRequest is a machine asking to be enrolled, as its approval shows
// it: enough for its user to tell it is theirs.
type MachineRequest struct {
	ID           string
	Code         string
	Name         string
	OS           string
	Capabilities []string
	ClientAddr   string
	CreatedAt    time.Time
	ExpiresAt    time.Time
}

// CapabilitiesText lists what the machine says it runs.
func (r MachineRequest) CapabilitiesText() string {
	return MachineRow{Capabilities: r.Capabilities}.CapabilitiesText()
}
