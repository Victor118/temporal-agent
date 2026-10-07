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
	// Its model (docs/design/machine-llm.md): how many calls at once (0 =
	// none), the provider and model it calls, its state ("ok", "refused";
	// "" = none), and whether it is set aside since a call was lost.
	MaxLLM      int
	LLMProvider string
	LLMModel    string
	LLMState    string
	AsideForLLM bool
}

// LLMText says whether the machine runs the model of its owner's turns, and
// which.
func (m MachineRow) LLMText() string {
	if m.LLMState == "" || m.MaxLLM == 0 {
		return ""
	}
	model := strings.TrimSpace(m.LLMProvider + " " + m.LLMModel)
	switch {
	case m.LLMState == "refused":
		return "modèle : " + model + ", clé refusée ou crédit épuisé (redémarre agent connect avec une clé valide)"
	case m.AsideForLLM:
		return "modèle : " + model + ", écarté depuis un appel perdu, jusqu'à sa reconnexion"
	}
	return "modèle : " + model + " (" + strconv.Itoa(m.MaxLLM) + " appels à la fois), pour les agents qui le permettent"
}

// openCoding counts its open directives other than the calls to the model.
func (m MachineRow) openCoding() int {
	n := 0
	for _, k := range m.OpenKinds {
		if k != "llm" {
			n++
		}
	}
	return n
}

// LoadText says how busy it is, each family under its own cap.
func (m MachineRow) LoadText() string {
	text := "directives en cours (" + strconv.Itoa(m.openCoding()) + " sur " + strconv.Itoa(m.MaxDirectives) + ")"
	if m.MaxLLM > 0 {
		text += ", appels au modèle (" + strconv.Itoa(len(m.OpenKinds)-m.openCoding()) + " sur " + strconv.Itoa(m.MaxLLM) + ")"
	}
	return text
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
var kindNames = map[string]string{"analyze_repo": "analyse de dépôt", "implement_feature": "implémentation", "echo": "echo", "llm": "appel au modèle"}

// PushText says whether the machine runs implementations, which push their
// branch with its owner's git identity (agent connect --allow-push): only
// said of a machine that has Claude Code.
func (m MachineRow) PushText() string {
	if m.ClaudeCode != "ok" {
		return ""
	}
	for _, c := range m.Capabilities {
		if c == "git-push" {
			return "implémentations : oui, poussées avec ton identité git"
		}
	}
	return "implémentations : non (agent connect --allow-push), elles vont au repli de l'installation"
}

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
