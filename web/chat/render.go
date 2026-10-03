package chat

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"log"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/victor/temporal-agent/session"
	"github.com/victor/temporal-agent/sse"
	"github.com/victor/temporal-agent/store"
)

//go:embed templates
var templateFS embed.FS

//go:embed static
var staticFS embed.FS

// Static serves the interface's stylesheet, scripts and fonts. The file names
// carry their versions, the stylesheet aside, so a short cache is enough.
func Static() http.Handler {
	sub, _ := fs.Sub(staticFS, "static")
	files := http.FileServer(http.FS(sub))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "private, max-age=300")
		files.ServeHTTP(w, r)
	})
}

// Page is everything the interface shows for one session.
type Page struct {
	Me      Person
	IsAdmin bool
	Roots   []*TreeNode
	Node    *TreeNode   // the current session; nil on the welcome page
	Crumbs  []*TreeNode // root down to Node
	View    string      // "thread" or "map"
	// StreamFrom is the ID of the last event of the page's stream (the
	// session's and the user's tree's) the page has seen: it connects from
	// there.
	StreamFrom string

	Thread  []ThreadItem
	Working bool // the agent is on a turn
	// WorkingAgent names the agent on the turn; empty when the server has
	// only the visibility queries to tell (after a restart).
	WorkingAgent string
	// The fork's starting summary: still being written, or failed.
	SummaryPending bool
	SummaryFailed  bool
	LastMessageID  int64 // what "Forker un fil" forks from

	Members        []Member
	Agent          AgentInfo
	Agents         []AgentInfo // the agents members can call by their mentions
	AgentMode      string
	AgentOnMention bool // a plain message does not call the agent
	Parent         *ParentInfo
	IsCreator      bool
	// Report is the fork's report to its parent; nil for a session that is
	// not a fork.
	Report *ReportView

	Map *MapView

	Notifications int
	Error         string
	// Fragment: the thread is rendered alone, to be swapped in. The composer
	// then comes with it, out of band, since its state follows the thread's.
	Fragment bool
	// Versions of the fragments the page reloads, by name: each carries its
	// own in a data-version attribute (see RenderFragment).
	Versions map[string]string
	// Rendered are those fragments as RenderPage rendered them to learn
	// their versions: the page shows these bytes rather than render them
	// again. Without one, the page renders the fragment.
	Rendered map[string]template.HTML
}

// SeveralAgents reports whether members can call more than one agent: the
// interface then tells how.
func (p Page) SeveralAgents() bool { return len(p.Agents) > 1 }

// ReportConfirm is what the report button asks before it posts: where the
// report goes, to how many readers, under whose name. The fork's members may
// not be the parent's. During an agent's turn, it says first that the report
// stops where the turn stands.
func (p Page) ReportConfirm() string {
	where := "la session parente"
	if p.Parent != nil && p.Parent.Accessible {
		title := p.Parent.Title
		if title == "" {
			title = "Session sans titre"
		}
		where = "« " + title + " » (" + pluralize(p.Parent.Members, "membre") + ")"
	}
	confirm := "Poster dans " + where + " un résumé de ce fork, signé de ton nom ?"
	if p.Report != nil && p.Report.AgentWorking {
		confirm = reportPartial + " " + confirm
	}
	return confirm
}

// Member is a session member, for the rail.
type Member struct {
	Person
	Email string
}

// AgentInfo is the session's agent.
type AgentInfo struct {
	ID          string
	Name        string
	Mention     string // what calls it: @Mention
	Description string
}

// ParentInfo is the session a fork started from.
type ParentInfo struct {
	SessionID  string
	Title      string
	Accessible bool // the viewer is a member of the parent
	Members    int  // how many members it has, when Accessible
	MessageID  int64
}

// ReportView is where a fork stands with its reports, for the viewer.
type ReportView struct {
	session.ReportState
	Error string // why the last click started no report
}

// Reason is why the viewer cannot report now, in words; empty when they can.
func (r ReportView) Reason() string {
	switch {
	case errors.Is(r.Refused, session.ErrNoParent):
		return "La session parente a été supprimée : ce fork n'a plus à qui rapporter."
	case errors.Is(r.Refused, session.ErrNotParentMember):
		return "Seul un membre de la session parente peut y rapporter, et tu n'en es pas membre."
	case errors.Is(r.Refused, session.ErrNotAFork):
		return "Cette session n'est pas un fork : elle n'a pas de session parente à qui rapporter."
	case r.Refused != nil:
		return "Rapport impossible."
	case r.Pending:
		return "Le rapport s'écrit. Il arrivera dans la session parente comme ton message, sans solliciter d'agent."
	case r.SummaryPending:
		return "Le brief du fork est en cours d'écriture."
	case r.NothingNew && r.LastReportedAt != nil:
		return "Rien de nouveau depuis le dernier rapport."
	case r.NothingNew:
		return "Rien à rapporter pour l'instant."
	}
	return ""
}

// Partial says that a report sent now covers only part of the agent's turn,
// in words; empty when the agent is not on one, or no report can be sent.
func (r ReportView) Partial() string {
	if !r.AgentWorking || !r.CanReport() {
		return ""
	}
	return reportPartial
}

// reportPartial is what a report sent during an agent's turn covers.
const reportPartial = "L'agent est en plein tour (il travaille ou attend une réponse) : le rapport couvrira ce qui est écrit jusqu'ici, la suite ira dans le rapport suivant."

// LastReported is when the latest report was posted, as the thread shows
// times; empty for none.
func (r ReportView) LastReported() string {
	if r.LastReportedAt == nil {
		return ""
	}
	return clock(*r.LastReportedAt)
}

// ItemView is a thread item with the page it is shown on: its actions need
// the session.
type ItemView struct {
	Page *Page
	Item ThreadItem
}

// LoginPage is the login form.
type LoginPage struct {
	Email string
	Next  string
	Error string
}

// NotificationsPage lists the results of scheduled tasks.
type NotificationsPage struct {
	Me    Person
	Items []Notification
}

// Notification is one delivered result.
type Notification struct {
	ID   int64
	HTML template.HTML
}

var tmpl = template.Must(template.New("").Funcs(template.FuncMap{
	"clock":       clock,
	"ago":         ago,
	"withMention": withMention,
	"statusLabel": statusLabel,
	"kindLabel":   kindLabel,
	"plural":      func(n int, word string) string { return pluralize(n, word) },
	"add":         func(a, b int) int { return a + b },
	"itemOf":      func(p *Page, it ThreadItem) ItemView { return ItemView{Page: p, Item: it} },
	"purposeMax":  func() int { return session.MaxPurposeRunes },
	"reloadOn":    reloadOn,
	"sessionGone": func() string { return session.EventSessionGone },
	"memberLeft":  func() string { return session.EventMemberLeft },
}).ParseFS(templateFS, "templates/*.html"))

// Render writes the named template (a page or a fragment) for data. It
// renders into a buffer first, so a template error is a 500, not half a page.
func Render(w http.ResponseWriter, name string, data any) {
	var buf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&buf, name, data); err != nil {
		log.Printf("chat: render %s: %v", name, err)
		http.Error(w, "Erreur de rendu", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	buf.WriteTo(w)
}

// VersionHeader is the header a fragment's reload sends with the version the
// page holds (page.html sets it from the fragment's data-version).
const VersionHeader = "X-Fragment-Version"

// versionPlaceholder stands for a fragment's version while it is rendered to
// be hashed: the version cannot be part of what it hashes. It is drawn at
// random by each process, so that no member can write it in a message.
var versionPlaceholder = func() string {
	b := make([]byte, 16)
	rand.Read(b)
	return "v" + hex.EncodeToString(b)
}()

// fragmentParts are the templates a fragment is made of, in order, when it
// is more than its own: the thread's reload brings the composer along, out
// of band, since its state follows the thread's.
var fragmentParts = map[string][]string{"thread": {"thread-inner", "composer"}}

// renderVersioned renders the fragment name, part by part, with its version,
// a hash of what it shows: the same content always has the same version,
// whatever data it came from.
func renderVersioned(name string, p *Page) ([][]byte, string, error) {
	if p.Versions == nil {
		p.Versions = map[string]string{}
	}
	p.Versions[name] = versionPlaceholder
	parts, ok := fragmentParts[name]
	if !ok {
		parts = []string{name}
	}
	out := make([][]byte, len(parts))
	hash := sha256.New()
	for i, part := range parts {
		var buf bytes.Buffer
		if err := tmpl.ExecuteTemplate(&buf, part, p); err != nil {
			return nil, "", err
		}
		out[i] = buf.Bytes()
		hash.Write(out[i])
	}
	version := hex.EncodeToString(hash.Sum(nil)[:12])
	p.Versions[name] = version
	for i := range out {
		out[i] = bytes.ReplaceAll(out[i], []byte(versionPlaceholder), []byte(version))
	}
	return out, version, nil
}

// RenderFragment writes a fragment a page reloads. When the page holds its
// version already (have, from VersionHeader), it answers 204 No Content:
// htmx swaps nothing, and nothing on the page moves.
func RenderFragment(w http.ResponseWriter, name string, p *Page, have string) {
	parts, version, err := renderVersioned(name, p)
	if err != nil {
		log.Printf("chat: render %s: %v", name, err)
		http.Error(w, "Erreur de rendu", http.StatusInternalServerError)
		return
	}
	if have != "" && have == version {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	for _, part := range parts {
		w.Write(part)
	}
}

// reloaded are the fragments a session's page reloads on its own: the
// thread, the tree, a fork's report section.
var reloaded = []string{"thread", "tree-items", "report"}

// RenderPage writes a whole page, its fragments carrying the versions their
// reloads would get: the first reload of an unchanged one swaps nothing.
// Each fragment is rendered once, as its reload renders it; the page shows
// its first part (the thread without the composer, which the page has in
// its place).
func RenderPage(w http.ResponseWriter, name string, p *Page) {
	fragment := p.Fragment
	p.Rendered = map[string]template.HTML{}
	for _, f := range reloaded {
		if f == "thread" && (p.Node == nil || p.View == "map") || f == "report" && p.Report == nil {
			continue
		}
		p.Fragment = f == "thread" // as its reload renders it, the composer along
		parts, _, err := renderVersioned(f, p)
		if err != nil {
			log.Printf("chat: render %s: %v", f, err)
			http.Error(w, "Erreur de rendu", http.StatusInternalServerError)
			return
		}
		p.Rendered[f] = template.HTML(parts[0]) // our own template's output, escaped
	}
	p.Fragment = fragment
	Render(w, name, p)
}

// reloadOn is the hx-trigger of a pane the page's stream reloads: on the
// events that change it, on a reload of the stream (it lost events), and
// once a minute, in case (unchanged, that is a 204).
func reloadOn(pane string) (string, error) {
	var events []string
	switch pane {
	case "thread":
		events = session.ThreadEvents
	case "report":
		events = session.ReportEvents
	case "tree":
		events = []string{session.EventTreeChanged}
	default:
		return "", fmt.Errorf("no events for pane %q", pane)
	}
	var b strings.Builder
	for _, e := range append(slices.Clone(events), sse.EventReload) {
		b.WriteString("sse:" + e + ", ")
	}
	b.WriteString("every 60s")
	return b.String(), nil
}

func clock(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	now := time.Now()
	if y, m, d := t.Date(); y == now.Year() && m == now.Month() && d == now.Day() {
		return t.Local().Format("15:04")
	}
	return t.Local().Format("02/01 15:04")
}

func ago(t time.Time) string {
	d := time.Since(t)
	switch {
	case t.IsZero():
		return ""
	case d < time.Minute:
		return "à l'instant"
	case d < time.Hour:
		return pluralize(int(d.Minutes()), "minute")
	case d < 48*time.Hour:
		return pluralize(int(d.Hours()), "heure")
	default:
		return pluralize(int(d.Hours()/24), "jour")
	}
}

func pluralize(n int, word string) string {
	s := strconv.Itoa(n) + " " + word
	if n > 1 {
		s += "s"
	}
	return s
}

var mentionRe = regexp.MustCompile(`(^|[^\w@.])(@[\w-]+)`)

// withMention escapes a member's message and highlights its @mentions.
func withMention(text string) template.HTML {
	escaped := template.HTMLEscapeString(text)
	return template.HTML(mentionRe.ReplaceAllString(escaped, `$1<span class="mention">$2</span>`))
}

func statusLabel(s Status) string {
	switch s {
	case StatusActive:
		return "active"
	case StatusWorking:
		return "l'agent travaille"
	case StatusWaiting:
		return "attend une réponse"
	}
	return "en veille"
}

// kindLabel names a node by its place in the tree.
func kindLabel(n *TreeNode) string {
	switch {
	case n.Depth == 0 && n.Orphan:
		return "Fork"
	case n.Depth == 0:
		return "Racine"
	case len(n.Children) == 0:
		return "Feuille"
	}
	return "Branche"
}

// UserPerson is how a user appears in the interface.
func UserPerson(u *store.User) Person { return NewPerson(u.ID, u.Name()) }
