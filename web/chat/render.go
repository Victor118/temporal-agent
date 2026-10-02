package chat

import (
	"bytes"
	"embed"
	"html/template"
	"io/fs"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"time"

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

	Thread  []ThreadItem
	Working bool // the agent is on a turn
	// The fork's starting summary: still being written, or failed.
	SummaryPending bool
	SummaryFailed  bool
	LastMessageID  int64 // what "Forker un fil" forks from

	Members        []Member
	Agent          AgentInfo
	AgentMode      string
	AgentOnMention bool // a plain message does not call the agent
	Parent         *ParentInfo
	IsCreator      bool

	Map *MapView

	Notifications int
	Error         string
	// Fragment: the thread is rendered alone, to be swapped in. The composer
	// then comes with it, out of band, since its state follows the thread's.
	Fragment bool
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
	Description string
}

// ParentInfo is the session a fork started from.
type ParentInfo struct {
	SessionID  string
	Title      string
	Accessible bool // the viewer is a member of the parent
	MessageID  int64
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
