package admin

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/victor/temporal-agent/store"
)

//go:embed templates
var templateFS embed.FS

// pageNames are the templates/<name>.html pages. Each gets its own template
// set (base + partials + the page), so every page can define "content"
// without colliding with the others.
var pageNames = []string{"overview", "agents", "agent", "agent_edit", "tools", "tool", "queues", "skills", "skill", "users", "user_edit", "login"}

type pageData struct {
	Nav   string
	Inv   *Inventory
	Data  any
	Flash string      // one-line confirmation of the last action
	Me    *store.User // the logged-in admin
}

type renderer struct {
	sets map[string]*template.Template
}

func newRenderer() *renderer {
	r := &renderer{sets: make(map[string]*template.Template)}
	for _, name := range pageNames {
		r.sets[name] = template.Must(template.New(name).Funcs(funcs).ParseFS(templateFS,
			"templates/base.html", "templates/partials/*.html", "templates/"+name+".html"))
	}
	return r
}

// page renders a full page.
func (r *renderer) page(w http.ResponseWriter, name string, data pageData) {
	r.execute(w, name, "base", data)
}

// fragment renders one named template of a page's set, for htmx to swap in.
func (r *renderer) fragment(w http.ResponseWriter, page, block string, data pageData) {
	r.execute(w, page, block, data)
}

// execute renders into a buffer first, so a template error yields a 500
// instead of half a page.
func (r *renderer) execute(w http.ResponseWriter, set, block string, data any) {
	var buf bytes.Buffer
	if err := r.sets[set].ExecuteTemplate(&buf, block, data); err != nil {
		log.Printf("admin: render %s/%s: %v", set, block, err)
		http.Error(w, "Erreur de rendu", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	buf.WriteTo(w)
}

var funcs = template.FuncMap{
	"ago":        ago,
	"join":       strings.Join,
	"prettyJSON": prettyJSON,
	"shortHash": func(h string) string {
		if len(h) > 8 {
			return h[:8]
		}
		return h
	},
}

func ago(t time.Time) string {
	if t.IsZero() {
		return "—"
	}
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("il y a %d s", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("il y a %d min", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("il y a %d h", int(d.Hours()))
	default:
		return fmt.Sprintf("il y a %d j", int(d.Hours()/24))
	}
}

func prettyJSON(raw json.RawMessage) string {
	var buf bytes.Buffer
	if err := json.Indent(&buf, raw, "", "  "); err != nil {
		return string(raw)
	}
	return buf.String()
}
