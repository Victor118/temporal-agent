package admin

import (
	"net/http/httptest"
	"strings"
	"testing"
)

// Every page and fragment renders against a full inventory: html/template
// only finds a bad field or call when it executes.
func TestRender_AllPages(t *testing.T) {
	inv := BuildInventory(testInputs())
	r := newRenderer()

	pages := map[string]any{
		"overview": nil,
		"agents":   nil,
		"agent":    inv.Agent("boss"),
		"tools":    nil,
		"tool":     inv.Tool("spawn_session"),
		"queues":   nil,
		"skills":   nil,
		"skill":    inv.Skill("present"),
	}
	for name, data := range pages {
		w := httptest.NewRecorder()
		r.page(w, name, pageData{Nav: name, Inv: inv, Data: data})
		if w.Code != 200 {
			t.Errorf("%s: status %d: %s", name, w.Code, w.Body)
			continue
		}
		if !strings.Contains(w.Body.String(), "</html>") {
			t.Errorf("%s: incomplete page", name)
		}
	}

	for _, f := range []struct{ set, block string }{{"queues", "queue_cards"}, {"agent", "prompt"}} {
		w := httptest.NewRecorder()
		r.fragment(w, f.set, f.block, pageData{Inv: inv, Data: "PROMPT"})
		if w.Code != 200 || strings.Contains(w.Body.String(), "<html") {
			t.Errorf("%s/%s: status %d, body %q", f.set, f.block, w.Code, w.Body)
		}
	}
}
