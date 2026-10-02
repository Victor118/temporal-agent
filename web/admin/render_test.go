package admin

import (
	"github.com/victor/temporal-agent/store"
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
		"overview":  nil,
		"agents":    nil,
		"agent":     inv.Agent("boss"),
		"tools":     nil,
		"tool":      inv.Tool("agent_coder"),
		"queues":    nil,
		"skills":    nil,
		"skill":     inv.Skill("present"),
		"users":     []store.User{{ID: "u1", Email: "a@b.c", Role: "admin"}, {ID: "u2", Email: "d@e.f", Role: "user"}},
		"user_edit": userForm{ID: "u2", Email: "d@e.f", Role: "user", Error: "boom"},
		"agent_edit": agentForm{ID: "boss", Name: "Boss", Picked: []string{"read_file"}, Globs: "agent_*",
			Error: "boom", Picker: buildPicker(inv, agentForm{Picked: []string{"read_file"}, Globs: "agent_*"}),
			Preview: previewData{Agent: inv.Agent("boss")}},
	}
	for name, data := range pages {
		w := httptest.NewRecorder()
		r.page(w, name, pageData{Nav: name, Inv: inv, Data: data, Me: &store.User{ID: "u1", Email: "a@b.c", Role: "admin"}})
		if w.Code != 200 {
			t.Errorf("%s: status %d: %s", name, w.Code, w.Body)
			continue
		}
		if !strings.Contains(w.Body.String(), "</html>") {
			t.Errorf("%s: incomplete page", name)
		}
	}

	preview := httptest.NewRecorder()
	r.execute(preview, "agent_edit", "allowlist_preview", previewData{Agent: inv.Agent("boss"), OOB: true,
		Picker: buildPicker(inv, agentForm{Globs: "agent_*"})})
	if preview.Code != 200 || !strings.Contains(preview.Body.String(), "agent_coder") {
		t.Errorf("allowlist preview: status %d, body %s", preview.Code, preview.Body)
	}

	login := httptest.NewRecorder()
	r.execute(login, "login", "login", pageData{Data: loginData{Email: "a@b.c", Error: "nope"}})
	if login.Code != 200 || !strings.Contains(login.Body.String(), `name="password"`) || !strings.Contains(login.Body.String(), `name="email"`) {
		t.Errorf("login: status %d, body %s", login.Code, login.Body)
	}

	for _, f := range []struct {
		set, block string
		data       any
	}{{"queues", "queue_cards", nil}, {"agent", "prompt", "PROMPT"}} {
		w := httptest.NewRecorder()
		r.fragment(w, f.set, f.block, pageData{Inv: inv, Data: f.data})
		if w.Code != 200 || strings.Contains(w.Body.String(), "<html") {
			t.Errorf("%s/%s: status %d, body %q", f.set, f.block, w.Code, w.Body)
		}
	}
}
