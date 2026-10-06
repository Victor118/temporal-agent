package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/victor/temporal-agent/machine"
	"github.com/victor/temporal-agent/store"
)

// filesStore holds machines by token, directives, and files as
// store.FileStore.SaveFile keeps them: one per (session, turn, call, name).
type filesStore struct {
	Store
	mu         sync.Mutex
	tokens     map[string]store.TokenUse // by token hash, all of machine m-1
	directives map[string]store.Directive
	files      []store.File
	revoked    bool
}

func (f *filesStore) MachineByToken(_ context.Context, hash string) (*store.Machine, store.TokenUse, error) {
	use, ok := f.tokens[hash]
	if !ok {
		return nil, store.TokenUnknown, nil
	}
	return &store.Machine{ID: "m-1", UserID: "u-1", Name: "maison"}, use, nil
}

func (f *filesStore) RevokeMachine(context.Context, string, string) ([]store.Directive, error) {
	f.revoked = true
	return nil, nil
}

func (f *filesStore) GetDirective(_ context.Context, id string) (*store.Directive, error) {
	d, ok := f.directives[id]
	if !ok {
		return nil, nil
	}
	return &d, nil
}

func (f *filesStore) ListCallFiles(_ context.Context, session, turn, call string) ([]store.File, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []store.File
	for _, fl := range f.files {
		if fl.SessionID == session && fl.TurnKey == turn && fl.CallID == call {
			out = append(out, fl)
		}
	}
	return out, nil
}

func (f *filesStore) SaveFile(_ context.Context, fl store.File, _ []byte) (store.File, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, old := range f.files {
		if old.SessionID == fl.SessionID && old.TurnKey == fl.TurnKey && old.CallID == fl.CallID && old.Name == fl.Name {
			if old.SHA256 != fl.SHA256 {
				return store.File{}, store.ErrFileExists
			}
			return old, nil
		}
	}
	f.files = append(f.files, fl)
	return fl, nil
}

func TestServeFiles(t *testing.T) {
	st := &filesStore{
		tokens: map[string]store.TokenUse{machine.HashToken("agm_now"): store.TokenCurrent, machine.HashToken("agm_old"): store.TokenRetired},
		directives: map[string]store.Directive{
			"d-run":   {ID: "d-run", MachineID: "m-1", UserID: "u-1", State: store.DirectiveRunning, SessionID: "s-1", TurnKey: "m7.jarvis", CallID: "call-1", AgentID: "jarvis"},
			"d-other": {ID: "d-other", MachineID: "m-2", State: store.DirectiveRunning, SessionID: "s-1", TurnKey: "m7.jarvis", CallID: "call-2"},
			"d-over":  {ID: "d-over", MachineID: "m-1", State: store.DirectiveCompleted, SessionID: "s-1", TurnKey: "m7.jarvis", CallID: "call-3"},
			"d-none":  {ID: "d-none", MachineID: "m-1", State: store.DirectiveRunning},
		}}
	var told []string
	g := &Gateway{Store: st, MaxFileBytes: 16, FilesPublished: func(session, turn, agent string, ids []string) {
		told = append(told, session+"/"+turn+"/"+agent+"/"+strings.Join(ids, ","))
	}}
	srv := httptest.NewServer(http.HandlerFunc(g.ServeFiles))
	defer srv.Close()
	put := func(token, directive, name string, body io.Reader) (int, machine.FileRef, string) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPut, srv.URL+"?directive="+url.QueryEscape(directive)+"&name="+url.QueryEscape(name), body)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		var ref machine.FileRef
		var refusal machine.UploadError
		json.Unmarshal(raw, &ref)
		json.Unmarshal(raw, &refusal)
		return resp.StatusCode, ref, refusal.Error
	}

	code, first, _ := put("agm_now", "d-run", "notes.md", strings.NewReader("# Notes"))
	if code != http.StatusOK || first.ID == "" || first.Size != 7 || first.ContentType != "text/markdown; charset=utf-8" || len(first.SHA256) != 64 {
		t.Fatalf("published: %d %+v", code, first)
	}
	if f := st.files[0]; f.SessionID != "s-1" || f.TurnKey != "m7.jarvis" || f.CallID != "call-1" || f.AgentID != "jarvis" || f.UserID != "u-1" {
		t.Errorf("stored %+v", f)
	}
	if len(told) != 1 || told[0] != "s-1/m7.jarvis/jarvis/"+first.ID {
		t.Errorf("told %v", told)
	}
	// The same file again (a retry): the one stored first.
	if code, again, _ := put("agm_now", "d-run", "notes.md", strings.NewReader("# Notes")); code != http.StatusOK || again.ID != first.ID || len(st.files) != 1 {
		t.Errorf("retry: %d %+v", code, again)
	}
	// Another content under that name: refused, in words for the model.
	if code, _, msg := put("agm_now", "d-run", "notes.md", strings.NewReader("# Other")); code != http.StatusConflict || !strings.Contains(msg, "different content") {
		t.Errorf("other content: %d %q", code, msg)
	}
	for _, c := range []struct {
		name, token, directive, file string
		body                         io.Reader
		want                         int
		says                         string
	}{
		{"another machine's directive", "agm_now", "d-other", "a.txt", strings.NewReader("x"), http.StatusNotFound, "unknown directive"},
		{"unknown directive", "agm_now", "d-nope", "a.txt", strings.NewReader("x"), http.StatusNotFound, "unknown directive"},
		{"directive over", "agm_now", "d-over", "a.txt", strings.NewReader("x"), http.StatusConflict, "over"},
		{"no session turn", "agm_now", "d-none", "a.txt", strings.NewReader("x"), http.StatusConflict, "no session turn"},
		{"a path", "agm_now", "d-run", "../a.txt", strings.NewReader("x"), http.StatusBadRequest, "path"},
		{"too large", "agm_now", "d-run", "big.bin", strings.NewReader(strings.Repeat("x", 17)), http.StatusRequestEntityTooLarge, "at most 16 B"},
		// No length said (chunked): read up to the limit, refused past it.
		{"too large, unsaid", "agm_now", "d-run", "big.bin", io.MultiReader(strings.NewReader(strings.Repeat("x", 17))), http.StatusRequestEntityTooLarge, "at most"},
		{"replaced token", "agm_old", "d-run", "a.txt", strings.NewReader("x"), http.StatusUnauthorized, "refused"},
		{"unknown token", "agm_who", "d-run", "a.txt", strings.NewReader("x"), http.StatusUnauthorized, "refused"},
	} {
		if code, _, msg := put(c.token, c.directive, c.file, c.body); code != c.want || !strings.Contains(msg, c.says) {
			t.Errorf("%s: %d %q", c.name, code, msg)
		}
	}
	if len(st.files) != 1 || st.revoked {
		t.Errorf("stored %d files, revoked %v", len(st.files), st.revoked)
	}

	// The most a directive may publish: then only its retries.
	for i := len(st.files); i < maxFilesPerDirective; i++ {
		st.files = append(st.files, store.File{ID: "f", SessionID: "s-1", TurnKey: "m7.jarvis", CallID: "call-1", Name: "f" + string(rune('a'+i))})
	}
	if code, _, msg := put("agm_now", "d-run", "one-more.txt", strings.NewReader("x")); code != http.StatusConflict || !strings.Contains(msg, "the most") {
		t.Errorf("past the most: %d %q", code, msg)
	}
	if code, again, _ := put("agm_now", "d-run", "notes.md", strings.NewReader("# Notes")); code != http.StatusOK || again.ID != first.ID {
		t.Errorf("a retry past the most: %d", code)
	}
}

// A result lists the files the database holds for its directive's call,
// whatever the machine says.
func TestCallFiles(t *testing.T) {
	st := &filesStore{files: []store.File{
		{ID: "f-1", SessionID: "s-1", TurnKey: "m7.jarvis", CallID: "call-1", Name: "a.md"},
		{ID: "f-2", SessionID: "s-1", TurnKey: "m7.jarvis", CallID: "call-2", Name: "b.md"},
	}}
	g := &Gateway{Store: st}
	refs := g.callFiles(context.Background(), store.Directive{ID: "d", SessionID: "s-1", TurnKey: "m7.jarvis", CallID: "call-1"})
	if len(refs) != 1 || refs[0].ID != "f-1" {
		t.Errorf("files %+v", refs)
	}
	if refs := g.callFiles(context.Background(), store.Directive{ID: "d"}); refs != nil {
		t.Errorf("no turn: %+v", refs)
	}
	res, _ := completion(machine.Message{Status: machine.StatusError, Error: "boom"}, refs)
	if len(res.Files) != 1 {
		t.Errorf("result %+v", res)
	}
}
