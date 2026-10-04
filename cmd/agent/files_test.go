package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/victor/temporal-agent/store"
)

// withFiles gives s1 a page an agent published, a CSV, and the parent p1
// (Alice's alone) a file of its own.
func withFiles(st *routeStore) {
	st.files = []store.File{
		{ID: "f-html", SessionID: "s1", TurnKey: "m2.default", Name: "rapport final é.html", ContentType: "text/html; charset=utf-8", Size: 23},
		{ID: "f-csv", SessionID: "s1", TurnKey: "m2.default", Name: "data.csv", ContentType: "text/csv; charset=utf-8", Size: 4},
		{ID: "f-parent", SessionID: "p1", TurnKey: "m1.default", Name: "plan.md", ContentType: "text/markdown; charset=utf-8", Size: 2},
	}
	st.contents = map[string][]byte{"f-html": []byte("<script>alert(1)</script>"), "f-csv": []byte("a,b\n"), "f-parent": []byte("# ")}
	st.others = map[string]store.Session{"p1": {SessionID: "p1", CreatedBy: "u-alice"}}
	st.otherMembers = map[string][]string{"p1": {"u-alice"}}
}

func TestFiles_DownloadByMembersOnly(t *testing.T) {
	h, st := newRouteTest(t)
	withFiles(st)
	bob := logIn(t, h, "bob@example.com")

	w := call(t, h, http.MethodGet, "/files/f-html", "", bob)
	if w.Code != http.StatusOK || w.Body.String() != "<script>alert(1)</script>" {
		t.Fatalf("a member's download: %d %s", w.Code, w.Body)
	}
	// Never rendered: an attachment, of a type no browser runs, never
	// sniffed, sandboxed all the same.
	for header, want := range map[string]string{
		"Content-Type":            "application/octet-stream",
		"Content-Disposition":     `attachment; filename="rapport final _.html"; filename*=UTF-8''rapport%20final%20%C3%A9.html`,
		"X-Content-Type-Options":  "nosniff",
		"Content-Security-Policy": "sandbox",
		"Content-Length":          "25",
	} {
		if got := w.Header().Get(header); got != want {
			t.Errorf("%s: %q, want %q", header, got, want)
		}
	}
	// A type no browser runs is served as it is.
	if w := call(t, h, http.MethodGet, "/files/f-csv", "", bob); w.Code != http.StatusOK || w.Header().Get("Content-Type") != "text/csv; charset=utf-8" ||
		!strings.HasPrefix(w.Header().Get("Content-Disposition"), "attachment;") {
		t.Errorf("csv: %d %v", w.Code, w.Header())
	}

	// A non-member, a file of a session they are not a member of (a fork's
	// parent, say), an unknown file: 404 alike.
	carol := logIn(t, h, "carol@example.com")
	for _, c := range []struct {
		who    *http.Cookie
		file   string
		status int
	}{
		{carol, "f-html", http.StatusNotFound},
		{bob, "f-parent", http.StatusNotFound},
		{bob, "f-none", http.StatusNotFound},
		{logIn(t, h, "alice@example.com"), "f-parent", http.StatusOK},
	} {
		if w := call(t, h, http.MethodGet, "/files/"+c.file, "", c.who); w.Code != c.status {
			t.Errorf("%s: %d, want %d", c.file, w.Code, c.status)
		}
	}
	// Without a login, the login page.
	if w := call(t, h, http.MethodGet, "/files/f-html", "", nil); w.Code != http.StatusSeeOther || !strings.HasPrefix(w.Header().Get("Location"), "/login") {
		t.Errorf("without login: %d %s", w.Code, w.Header().Get("Location"))
	}
	// Leaving the session closes its files.
	st.members = []string{"u-alice"}
	if w := call(t, h, http.MethodGet, "/files/f-html", "", bob); w.Code != http.StatusNotFound {
		t.Errorf("after leaving: %d", w.Code)
	}
}

func TestFiles_ListedForMembers(t *testing.T) {
	h, st := newRouteTest(t)
	withFiles(st)
	w := call(t, h, http.MethodGet, "/sessions/s1/files", "", logIn(t, h, "bob@example.com"))
	var files []sessionFile
	json.Unmarshal(w.Body.Bytes(), &files)
	if w.Code != http.StatusOK || len(files) != 2 || files[0].ID != "f-html" || files[0].URL != "/files/f-html" || files[1].Name != "data.csv" {
		t.Errorf("list: %d %s", w.Code, w.Body)
	}
	if w := call(t, h, http.MethodGet, "/sessions/s1/files", "", logIn(t, h, "carol@example.com")); w.Code != http.StatusNotFound {
		t.Errorf("a non-member's list: %d", w.Code)
	}
}

func TestServedType(t *testing.T) {
	for in, want := range map[string]string{
		"text/html; charset=utf-8":     "application/octet-stream",
		"image/svg+xml":                "application/octet-stream",
		"application/xml":              "application/octet-stream",
		"text/xml":                     "application/octet-stream",
		"":                             "application/octet-stream",
		"image/png":                    "image/png",
		"application/pdf":              "application/pdf",
		"text/markdown; charset=utf-8": "text/markdown; charset=utf-8",
		"application/json; foo=bar":    "application/json",
	} {
		if got := servedType(in); got != want {
			t.Errorf("servedType(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestAttachment(t *testing.T) {
	for in, want := range map[string]string{
		"report.pdf":   `attachment; filename="report.pdf"; filename*=UTF-8''report.pdf`,
		`a"b\c%.txt`:   `attachment; filename="a_b_c_.txt"; filename*=UTF-8''a%22b%5Cc%25.txt`,
		"données.csv":  `attachment; filename="donn_es.csv"; filename*=UTF-8''donn%C3%A9es.csv`,
		"a;b=c d.json": `attachment; filename="a;b=c d.json"; filename*=UTF-8''a%3Bb%3Dc%20d.json`,
	} {
		if got := attachment(in); got != want {
			t.Errorf("attachment(%q) = %q, want %q", in, got, want)
		}
	}
}
