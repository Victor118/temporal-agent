package main

import (
	"fmt"
	"log"
	"mime"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/victor/temporal-agent/auth"
	"github.com/victor/temporal-agent/store"
)

// fileDownload serves a file an agent published, to the members of its
// session only: a non-member, as for an unknown file, gets 404 (whether it
// exists is not theirs to learn). A fork's members reach the parent's files
// only if they are members of the parent.
//
// What an agent publishes comes from a model, and may be written to attack
// the reader: an HTML page, an SVG with a script. So a file is never
// rendered by the browser: always an attachment, its type one of a few
// that no browser runs (anything else is application/octet-stream), never
// sniffed, and sandboxed should a browser open it all the same.
func (u *ui) fileDownload(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	f, err := u.store.GetFile(ctx, chi.URLParam(r, "fileID"))
	if err != nil {
		log.Printf("file: %v", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	if f == nil {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}
	ok, err := u.sessions.IsMember(ctx, f.SessionID, auth.UserFrom(ctx).ID)
	if err != nil {
		log.Printf("file: membership check: %v", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	if !ok {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}
	content, err := u.store.ReadFileContent(ctx, f.ID)
	if err != nil {
		log.Printf("file: %v", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	if content == nil {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}
	h := w.Header()
	h.Set("Content-Type", servedType(f.ContentType))
	h.Set("Content-Disposition", attachment(f.Name))
	h.Set("Content-Length", strconv.Itoa(len(content)))
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Content-Security-Policy", "sandbox")
	h.Set("Cache-Control", "private")
	w.Write(content)
}

// servableTypes are the types a file is served as: none of them is run by
// a browser.
var servableTypes = map[string]bool{
	"text/plain": true, "text/csv": true, "text/markdown": true, "text/tab-separated-values": true,
	"application/json": true, "application/yaml": true, "application/pdf": true, "application/zip": true,
	"image/png": true, "image/jpeg": true, "image/gif": true, "image/webp": true,
}

// servedType is the type a file of type t is served as: t when it is one of
// servableTypes, application/octet-stream otherwise (HTML, SVG, XML, and
// whatever a browser could run).
func servedType(t string) string {
	base, params, err := mime.ParseMediaType(t)
	if err != nil || !servableTypes[base] {
		return "application/octet-stream"
	}
	if cs, ok := params["charset"]; ok && strings.HasPrefix(base, "text/") {
		return mime.FormatMediaType(base, map[string]string{"charset": cs})
	}
	return base
}

// attachment is the Content-Disposition of a file named name: its name in
// ASCII for the clients that read no other (filename, RFC 6266), and whole
// in UTF-8 (filename*, RFC 5987).
func attachment(name string) string {
	var ascii, encoded strings.Builder
	for _, r := range name {
		if r < 0x20 || r > 0x7e || r == '"' || r == '\\' || r == '%' {
			ascii.WriteByte('_')
		} else {
			ascii.WriteRune(r)
		}
	}
	for _, b := range []byte(name) {
		if isAttrChar(b) {
			encoded.WriteByte(b)
		} else {
			fmt.Fprintf(&encoded, "%%%02X", b)
		}
	}
	return `attachment; filename="` + ascii.String() + `"; filename*=UTF-8''` + encoded.String()
}

// isAttrChar reports whether b goes as it is in an RFC 5987 value.
func isAttrChar(b byte) bool {
	switch {
	case 'a' <= b && b <= 'z', 'A' <= b && b <= 'Z', '0' <= b && b <= '9':
		return true
	}
	return strings.IndexByte("!#$&+-.^_`|~", b) >= 0
}

// sessionFile is a file in the JSON API: its metadata, and where it is
// downloaded.
type sessionFile struct {
	store.File
	URL string `json:"url"`
}

// listFiles returns the files agents published in the session, oldest
// first.
func (a *api) listFiles(w http.ResponseWriter, r *http.Request) {
	files, err := a.store.ListSessionFiles(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		log.Printf("files: %v", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	out := make([]sessionFile, len(files))
	for i, f := range files {
		out[i] = sessionFile{File: f, URL: "/files/" + f.ID}
	}
	writeJSON(w, http.StatusOK, out)
}
