package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/google/uuid"

	"github.com/victor/temporal-agent/machine"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/tool"
)

// maxFilesPerDirective bounds what one directive publishes, retries
// included (a retry of the same file is no new file): a machine publishes
// machine.MaxOutputFiles at most, the rest is room for a renamed one.
const maxFilesPerDirective = 2 * machine.MaxOutputFiles

func (g *Gateway) maxFileBytes() int64 {
	if g.MaxFileBytes > 0 {
		return g.MaxFileBytes
	}
	return tool.DefaultMaxFileBytes
}

// ServeFiles is PUT /machines/files (machine.FilesPath): a machine publishes
// a file for a directive it runs, attached to the directive's session turn
// and tool call, as a worker's tool would (store.FileStore.SaveFile: the
// same name and content again is the file stored first, another content is
// refused). Its token is checked as for its connection, but a token it no
// longer holds is only refused here, never taken for a copy: an upload may
// race the rotation of its connection, which the WebSocket alone watches.
// The directive must be the machine's and running. The body is read up to
// MaxFileBytes (FILES_MAX_BYTES), one byte more refuses it.
func (g *Gateway) ServeFiles(w http.ResponseWriter, r *http.Request) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || token == "" {
		uploadError(w, http.StatusUnauthorized, "machine token required")
		return
	}
	m, use, err := g.Store.MachineByToken(r.Context(), machine.HashToken(token))
	if err != nil {
		log.Printf("machines: token lookup: %v", err)
		uploadError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if (use != store.TokenCurrent && use != store.TokenPending) || m.RevokedAt != nil {
		uploadError(w, http.StatusUnauthorized, "machine token refused")
		return
	}
	id := r.URL.Query().Get("directive")
	d, err := g.Store.GetDirective(r.Context(), id)
	switch {
	case err != nil:
		log.Printf("machines: directive %s: %v", id, err)
		uploadError(w, http.StatusInternalServerError, "internal error")
		return
	case d == nil || d.MachineID != m.ID:
		// Another machine's is none of this one's business: unknown.
		uploadError(w, http.StatusNotFound, "unknown directive")
		return
	case d.State != store.DirectiveRunning:
		uploadError(w, http.StatusConflict, "the directive is over: nothing is published for it any more")
		return
	case d.SessionID == "" || d.TurnKey == "" || d.CallID == "":
		uploadError(w, http.StatusConflict, "this run belongs to no session turn (a scheduled task, for one): there is no session to publish to")
		return
	}
	name, err := tool.CleanFileName(r.URL.Query().Get("name"))
	if err != nil {
		uploadError(w, http.StatusBadRequest, err.Error())
		return
	}
	max := g.maxFileBytes()
	if r.ContentLength > max {
		uploadError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("too large: at most %s may be published", tool.FormatSize(max)))
		return
	}
	content, err := io.ReadAll(io.LimitReader(r.Body, max+1))
	if err != nil {
		uploadError(w, http.StatusBadRequest, "read the file: "+err.Error())
		return
	}
	if int64(len(content)) > max {
		uploadError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("too large: at most %s may be published", tool.FormatSize(max)))
		return
	}
	published, err := g.Store.ListCallFiles(r.Context(), d.SessionID, d.TurnKey, d.CallID)
	if err != nil {
		log.Printf("machines: files of directive %s: %v", d.ID, err)
		uploadError(w, http.StatusInternalServerError, "internal error")
		return
	}
	sum := sha256.Sum256(content)
	sha := hex.EncodeToString(sum[:])
	if len(published) >= maxFilesPerDirective && !slicesHasFile(published, name, sha) {
		uploadError(w, http.StatusConflict, fmt.Sprintf("this run published %d files already, the most it may", maxFilesPerDirective))
		return
	}
	f, err := g.Store.SaveFile(r.Context(), store.File{
		ID:          uuid.NewString(),
		SessionID:   d.SessionID,
		TurnKey:     d.TurnKey,
		CallID:      d.CallID,
		AgentID:     d.AgentID,
		UserID:      d.UserID,
		Name:        name,
		ContentType: tool.ContentType(name, content),
		Size:        int64(len(content)),
		SHA256:      sha,
	}, content)
	switch {
	case errors.Is(err, store.ErrFileExists):
		uploadError(w, http.StatusConflict, fmt.Sprintf("a file named %s was already published by this run with different content: rename one of them", name))
		return
	case errors.Is(err, store.ErrFileSessionGone):
		uploadError(w, http.StatusGone, "the session was deleted")
		return
	case err != nil:
		log.Printf("machines: save file %q of directive %s: %v", name, d.ID, err)
		uploadError(w, http.StatusInternalServerError, "internal error")
		return
	}
	log.Printf("machines: machine %s published %q (%d bytes) for directive %s", m.ID, f.Name, f.Size, d.ID)
	if g.FilesPublished != nil {
		g.FilesPublished(d.SessionID, d.TurnKey, d.AgentID, []string{f.ID})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(fileRef(f))
}

// slicesHasFile reports a file of that name and content among files: its
// retry, which stores nothing more.
func slicesHasFile(files []store.File, name, sha string) bool {
	for _, f := range files {
		if f.Name == name && f.SHA256 == sha {
			return true
		}
	}
	return false
}

func fileRef(f store.File) machine.FileRef {
	return machine.FileRef{ID: f.ID, Name: f.Name, ContentType: f.ContentType, Size: f.Size, SHA256: f.SHA256}
}

func uploadError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(machine.UploadError{Error: msg})
}

// callFiles are the files published for d's turn and call: what its result
// lists, read from the database rather than from the machine's word. A
// failure to read them loses the list, not the result.
func (g *Gateway) callFiles(ctx context.Context, d store.Directive) []machine.FileRef {
	if d.SessionID == "" || d.TurnKey == "" || d.CallID == "" {
		return nil
	}
	files, err := g.Store.ListCallFiles(ctx, d.SessionID, d.TurnKey, d.CallID)
	if err != nil {
		log.Printf("machines: files of directive %s: %v", d.ID, err)
		return nil
	}
	var refs []machine.FileRef
	for _, f := range files {
		refs = append(refs, fileRef(f))
	}
	return refs
}
