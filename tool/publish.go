package tool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/victor/temporal-agent/store"
)

// DefaultMaxFileBytes bounds a file exec publishes when the worker sets no
// FILES_MAX_BYTES.
const DefaultMaxFileBytes = 20 << 20

// MaxTextFileBytes bounds a file publish_file publishes: its content is the
// model's input, which goes through Temporal and stays in the session's
// history, read again by every LLM call. A large file is exec's to publish.
const MaxTextFileBytes = 1 << 20

// publishBudget bounds the time exec takes to publish its files once the
// command is over; its Timeout makes room for it. What it leaves
// unpublished is said, never failed: a failed activity would hide the files
// already stored.
const publishBudget = 60 * time.Second

// maxFileNameBytes bounds a published file's name, as most file systems do.
const maxFileNameBytes = 255

// maxPublishPaths bounds the files one exec call publishes.
const maxPublishPaths = 20

// TurnRef is the session turn a run works for: what it publishes is
// attached there.
type TurnRef struct {
	SessionID string `json:"session_id"`
	TurnKey   string `json:"turn_key"`
}

// FileRef is a published file as the workflow sees it: never its content.
type FileRef struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	ContentType string `json:"content_type"`
	Size        int64  `json:"size"`
	SHA256      string `json:"sha256"`
}

// FileSaver stores a published file (store.FileStore).
type FileSaver interface {
	SaveFile(ctx context.Context, f store.File, content []byte) (store.File, error)
}

// Publisher publishes files for the tools of a worker: publish_file, and
// exec's publish. The content goes from the tool to the store directly;
// the call's result, and so Temporal, only carries its reference.
type Publisher struct {
	Store FileSaver
	// MaxBytes is the largest file exec publishes (FILES_MAX_BYTES);
	// DefaultMaxFileBytes when not positive. publish_file has its own
	// bound, MaxTextFileBytes.
	MaxBytes int64
	// budget replaces publishBudget, shorter, in tests.
	budget time.Duration
}

func (p *Publisher) maxBytes() int64 {
	if p == nil || p.MaxBytes <= 0 {
		return DefaultMaxFileBytes
	}
	return p.MaxBytes
}

// Published collects the files a tool call publishes, for the activity to
// return their references (ExecuteToolOutput.Files).
type Published struct {
	mu    sync.Mutex
	files []FileRef
}

type publishedKey struct{}

// WithPublished gives a tool call a collector of the files it publishes.
func WithPublished(ctx context.Context) (context.Context, *Published) {
	p := &Published{}
	return context.WithValue(ctx, publishedKey{}, p), p
}

// Files are the files published so far, in order.
func (p *Published) Files() []FileRef {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]FileRef(nil), p.files...)
}

func recordPublished(ctx context.Context, f FileRef) {
	if p, ok := ctx.Value(publishedKey{}).(*Published); ok {
		p.mu.Lock()
		p.files = append(p.files, f)
		p.mu.Unlock()
	}
}

// errNoTurn refuses a file from a run that belongs to no session turn: a
// scheduled task, for one. Its members could not see it anywhere.
var errNoTurn = errors.New("Cannot publish: this run belongs to no session turn (a scheduled task, for instance), so there is no session to attach a file to. Put the content in your answer instead.")

// nameTaken refuses a second file of one call under one name, with other
// content: it would replace the first in the members' eyes.
func nameTaken(name string) error {
	return fmt.Errorf("a file named %s was already published by this call with different content: rename one of them", name)
}

// errNoCall refuses a file from a call given no call context: the workflow
// read a catalog that did not yet say the tool needs one. The next call
// reads it again.
var errNoCall = errors.New("Cannot publish: the worker could not tell which turn to attach the file to. Nothing was published; try again.")

// publish stores content as a file named name, attached to the session turn
// the call works for (CallContext.Turn), and returns it. A file the same
// call already published under that name is returned as it was stored
// (store.FileStore.SaveFile).
func (p *Publisher) publish(ctx context.Context, name string, content []byte, max int64) (store.File, error) {
	call, err := p.call(ctx)
	if err != nil {
		return store.File{}, err
	}
	name, err = CleanFileName(name)
	if err != nil {
		return store.File{}, err
	}
	if int64(len(content)) > max {
		return store.File{}, fmt.Errorf("%s is too large: at most %s may be published", name, FormatSize(max))
	}
	sum := sha256.Sum256(content)
	f, err := p.Store.SaveFile(ctx, store.File{
		ID:          uuid.NewString(),
		SessionID:   call.Turn.SessionID,
		TurnKey:     call.Turn.TurnKey,
		CallID:      call.CallID,
		AgentID:     AgentIDFromContext(ctx),
		UserID:      UserIDFromContext(ctx),
		Name:        name,
		ContentType: contentType(name, content),
		Size:        int64(len(content)),
		SHA256:      hex.EncodeToString(sum[:]),
	}, content)
	if errors.Is(err, store.ErrFileSessionGone) {
		return store.File{}, errors.New("Cannot publish: the session was deleted")
	}
	if errors.Is(err, store.ErrFileExists) {
		return store.File{}, nameTaken(name)
	}
	if err != nil {
		return store.File{}, fmt.Errorf("publish %s: %w", name, err)
	}
	recordPublished(ctx, FileRef{ID: f.ID, Name: f.Name, ContentType: f.ContentType, Size: f.Size, SHA256: f.SHA256})
	return f, nil
}

// call is the context of a call that may publish: one of a session turn
// (call.Turn), with its ID. Any other is refused, before it makes anything
// to publish.
func (p *Publisher) call(ctx context.Context) (CallContext, error) {
	if p == nil || p.Store == nil {
		return CallContext{}, errors.New("Cannot publish: this worker has no file store")
	}
	call, ok := CallFromContext(ctx)
	if !ok {
		return CallContext{}, errNoCall
	}
	if call.Turn == nil || call.Turn.SessionID == "" || call.Turn.TurnKey == "" {
		return CallContext{}, errNoTurn
	}
	if call.CallID == "" {
		return CallContext{}, errNoCall // the file could not be told from a retry's
	}
	return call, nil
}

// published is how the model reads a file it published.
func published(f store.File) string {
	return fmt.Sprintf("%s (%s, id %s)", f.Name, FormatSize(f.Size), f.ID)
}

// CleanFileName is name as a published file takes it: trimmed, a name and
// not a path (no separator, not "." nor ".."), with no control or format
// character (a right-to-left override would show "exe.pdf" for "fdp.exe"),
// valid UTF-8, at most maxFileNameBytes. Anything else is refused, not
// mended: the model chose it and can choose another.
func CleanFileName(name string) (string, error) {
	name = strings.TrimSpace(name)
	switch {
	case name == "":
		return "", errors.New("the file needs a name")
	case name == "." || name == "..":
		return "", fmt.Errorf("%q is not a file name", name)
	case strings.ContainsAny(name, `/\`):
		return "", fmt.Errorf("%q is a path: give a file name, with no / or \\", name)
	case !utf8.ValidString(name):
		return "", errors.New("the file name is not valid UTF-8")
	case len(name) > maxFileNameBytes:
		return "", fmt.Errorf("the file name is too long: at most %d bytes", maxFileNameBytes)
	}
	for _, r := range name {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return "", fmt.Errorf("the file name %q holds a control or invisible character", name)
		}
	}
	return name, nil
}

// fileTypes are the types of the files an agent publishes most, which the
// system's table may lack.
var fileTypes = map[string]string{
	".md":       "text/markdown; charset=utf-8",
	".markdown": "text/markdown; charset=utf-8",
	".csv":      "text/csv; charset=utf-8",
	".tsv":      "text/tab-separated-values; charset=utf-8",
	".txt":      "text/plain; charset=utf-8",
	".log":      "text/plain; charset=utf-8",
	".json":     "application/json",
	".yaml":     "application/yaml",
	".yml":      "application/yaml",
	".svg":      "image/svg+xml",
	".pdf":      "application/pdf",
	".png":      "image/png",
	".jpg":      "image/jpeg",
	".jpeg":     "image/jpeg",
	".gif":      "image/gif",
	".webp":     "image/webp",
	".zip":      "application/zip",
	".html":     "text/html; charset=utf-8",
	".xml":      "application/xml",
}

// contentType is what a file says it is: from its name's extension, else
// from its first bytes. Informative: how a file is served does not trust it.
func contentType(name string, content []byte) string {
	ext := strings.ToLower(filepath.Ext(name))
	if t, ok := fileTypes[ext]; ok {
		return t
	}
	if t := mime.TypeByExtension(ext); t != "" {
		return t
	}
	return http.DetectContentType(content)
}

// FormatSize is a size as the model reads it: 820 B, 12.4 KB, 3.1 MB.
func FormatSize(n int64) string {
	switch {
	case n < 1024:
		return fmt.Sprintf("%d B", n)
	case n < 1024*1024:
		return fmt.Sprintf("%.1f KB", float64(n)/1024)
	}
	return fmt.Sprintf("%.1f MB", float64(n)/(1024*1024))
}

// RegisterPublishFileTool registers publish_file: a text the model writes
// (Markdown, CSV, SVG, JSON…) becomes a file the session's members download,
// attached to the turn. It reads no disk: any worker with the store runs it.
func RegisterPublishFileTool(r *Registry, p *Publisher) {
	r.Register(&Tool{
		Name: "publish_file",
		Description: `Publish a file to the session: its members see it attached to your answer and can download it. Use it for a short text you write that the user should keep as a file — a report in Markdown, a table in CSV, a diagram in SVG, data in JSON — rather than pasting it into your answer. What you write here stays in the conversation, as any tool input: for anything large, write it with exec and publish it with exec's publish parameter.
Mention the file by its name in your answer; do not repeat its content.`,
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"name": {"type": "string", "description": "File name with its extension, e.g. report.md. A name, not a path."},
				"content": {"type": "string", "description": "The file's content, as text (at most ` + FormatSize(MaxTextFileBytes) + `)."}
			},
			"required": ["name", "content"]
		}`),
		Kind: ToolKindActivity,
		// The session turn the file is attached to (CallContext.Turn).
		NeedsCallContext: true,
		Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
			var params struct {
				Name    string `json:"name"`
				Content string `json:"content"`
			}
			if err := json.Unmarshal(input, &params); err != nil {
				return "", fmt.Errorf("parse input: %w", err)
			}
			f, err := p.publish(ctx, params.Name, []byte(params.Content), MaxTextFileBytes)
			if err != nil {
				return "", err
			}
			return "Published " + published(f) + ". The session's members can download it from your answer.", nil
		},
	})
}

// publishFromWorkspace publishes the files at paths in the workspace, after
// a command: each one read through root, so that a link leading out of the
// workspace is refused, and only a regular file is read (a FIFO would block).
// It returns what to put before the command's output: what was published,
// and why the others were not. Before, so that a long output clipped from
// its end (a fork's summary reads the head of a result) keeps the files'
// names. A failure is no error: the command ran, and its output stands.
//
// Publishing takes at most publishBudget: what is left once it runs out is
// said not published, the files already stored stay so, and the activity
// ends within the tool's Timeout.
func (p *Publisher) publishFromWorkspace(ctx context.Context, ws workspace, paths []string) string {
	if len(paths) == 0 {
		return ""
	}
	budget := publishBudget
	if p != nil && p.budget > 0 {
		budget = p.budget
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	outOfTime := fmt.Errorf("not published: the %s given to publish ran out", budget)
	var done, failed []string
	fail := func(path string, err error) { failed = append(failed, fmt.Sprintf("- %s: %v", path, err)) }
	if len(paths) > maxPublishPaths {
		for _, path := range paths[maxPublishPaths:] {
			fail(path, fmt.Errorf("at most %d files per command", maxPublishPaths))
		}
		paths = paths[:maxPublishPaths]
	}
	root, err := ws.open()
	if err != nil {
		for _, path := range paths {
			fail(path, err)
		}
		return publishReport(done, failed)
	}
	defer root.Close()
	// A file is published under its base name: out/a.csv and tmp/a.csv
	// would be one name. The first goes, the others are refused unread.
	names := map[string]bool{}
	for _, path := range paths {
		if ctx.Err() != nil {
			fail(path, outOfTime)
			continue
		}
		name := filepath.Base(filepath.Clean(path))
		if names[name] {
			fail(path, nameTaken(name))
			continue
		}
		names[name] = true
		content, err := p.readPublished(ctx, root, path)
		if err != nil && ctx.Err() != nil {
			err = outOfTime
		}
		if err != nil {
			fail(path, err)
			continue
		}
		f, err := p.publish(ctx, name, content, p.maxBytes())
		if err != nil && ctx.Err() != nil {
			// Cut short: the store wrote nothing of it (one transaction).
			err = outOfTime
		}
		if err != nil {
			fail(path, err)
			continue
		}
		done = append(done, "- "+published(f))
	}
	return publishReport(done, failed)
}

// readPublished reads the file at path, relative to the workspace, up to
// one byte past the largest size: enough to refuse a larger one without
// reading it whole. An absolute path is refused, not taken relative to the
// workspace as the file tools do: a command's /tmp/out.csv is not the
// workspace's tmp/out.csv. The read stops when ctx ends: the budget of
// publishing covers it.
func (p *Publisher) readPublished(ctx context.Context, root *os.Root, path string) ([]byte, error) {
	if filepath.IsAbs(path) {
		return nil, errors.New("absolute path: give a path relative to the workspace")
	}
	rel, err := relPath(path)
	if err != nil {
		return nil, err
	}
	f, err := openRegular(root, rel, os.O_RDONLY)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	max := p.maxBytes()
	content, err := io.ReadAll(io.LimitReader(ctxReader{ctx, f}, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(content)) > max {
		return nil, fmt.Errorf("too large: at most %s may be published", FormatSize(max))
	}
	return content, nil
}

// ctxReader reads r until ctx ends, checked before each read.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c ctxReader) Read(b []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(b)
}

func publishReport(done, failed []string) string {
	var b strings.Builder
	if len(done) > 0 {
		b.WriteString("Files published (the session's members can download them from your answer):\n")
		b.WriteString(strings.Join(done, "\n"))
		b.WriteString("\n\n")
	}
	if len(failed) > 0 {
		b.WriteString("Files not published:\n")
		b.WriteString(strings.Join(failed, "\n"))
		b.WriteString("\n\n")
	}
	return b.String()
}
