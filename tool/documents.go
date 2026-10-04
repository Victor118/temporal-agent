package tool

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/subproc"
)

// documentAssets are what the tools give pandoc: the slides' template, the
// filter, a Markdown document's default metadata.
//
//go:embed documents/slides.typst documents/filter.lua documents/metadata.yaml
var documentAssets embed.FS

const (
	// renderTimeout bounds the rendering of one document, pandoc and typst
	// together.
	renderTimeout = 60 * time.Second
	// documentFilesBudget bounds the reading of the files a document uses
	// from the store.
	documentFilesBudget = 30 * time.Second
	// maxConcurrentRenders bounds the renderings one worker runs at once:
	// they share the machine with the workflows the main worker serves.
	maxConcurrentRenders = 2
	// renderWait bounds how long a call waits for a rendering to end
	// before it is refused as busy: renderings take a second or two, so a
	// burst is absorbed; a queue that does not move is said.
	renderWait = 30 * time.Second
	// typstJobs bounds the threads typst compiles with.
	typstJobs = 2
	// documentTimeout bounds one call: a slot, the files, the rendering,
	// publishing, and the margin to return.
	documentTimeout = renderWait + documentFilesBudget + renderTimeout + publishBudget + TimeoutMargin
	// documentDirPrefix names a call's directory under Documents.Dir.
	documentDirPrefix = "document-"
	// maxDocumentFiles bounds the files one document uses.
	maxDocumentFiles = 20
	// maxTypstSource bounds the typst pandoc writes from a Markdown source
	// (at most MaxTextFileBytes): far more than it ever takes.
	maxTypstSource = 16 << 20
	// stderrTail is how much of what pandoc or typst says the model reads:
	// the end, where the errors are.
	stderrTail = 4 << 10
	// typstMemory bounds typst's address space, in KiB: a document that
	// builds a huge array fails alone, not the worker it shares the machine
	// with.
	typstMemory = 2 << 20
	// pandocHeap bounds pandoc's heap, its runtime's own way: it reserves
	// far more address space than it uses, which an address space limit
	// would refuse.
	pandocHeap = "-M1024m"
	// DefaultTypstPackages is where the image puts the typst packages a
	// document may import (TYPST_PACKAGES).
	DefaultTypstPackages = "/usr/local/share/typst/packages"
)

// noProxy is a proxy no connection reaches: the port 0 of the loopback.
// typst downloads a package it does not find in its package path; with
// this proxy, it cannot. A cache it cannot write is not enough: it
// downloads first, and fails to unpack after.
const noProxy = "http://127.0.0.1:0"

var proxyNames = []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy", "NO_PROXY", "no_proxy"}

// FileReader reads the files of the store a document uses
// (store.FileStore).
type FileReader interface {
	GetFile(ctx context.Context, id string) (*store.File, error)
	ReadFileContent(ctx context.Context, id string) ([]byte, error)
}

// Documents renders the documents of render_pdf and make_slides with pandoc
// and typst, and publishes them.
//
// Each call gets a directory of its own under Dir, never the agent's
// workspace, removed once it is over. The worker writes everything there:
// the files the document uses, copied from the store, and what pandoc
// reads; pandoc and typst, run as RunAs, write nothing: they read their
// input from stdin and write their output to stdout, read up to the largest
// file the worker publishes. Nothing of the directory is RunAs's: a process
// of its could not swap a file of it for a link.
//
// pandoc runs with --sandbox: it reads no file but those its command line
// names (a filter of the tools' puts the call's images in its media bag).
// typst reads no file outside the directory (--root), imports only the
// packages the image embeds (Packages), downloads none (noProxy), and gets
// at most typstMemory. Both run as exec's commands do: a filtered
// environment, their process group killed when they return, counted by
// Runs, refused on a worker running as root with no RunAs.
type Documents struct {
	// Dir is where each call makes its directory.
	Dir string
	// Packages holds the typst packages a document may import, read-only
	// to RunAs: typst's package path and cache.
	Packages string
	RunAs    *subproc.Identity
	Runs     Holder
	Pub      *Publisher
	Files    FileReader

	// pandoc and typst are the binaries' paths (DocumentToolsAvailable).
	pandoc, typst string
	// slots bounds the renderings under way (maxConcurrentRenders);
	// made by RegisterDocumentTools, shared by both tools.
	slots chan struct{}
	// timeout and wait replace renderTimeout and renderWait, shorter, in
	// tests.
	timeout, wait time.Duration
}

// DocumentToolsAvailable reports whether pandoc and typst are installed: a
// worker without them has no business offering render_pdf and make_slides.
func DocumentToolsAvailable() bool {
	_, pandocErr := exec.LookPath("pandoc")
	_, typstErr := exec.LookPath("typst")
	return pandocErr == nil && typstErr == nil
}

// documentFile is a published file a document uses, under its name.
type documentFile struct {
	name    string
	content []byte
}

// RegisterDocumentTools registers render_pdf and make_slides, which render
// a document with d and publish it to the session.
func RegisterDocumentTools(r *Registry, d *Documents) {
	if d.slots == nil {
		d.slots = make(chan struct{}, maxConcurrentRenders)
	}
	if d.pandoc == "" {
		d.pandoc, _ = exec.LookPath("pandoc")
	}
	if d.typst == "" {
		d.typst, _ = exec.LookPath("typst")
	}
	filesParam := `"files": {"type": "array", "items": {"type": "string"}, "description": "IDs of files already published in this session (at most ` + strconv.Itoa(maxDocumentFiles) + `, together within the worker's size limit) that the document uses, such as images: each is put next to the document under its published name, which the source refers to as is (image(\"chart.png\"), ![](chart.png))."}`
	timeout := documentTimeout

	r.Register(&Tool{
		Name: "render_pdf",
		Description: `Render a PDF document and publish it to the session: its members see it attached to your answer and can download it. Use it for a report, a letter, a summary the user should keep as a PDF.
format markdown: Pandoc Markdown, rendered through typst. A YAML metadata block at the top sets title, author, date, lang (e.g. fr, for hyphenation and quotes), papersize (a4 by default), fontsize, toc: true, section-numbering: "1.1". Tables, footnotes, math ($...$) and code blocks work; a raw typst block (` + "```{=typst}" + `) passes typst through.
format typst: a typst document, compiled as is. Only the typst packages this worker embeds can be imported: @preview/touying:0.8.0.
The source and its files are all the document can read: no other file, no URL. If the document does not compile, the result says why: fix the source and call again.
Mention the file by its name in your answer; do not repeat its content.`,
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"source": {"type": "string", "description": "The document's source (at most ` + FormatSize(MaxTextFileBytes) + `)."},
				"format": {"type": "string", "enum": ["markdown", "typst"], "description": "The source's language."},
				"name": {"type": "string", "description": "The PDF's file name, e.g. rapport.pdf (.pdf is added if missing). A name, not a path."},
				` + filesParam + `
			},
			"required": ["source", "format", "name"]
		}`),
		Kind:             ToolKindActivity,
		Timeout:          timeout,
		NeedsCallContext: true, // the session turn the PDF is attached to
		Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
			var params struct {
				Source string   `json:"source"`
				Format string   `json:"format"`
				Name   string   `json:"name"`
				Files  []string `json:"files"`
			}
			if err := json.Unmarshal(input, &params); err != nil {
				return "", fmt.Errorf("parse input: %w", err)
			}
			var steps []renderStep
			switch params.Format {
			case "markdown":
				steps = []renderStep{d.pandocStep("typst", "-s", "--metadata-file={data}/metadata.yaml"), d.typstStep()}
			case "typst":
				steps = []renderStep{d.typstStep()}
			default:
				return "", fmt.Errorf("format must be markdown or typst, not %q", params.Format)
			}
			return d.render(ctx, params.Source, params.Name, "pdf", params.Files, steps)
		},
	})

	r.Register(&Tool{
		Name: "make_slides",
		Description: `Make a slide deck from Markdown and publish it to the session: its members see it attached to your answer and can download it.
format pptx: a PowerPoint file the user can edit. format pdf: a PDF to present as is, 16:9.
The Markdown: a YAML metadata block at the top (title, subtitle, author, date, lang) makes the title slide; "# Heading" opens a section (a slide of its own), "## Heading" starts a slide with that title, a line of "---" starts a new slide under the same title. On a slide: paragraphs, lists, **bold**, *italics*, tables, code blocks, math ($...$), images (![caption](chart.png){width=60%}, one of the files). A ::: notes block holds the speaker's notes: kept as notes in pptx, left out of the PDF. Keep a slide short: what does not fit is cut off.
The Markdown and its files are all the deck can read: no other file, no URL. If it does not render, the result says why: fix the Markdown and call again.
Mention the file by its name in your answer; do not repeat its content.`,
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"markdown": {"type": "string", "description": "The slides, in Pandoc Markdown (at most ` + FormatSize(MaxTextFileBytes) + `)."},
				"format": {"type": "string", "enum": ["pptx", "pdf"], "description": "pptx (editable) or pdf."},
				"name": {"type": "string", "description": "The deck's file name, e.g. bilan.pptx (the format's extension is added if missing). A name, not a path."},
				` + filesParam + `
			},
			"required": ["markdown", "format", "name"]
		}`),
		Kind:             ToolKindActivity,
		Timeout:          timeout,
		NeedsCallContext: true, // the session turn the deck is attached to
		Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
			var params struct {
				Markdown string   `json:"markdown"`
				Format   string   `json:"format"`
				Name     string   `json:"name"`
				Files    []string `json:"files"`
			}
			if err := json.Unmarshal(input, &params); err != nil {
				return "", fmt.Errorf("parse input: %w", err)
			}
			var steps []renderStep
			switch params.Format {
			case "pptx":
				steps = []renderStep{d.pandocStep("pptx", "--slide-level=2", "--lua-filter={data}/filter.lua")}
			case "pdf":
				steps = []renderStep{d.pandocStep("typst", "--template={data}/templates/slides.typst", "--lua-filter={data}/filter.lua"), d.typstStep()}
			default:
				return "", fmt.Errorf("format must be pptx or pdf, not %q", params.Format)
			}
			return d.render(ctx, params.Markdown, params.Name, params.Format, params.Files, steps)
		},
	})
}

// renderStep is one program of a rendering: it reads the step before's
// output (the source, for the first) and writes the next one's input (the
// document, for the last), at most max bytes.
type renderStep struct {
	program string // what the model reads it as: pandoc, typst
	args    func(call callDir) []string
	path    string
	// intermediate: what it writes is the next step's input, not the
	// document.
	intermediate bool
}

// pandocStep converts Markdown to format. In args, {data} is the call's
// data directory, where the assets are.
func (d *Documents) pandocStep(format string, args ...string) renderStep {
	return renderStep{program: "pandoc", path: d.pandoc, intermediate: format == "typst", args: func(c callDir) []string {
		out := []string{"+RTS", pandocHeap, "-RTS", "--sandbox", "--data-dir=" + c.data, "-f", "markdown", "-t", format, "-o", "-"}
		for _, a := range args {
			out = append(out, strings.ReplaceAll(a, "{data}", c.data))
		}
		return out
	}}
}

// typstStep compiles typst to PDF, in a shell that bounds its memory first.
func (d *Documents) typstStep() renderStep {
	return renderStep{program: "typst", path: "/bin/sh", args: func(c callDir) []string {
		return []string{"-c", `ulimit -c 0 && ulimit -v "$1" && shift && exec "$@"`, "sh", strconv.Itoa(typstMemory),
			d.typst, "compile", "--jobs", strconv.Itoa(typstJobs), "--root", c.src, "--package-path", d.packages(), "--package-cache-path", d.packages(), "-", "-"}
	}}
}

func (d *Documents) packages() string {
	if d.Packages == "" {
		return DefaultTypstPackages
	}
	return d.Packages
}

// callDir is the directory of one call: src holds the files the document
// uses, and is where pandoc and typst run; data, the assets pandoc reads.
type callDir struct {
	root, src, data string
}

// render renders source through steps, as a file of name with ext, and
// publishes it.
func (d *Documents) render(ctx context.Context, source, name, ext string, fileIDs []string, steps []renderStep) (string, error) {
	// Everything that refuses the call before it renders anything.
	if strings.TrimSpace(source) == "" {
		return "", errors.New("the source is empty")
	}
	if len(source) > MaxTextFileBytes {
		return "", fmt.Errorf("the source is too large: at most %s", FormatSize(MaxTextFileBytes))
	}
	name, err := CleanFileName(name)
	if err != nil {
		return "", err
	}
	if !strings.HasSuffix(strings.ToLower(name), "."+ext) {
		if name, err = CleanFileName(name + "." + ext); err != nil {
			return "", err
		}
	}
	if len(fileIDs) > maxDocumentFiles {
		return "", fmt.Errorf("a document uses at most %d files", maxDocumentFiles)
	}
	if err := subproc.CheckRunAs(d.RunAs); err != nil {
		return "", fmt.Errorf("documents: %w", err)
	}
	if d.RunAs != nil && d.Runs == nil {
		return "", errors.New("documents: commands run as RUN_AS_UID need a count of them (subproc.Runs)")
	}
	call, err := d.Pub.call(ctx)
	if err != nil {
		return "", err
	}

	// A slot before the files are read: a call waiting for one holds
	// none of them in memory.
	release, err := d.acquire(ctx)
	if err != nil {
		return "", err
	}
	defer release()

	files, err := d.loadFiles(ctx, call.Turn.SessionID, fileIDs)
	if err != nil {
		return "", err
	}
	dir, err := d.makeDir(files)
	if err != nil {
		return "", fmt.Errorf("documents: prepare the directory: %w", err)
	}
	defer os.RemoveAll(dir.root)

	out, warnings, err := d.run(ctx, dir, []byte(source), steps)
	if err != nil {
		return "", err
	}

	pubCtx, cancel := context.WithTimeout(ctx, publishBudget)
	defer cancel()
	f, err := d.Pub.publish(pubCtx, name, out, d.Pub.maxBytes())
	if err != nil {
		return "", err
	}
	result := "Published " + published(f) + ". The session's members can download it from your answer."
	if warnings != "" {
		result += "\n\nWarnings:\n" + warnings
	}
	return result, nil
}

// acquire takes one of the worker's rendering slots, waiting for one at
// most renderWait: past it, the call is refused, and the model may try
// again later.
func (d *Documents) acquire(ctx context.Context) (release func(), err error) {
	wait := renderWait
	if d.wait > 0 {
		wait = d.wait
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case d.slots <- struct{}{}:
		return func() { <-d.slots }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, fmt.Errorf("this worker is busy rendering other documents (at most %d at a time): try again in a moment", maxConcurrentRenders)
	}
}

// loadFiles reads the files of ids from the store: each one published in
// sessionID, under a name no other has. A file of another session is
// refused as one that does not exist: the call learns nothing of it.
func (d *Documents) loadFiles(ctx context.Context, sessionID string, ids []string) ([]documentFile, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	if d.Files == nil {
		return nil, errors.New("documents: this worker has no file store to read the files from")
	}
	ctx, cancel := context.WithTimeout(ctx, documentFilesBudget)
	defer cancel()
	max := d.Pub.maxBytes()
	var total int64
	names := map[string]bool{}
	files := make([]documentFile, 0, len(ids))
	for _, id := range ids {
		unknown := fmt.Errorf("no file %q in this session: give the ID of a file published here", id)
		f, err := d.Files.GetFile(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("read file %s: %w", id, err)
		}
		if f == nil || f.SessionID != sessionID {
			return nil, unknown
		}
		name, err := CleanFileName(f.Name)
		if err != nil {
			return nil, fmt.Errorf("file %s: %w", id, err)
		}
		if names[name] {
			return nil, fmt.Errorf("two of the files are named %s: the document could not tell them apart", name)
		}
		names[name] = true
		if total += f.Size; total > max {
			return nil, fmt.Errorf("the files are too large together: at most %s", FormatSize(max))
		}
		content, err := d.Files.ReadFileContent(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("read file %s: %w", id, err)
		}
		if content == nil {
			return nil, unknown
		}
		files = append(files, documentFile{name: name, content: content})
	}
	return files, nil
}

// makeDir makes the call's directory, with the files in src and the assets
// in data: the worker's, which RunAs reads and cannot change.
func (d *Documents) makeDir(files []documentFile) (callDir, error) {
	root, err := os.MkdirTemp(d.Dir, documentDirPrefix+"*")
	if err != nil {
		return callDir{}, err
	}
	dir := callDir{root: root, src: filepath.Join(root, "src"), data: filepath.Join(root, "data")}
	err = func() error {
		// MkdirTemp makes it 0700: RunAs must go through.
		if err := os.Chmod(root, 0o755); err != nil {
			return err
		}
		for _, sub := range []string{dir.src, dir.data, filepath.Join(dir.data, "templates")} {
			if err := os.Mkdir(sub, 0o755); err != nil {
				return err
			}
		}
		assets := map[string]string{"slides.typst": "templates/slides.typst", "filter.lua": "filter.lua", "metadata.yaml": "metadata.yaml"}
		for asset, path := range assets {
			content, err := documentAssets.ReadFile("documents/" + asset)
			if err != nil {
				return err
			}
			if err := writeNew(filepath.Join(dir.data, path), content); err != nil {
				return err
			}
		}
		for _, f := range files {
			if err := writeNew(filepath.Join(dir.src, f.name), f.content); err != nil {
				return err
			}
		}
		return nil
	}()
	if err != nil {
		os.RemoveAll(root)
		return callDir{}, err
	}
	return dir, nil
}

// Sweep removes the directories of calls under Dir that a worker killed
// mid-call left there: those older than a call may last (documentTimeout),
// so that another worker process rendering in the same Dir keeps its own.
// A link is removed, never followed.
func (d *Documents) Sweep() (removed int, err error) {
	entries, err := os.ReadDir(d.Dir)
	if err != nil {
		return 0, err
	}
	var errs []error
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), documentDirPrefix) {
			continue
		}
		path := filepath.Join(d.Dir, e.Name())
		fi, err := os.Lstat(path)
		if err != nil || time.Since(fi.ModTime()) < documentTimeout {
			continue
		}
		if err := os.RemoveAll(path); err != nil {
			errs = append(errs, err)
			continue
		}
		removed++
	}
	return removed, errors.Join(errs...)
}

// writeNew writes a file that must not exist yet, readable by all.
func writeNew(path string, content []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	_, err = f.Write(content)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// run runs steps in turn on input, within the rendering's time, and returns
// the document and what the programs said along the way. An error is the
// model's to read: the end of what the failed program said.
func (d *Documents) run(ctx context.Context, dir callDir, input []byte, steps []renderStep) ([]byte, string, error) {
	renderCtx, cancel := context.WithTimeout(ctx, d.limit())
	defer cancel()
	env := documentEnv(dir.root)
	var warnings []string
	for _, step := range steps {
		max := d.Pub.maxBytes()
		if step.intermediate {
			max = maxTypstSource
		}
		out, said, err := d.runStep(renderCtx, dir, env, step, input, max)
		switch {
		case err != nil && ctx.Err() != nil:
			// The call itself ended (cancelled, its worker stopping):
			// the program was killed for it, and said nothing of use.
			return nil, "", ctx.Err()
		case err != nil && renderCtx.Err() != nil:
			return nil, "", fmt.Errorf("the rendering took longer than %s, the most it may take: make the document simpler, or split it", d.limit())
		case err != nil:
			return nil, "", err
		}
		if said != "" {
			warnings = append(warnings, step.program+": "+said)
		}
		input = out
	}
	return input, strings.Join(warnings, "\n"), nil
}

// runStep runs one step, as RunAs, in the call's src directory.
func (d *Documents) runStep(ctx context.Context, dir callDir, env []string, step renderStep, input []byte, max int64) ([]byte, string, error) {
	cmd := exec.CommandContext(ctx, step.path, step.args(dir)...)
	cmd.Dir = dir.src
	cmd.Env = env
	cmd.Stdin = bytes.NewReader(input)
	stdout := &cappedBuffer{max: max}
	stderr := &tailBuffer{max: stderrTail}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	d.RunAs.Apply(cmd)
	subproc.KillGroupOnCancel(cmd, syscall.SIGKILL, execKillGrace)

	release := hold(d.Runs)
	err := cmd.Run()
	subproc.KillGroup(cmd)
	release()

	said := strings.TrimSpace(stderr.String())
	switch {
	case err != nil && ctx.Err() != nil:
		return nil, "", ctx.Err() // run says why
	case stdout.exceeded:
		if step.intermediate {
			return nil, "", fmt.Errorf("%s wrote more than %s from the source: make the document simpler", step.program, FormatSize(max))
		}
		return nil, "", fmt.Errorf("the document is too large: at most %s may be published", FormatSize(max))
	case err != nil:
		if said == "" {
			said = err.Error()
		}
		return nil, "", fmt.Errorf("%s could not render the document:\n%s", step.program, said)
	}
	return stdout.Bytes(), said, nil
}

// limit bounds the rendering of one document: renderTimeout, but in tests.
func (d *Documents) limit() time.Duration {
	if d.timeout > 0 {
		return d.timeout
	}
	return renderTimeout
}

// documentEnv is pandoc's and typst's environment: the worker's, filtered,
// with home the call's directory (theirs, in their user's home, could hold
// what exec left there: a font, pandoc's data), no proxy that leads
// anywhere, and the time of the call as the document's date, one date for
// all of its steps. TMPDIR is the call's directory too, which they cannot
// write: neither writes a temporary file, and one that tried would fail
// rather than leave it in the shared /tmp.
func documentEnv(home string) []string {
	drop := map[string]bool{"HOME": true, "TMPDIR": true}
	for _, n := range proxyNames {
		drop[n] = true
	}
	var env []string
	for _, kv := range subproc.Env(os.Environ(), nil, nil) {
		if name, _, _ := strings.Cut(kv, "="); !drop[name] {
			env = append(env, kv)
		}
	}
	env = append(env, "HOME="+home, "TMPDIR="+home, "SOURCE_DATE_EPOCH="+strconv.FormatInt(time.Now().Unix(), 10))
	for _, n := range proxyNames {
		if !strings.EqualFold(n, "no_proxy") {
			env = append(env, n+"="+noProxy)
		}
	}
	return env
}

// cappedBuffer keeps what is written up to max bytes, and fails the write
// that goes past: the program then writes to a closed pipe and stops.
type cappedBuffer struct {
	bytes.Buffer
	max      int64
	exceeded bool
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	if int64(b.Len()+len(p)) > b.max {
		b.exceeded = true
		return 0, errors.New("output too large")
	}
	return b.Buffer.Write(p)
}

// tailBuffer keeps the last max bytes written.
type tailBuffer struct {
	buf []byte
	max int
	cut bool
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	b.buf = append(b.buf, p...)
	if len(b.buf) > b.max {
		b.buf = append(b.buf[:0], b.buf[len(b.buf)-b.max:]...)
		b.cut = true
	}
	return len(p), nil
}

// String is what was kept, from its first whole character.
func (b *tailBuffer) String() string {
	s := strings.ToValidUTF8(string(b.buf), "")
	if b.cut {
		return "…" + s
	}
	return s
}
