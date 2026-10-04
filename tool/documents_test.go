package tool

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/subproc"
	"github.com/victor/temporal-agent/subproc/subproctest"
)

// fileShelf is the store's published files, as the document tools read
// them.
type fileShelf map[string]shelved

type shelved struct {
	file    store.File
	content []byte
}

func (s fileShelf) GetFile(_ context.Context, id string) (*store.File, error) {
	f, ok := s[id]
	if !ok {
		return nil, nil
	}
	return &f.file, nil
}

func (s fileShelf) ReadFileContent(_ context.Context, id string) ([]byte, error) {
	return s[id].content, nil
}

// shelve puts a file of session s1 (turnCall's) on the shelf.
func (s fileShelf) shelve(id, session, name string, content []byte) {
	s[id] = shelved{store.File{ID: id, SessionID: session, Name: name, Size: int64(len(content))}, content}
}

// documentsFor are the document tools as a worker registers them: as root
// (the agent's container), pandoc and typst run as nobody.
func documentsFor(t *testing.T, pandoc, typst string) (*Documents, *Registry, *fileSaver, fileShelf) {
	t.Helper()
	id := subproctest.Identity(t)
	saver := &fileSaver{}
	shelf := fileShelf{}
	d := &Documents{
		Dir:      subproctest.Dir(t, nil),
		Packages: os.Getenv("TYPST_PACKAGES"),
		RunAs:    id,
		Runs:     subproc.NewRuns(id),
		Pub:      &Publisher{Store: saver},
		Files:    shelf,
		pandoc:   pandoc,
		typst:    typst,
	}
	r := NewRegistry()
	RegisterDocumentTools(r, d)
	return d, r, saver, shelf
}

// callAt is turnCall's context, scheduled at a fixed time.
var callAt = time.Date(2026, 10, 5, 9, 30, 0, 0, time.UTC)

func renderDoc(t *testing.T, r *Registry, tool string, params map[string]any) (string, error) {
	t.Helper()
	return renderDocIn(t, r, turnCall, tool, params)
}

func renderDocIn(t *testing.T, r *Registry, cc CallContext, tool string, params map[string]any) (string, error) {
	t.Helper()
	input, _ := json.Marshal(params)
	ctx, _ := callCtx(cc)
	return r.Execute(WithCallTime(ctx, callAt), tool, input)
}

// fakeProgram writes a shell script standing for pandoc or typst, that
// nobody can run.
func fakeProgram(t *testing.T, script string) string {
	t.Helper()
	path := filepath.Join(subproctest.Dir(t, nil), "fake")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+script+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

// Everything that refuses a call is said before anything runs: the
// programs here would fail it otherwise.
func TestDocuments_Refusals(t *testing.T) {
	big := strings.Repeat("x", MaxTextFileBytes+1)
	for _, c := range []struct {
		name, tool string
		params     map[string]any
		cc         *CallContext
		max        int64
		want       string
	}{
		{name: "pdf in html", tool: "render_pdf", params: map[string]any{"source": "x", "format": "html", "name": "a"}, want: "format must be markdown or typst"},
		{name: "slides in docx", tool: "make_slides", params: map[string]any{"markdown": "x", "format": "docx", "name": "a"}, want: "format must be pptx or pdf"},
		{name: "empty", tool: "render_pdf", params: map[string]any{"source": " \n", "format": "typst", "name": "a"}, want: "source is empty"},
		{name: "too large", tool: "make_slides", params: map[string]any{"markdown": big, "format": "pptx", "name": "a"}, want: "source is too large"},
		{name: "a path", tool: "render_pdf", params: map[string]any{"source": "x", "format": "typst", "name": "../a.pdf"}, want: "is a path"},
		{name: "a control character", tool: "render_pdf", params: map[string]any{"source": "x", "format": "typst", "name": "a\x00.pdf"}, want: "control"},
		{name: "too many files", tool: "render_pdf", params: map[string]any{"source": "x", "format": "typst", "name": "a", "files": strings.Split(strings.Repeat("f,", maxDocumentFiles), ",")}, want: "at most 20 files"},
		{name: "no session turn", tool: "render_pdf", params: map[string]any{"source": "x", "format": "typst", "name": "a"}, cc: &CallContext{CallID: "toolu_1"}, want: "no session turn"},
		{name: "an unknown file", tool: "render_pdf", params: map[string]any{"source": "x", "format": "typst", "name": "a", "files": []string{"nope"}}, want: `no file "nope" in this session`},
		// Another session's file is one that does not exist.
		{name: "another session's file", tool: "make_slides", params: map[string]any{"markdown": "x", "format": "pdf", "name": "a", "files": []string{"f-other"}}, want: `no file "f-other" in this session`},
		{name: "two files of one name", tool: "render_pdf", params: map[string]any{"source": "x", "format": "typst", "name": "a", "files": []string{"f-logo", "f-logo-again"}}, want: "two of the files are named logo.png"},
		{name: "files too large together", tool: "render_pdf", params: map[string]any{"source": "x", "format": "typst", "name": "a", "files": []string{"f-logo", "f-chart"}}, max: 6, want: "too large together"},
	} {
		t.Run(c.name, func(t *testing.T) {
			d, r, saver, shelf := documentsFor(t, "/nonexistent/pandoc", "/nonexistent/typst")
			d.Pub.MaxBytes = c.max
			shelf.shelve("f-logo", "s1", "logo.png", []byte("logo"))
			shelf.shelve("f-logo-again", "s1", "logo.png", []byte("logo 2"))
			shelf.shelve("f-chart", "s1", "chart.png", []byte("chart"))
			shelf.shelve("f-other", "s2", "secret.png", []byte("secret"))
			cc := turnCall
			if c.cc != nil {
				cc = *c.cc
			}
			_, err := renderDocIn(t, r, cc, c.tool, c.params)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("error %v, want %q", err, c.want)
			}
			if len(saver.files) != 0 {
				t.Errorf("published %+v", saver.files)
			}
		})
	}
}

// The document is what the last program writes, published under its name
// with the format's extension; the programs run in a directory that holds
// the call's files, with the call's time as the document's date and no
// proxy that leads anywhere, and the directory is gone once the call is.
func TestDocuments_Publishes(t *testing.T) {
	typst := fakeProgram(t, `cat; echo; ls; echo "HOME=$HOME"; echo "SOURCE_DATE_EPOCH=$SOURCE_DATE_EPOCH"; echo "HTTPS_PROXY=$HTTPS_PROXY"; echo "uid=$(id -u)"; echo careful >&2`)
	d, r, saver, shelf := documentsFor(t, "/nonexistent/pandoc", typst)
	shelf.shelve("f-logo", "s1", "logo.png", []byte("logo"))
	out, err := renderDoc(t, r, "render_pdf", map[string]any{"source": "#lorem(3)", "format": "typst", "name": "rapport", "files": []string{"f-logo"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(saver.files) != 1 || saver.files[0].Name != "rapport.pdf" || saver.files[0].ContentType != "application/pdf" {
		t.Fatalf("published %+v", saver.files)
	}
	doc := string(saver.contents[saver.files[0].ID])
	for _, want := range []string{"#lorem(3)\n", "logo.png\n", "HOME=" + d.Dir + "/document-", "SOURCE_DATE_EPOCH=" + strconv.FormatInt(callAt.Unix(), 10), "HTTPS_PROXY=" + noProxy} {
		if !strings.Contains(doc, want) {
			t.Errorf("document %q, want %q in it", doc, want)
		}
	}
	if os.Geteuid() == 0 && !strings.Contains(doc, "uid="+strconv.Itoa(int(subproctest.UID))) {
		t.Errorf("document %q: typst did not run as nobody", doc)
	}
	// What typst said on its way is the model's to read.
	if !strings.Contains(out, "Published rapport.pdf (") || !strings.Contains(out, "Warnings:\ntypst: careful") {
		t.Errorf("result %q", out)
	}
	if left, _ := os.ReadDir(d.Dir); len(left) != 0 {
		t.Errorf("left behind: %v", left)
	}
}

// A document that does not compile is an error the model reads: the end of
// what the program said, where its errors are.
func TestDocuments_CompileErrorIsReadable(t *testing.T) {
	typst := fakeProgram(t, `head -c 10000 /dev/zero | tr '\0' 'x' >&2; printf '\nerror: unknown variable: lorm\n  ┌─ <stdin>:1:2\n' >&2; exit 1`)
	d, r, saver, _ := documentsFor(t, "/nonexistent/pandoc", typst)
	_, err := renderDoc(t, r, "render_pdf", map[string]any{"source": "#lorm(3)", "format": "typst", "name": "a.pdf"})
	if err == nil || !strings.HasPrefix(err.Error(), "typst could not render the document:\n…") ||
		!strings.HasSuffix(err.Error(), "error: unknown variable: lorm\n  ┌─ <stdin>:1:2") || len(err.Error()) > stderrTail+100 {
		t.Errorf("error %v", err)
	}
	if len(saver.files) != 0 {
		t.Errorf("published %+v", saver.files)
	}
	if left, _ := os.ReadDir(d.Dir); len(left) != 0 {
		t.Errorf("left behind: %v", left)
	}
}

// A rendering is bounded in time, and its document in size: past either,
// nothing is published.
func TestDocuments_Bounds(t *testing.T) {
	t.Run("time", func(t *testing.T) {
		d, r, saver, _ := documentsFor(t, fakeProgram(t, "sleep 30"), "/nonexistent/typst")
		d.timeout = 300 * time.Millisecond
		start := time.Now()
		_, err := renderDoc(t, r, "make_slides", map[string]any{"markdown": "## A", "format": "pptx", "name": "a"})
		if err == nil || !strings.Contains(err.Error(), "took longer than 300ms") || time.Since(start) > 10*time.Second || len(saver.files) != 0 {
			t.Errorf("error %v after %s, published %+v", err, time.Since(start), saver.files)
		}
	})
	t.Run("size", func(t *testing.T) {
		d, r, saver, _ := documentsFor(t, fakeProgram(t, "head -c 100000 /dev/zero"), "/nonexistent/typst")
		d.Pub.MaxBytes = 1000
		_, err := renderDoc(t, r, "make_slides", map[string]any{"markdown": "## A", "format": "pptx", "name": "a"})
		if err == nil || !strings.Contains(err.Error(), "too large: at most 1000 B") || len(saver.files) != 0 {
			t.Errorf("error %v, published %+v", err, saver.files)
		}
	})
}

// A worker running as root without an identity renders nothing: pandoc and
// typst would run as root.
func TestDocuments_RefusesToRunAsRoot(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("only a worker running as root refuses")
	}
	r := NewRegistry()
	RegisterDocumentTools(r, &Documents{Dir: t.TempDir(), Pub: &Publisher{Store: &fileSaver{}}, pandoc: "/bin/true", typst: "/bin/true"})
	if _, err := renderDoc(t, r, "render_pdf", map[string]any{"source": "x", "format": "typst", "name": "a"}); !errors.Is(err, subproc.ErrRootWithoutIdentity) {
		t.Errorf("err = %v, want a refusal", err)
	}
}

// realDocuments are the document tools with the real pandoc and typst, as
// the image installs them; the test is skipped without them.
func realDocuments(t *testing.T) (*Registry, *fileSaver, fileShelf) {
	t.Helper()
	if !DocumentToolsAvailable() {
		t.Skip("pandoc and typst are not installed")
	}
	d, r, saver, shelf := documentsFor(t, "", "")
	if _, err := os.Stat(d.packages()); err != nil {
		t.Skipf("no typst packages: %v", err)
	}
	return r, saver, shelf
}

// pngImage is a PNG of a red square.
func pngImage(t *testing.T) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 40, 30))
	draw.Draw(img, img.Bounds(), &image.Uniform{color.RGBA{R: 255, A: 255}}, image.Point{}, draw.Src)
	var b bytes.Buffer
	if err := png.Encode(&b, img); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

const slidesMarkdown = `---
title: Bilan du trimestre
author: Jarvis
lang: fr
---

# Contexte

## Où en est-on ?

- Livraison : *à l'heure*
- Budget : **respecté**

---

Suite de la diapo.

## Graphique

![Ventes](chart.png){width=30%}

::: notes
Insister sur T2.
:::

## Tableau

| Trimestre | CA |
|-----------|---:|
| T1 | 10 |
| T2 | 12 |
`

// Each format renders for real, the call's image included.
func TestDocuments_Render(t *testing.T) {
	r, saver, shelf := realDocuments(t)
	shelf.shelve("f-chart", "s1", "chart.png", pngImage(t))
	for i, c := range []struct {
		tool   string
		params map[string]any
		file   string
		check  func(t *testing.T, doc []byte)
	}{
		{"render_pdf", map[string]any{"format": "markdown", "name": "rapport", "source": "---\ntitle: Rapport\nlang: fr\n---\n\n# Introduction\n\nÉté, « guillemets », $x^2$.\n\n![Ventes](chart.png){width=20%}\n\n| A | B |\n|---|---|\n| 1 | 2 |\n"}, "rapport.pdf", isPDF},
		{"render_pdf", map[string]any{"format": "typst", "name": "note.pdf", "source": "#import \"@preview/touying:0.8.0\": *\n= Note\n#image(\"chart.png\", width: 1cm)\n#datetime.today().display()"}, "note.pdf", isPDF},
		{"make_slides", map[string]any{"format": "pptx", "name": "bilan", "markdown": slidesMarkdown}, "bilan.pptx", func(t *testing.T, doc []byte) {
			z, err := zip.NewReader(bytes.NewReader(doc), int64(len(doc)))
			if err != nil {
				t.Fatal(err)
			}
			var media, slides int
			for _, f := range z.File {
				switch {
				case strings.HasPrefix(f.Name, "ppt/media/"):
					media++
				case strings.HasPrefix(f.Name, "ppt/slides/slide"):
					slides++
				}
			}
			if media != 1 || slides < 5 {
				t.Errorf("%d images, %d slides", media, slides)
			}
		}},
		{"make_slides", map[string]any{"format": "pdf", "name": "bilan", "markdown": slidesMarkdown}, "bilan.pdf", isPDF},
	} {
		t.Run(c.file, func(t *testing.T) {
			c.params["files"] = []string{"f-chart"}
			cc := turnCall
			cc.CallID = fmt.Sprint("toolu_", i)
			out, err := renderDocIn(t, r, cc, c.tool, c.params)
			if err != nil {
				t.Fatal(err)
			}
			f := saver.files[len(saver.files)-1]
			if f.Name != c.file || !strings.Contains(out, "Published "+c.file) {
				t.Fatalf("published %+v: %s", f, out)
			}
			c.check(t, saver.contents[f.ID])

			// A retry renders the same bytes, and so publishes nothing more.
			if _, err := renderDocIn(t, r, cc, c.tool, c.params); err != nil {
				t.Errorf("retry: %v", err)
			}
			if saver.files[len(saver.files)-1].ID != f.ID {
				t.Errorf("a retry published a second file")
			}
		})
	}
}

func isPDF(t *testing.T, doc []byte) {
	t.Helper()
	if !bytes.HasPrefix(doc, []byte("%PDF-")) {
		t.Errorf("not a PDF: %q", doc[:min(len(doc), 20)])
	}
}

// A document reads nothing but its source and its files: no file outside
// its directory, no URL, and typst downloads no package — it does not even
// try (the proxy is refused before any request leaves).
func TestDocuments_Confined(t *testing.T) {
	r, saver, _ := realDocuments(t)
	for _, c := range []struct {
		name, tool string
		params     map[string]any
		want       []string
	}{
		{"typst reads /etc/passwd", "render_pdf", map[string]any{"format": "typst", "source": `#read("/etc/passwd")`}, []string{"typst could not render", "file not found"}},
		{"typst climbs out", "render_pdf", map[string]any{"format": "typst", "source": `#read("../../../../etc/passwd")`}, []string{"escape the project root"}},
		{"raw typst in Markdown", "render_pdf", map[string]any{"format": "markdown", "source": "```{=typst}\n#read(\"/etc/passwd\")\n```\n"}, []string{"file not found"}},
		{"a Markdown image out of the directory", "render_pdf", map[string]any{"format": "markdown", "source": "![x](../../../../etc/hostname)\n"}, []string{"escape the project root"}},
		{"pandoc reads /etc/passwd", "make_slides", map[string]any{"format": "pptx", "markdown": "## A\n\n![x](/etc/passwd)\n"}, []string{"pandoc could not render", "/etc/passwd' not found"}},
		{"pandoc include", "make_slides", map[string]any{"format": "pptx", "markdown": "## A\n\n\\input{/etc/passwd}\n"}, nil},
		{"pandoc fetches a URL", "make_slides", map[string]any{"format": "pptx", "markdown": "## A\n\n![x](https://example.com/x.png)\n"}, []string{"pandoc could not render", "not found"}},
		{"typst downloads a package", "render_pdf", map[string]any{"format": "typst", "source": `#import "@preview/cetz:0.4.2"`}, []string{"failed to download package", "Connection refused"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			c.params["name"] = "a"
			out, err := renderDoc(t, r, c.tool, c.params)
			if c.want == nil {
				// pandoc leaves what it cannot read out: it must not be in
				// the document.
				if err == nil {
					doc := saver.contents[saver.files[len(saver.files)-1].ID]
					if bytes.Contains(doc, []byte("root:")) {
						t.Errorf("the document holds /etc/passwd")
					}
				}
				return
			}
			if err == nil {
				t.Fatalf("rendered: %s", out)
			}
			for _, want := range c.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %v, want %q", err, want)
				}
			}
			if strings.Contains(err.Error(), "downloading") || strings.Contains(err.Error(), "root:x:0:0") {
				t.Errorf("error %v", err)
			}
		})
	}
	for _, f := range saver.files {
		if bytes.Contains(saver.contents[f.ID], []byte("root:x:0:0")) {
			t.Errorf("%s holds /etc/passwd", f.Name)
		}
	}
}
