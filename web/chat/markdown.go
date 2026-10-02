package chat

import (
	"bytes"
	"encoding/json"
	"html/template"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/extension"
	"github.com/yuin/goldmark/renderer/html"
)

// md renders the agents' Markdown. Raw HTML in the source is escaped, not
// passed through, and javascript: links are dropped (goldmark's defaults,
// kept on purpose): the text comes from a model, which a web page or a
// repository can steer.
var md = goldmark.New(
	goldmark.WithExtensions(extension.GFM),
	goldmark.WithRendererOptions(html.WithHardWraps()),
)

// Markdown renders source to safe HTML.
func Markdown(source string) template.HTML {
	var buf bytes.Buffer
	if err := md.Convert([]byte(source), &buf); err != nil {
		return template.HTML(template.HTMLEscapeString(source))
	}
	return template.HTML(buf.String())
}

// text decodes a stored message's content: a JSON string, or raw JSON for
// anything else.
func text(content string) string {
	var s string
	if json.Unmarshal([]byte(content), &s) == nil {
		return s
	}
	return content
}
