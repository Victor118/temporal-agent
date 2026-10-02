package main

import (
	"context"
	"testing"

	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/tool"
)

type publishedTools struct{ records map[string]store.ToolRecord }

func (p *publishedTools) ListTools(context.Context) ([]store.ToolRecord, error) { return nil, nil }
func (p *publishedTools) UpsertTool(_ context.Context, r store.ToolRecord) error {
	p.records[r.Name] = r
	return nil
}

// What a tool is goes into the tools table with it: the server, which runs
// no tool, reads it from there.
func TestPublishTools_CarriesTheProperties(t *testing.T) {
	r := tool.NewRegistry()
	tool.RegisterMemoryTools(r, nil)
	tool.RegisterExecTool(r, t.TempDir(), nil, nil)
	tool.RegisterAskUserTool(r, func() {})
	p := &publishedTools{records: map[string]store.ToolRecord{}}
	publishTools(p, nil, r, "tools")

	if !p.records["save_user_memory"].PrivateInput || !p.records["exec"].Sensitive || !p.records["ask_user"].NeedsCallContext {
		t.Errorf("published %+v", p.records)
	}
	if p.records["exec"].PrivateInput || p.records["save_user_memory"].Sensitive {
		t.Errorf("properties leaked between tools: %+v", p.records)
	}
}
