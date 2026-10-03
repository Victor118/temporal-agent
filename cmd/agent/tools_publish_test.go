package main

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/tool"
)

// publishedTools is the tools table: one row per name, with its queue.
type publishedTools struct {
	records    map[string]store.ToolRecord
	failDelete bool
}

func (p *publishedTools) ListTools(context.Context) ([]store.ToolRecord, error) { return nil, nil }
func (p *publishedTools) UpsertTool(_ context.Context, r store.ToolRecord) error {
	p.records[r.Name] = r
	return nil
}
func (p *publishedTools) DeleteTool(_ context.Context, name, queue string) (bool, error) {
	if p.failDelete {
		return false, errors.New("db down")
	}
	if r, ok := p.records[name]; ok && r.TaskQueue == queue {
		delete(p.records, name)
		return true, nil
	}
	return false, nil
}

// What a tool is goes into the tools table with it: the server, which runs
// no tool, reads it from there.
func TestPublishTools_CarriesTheProperties(t *testing.T) {
	r := tool.NewRegistry()
	tool.RegisterMemoryTools(r, nil)
	tool.RegisterExecTool(r, t.TempDir(), nil, nil)
	tool.RegisterAskUserTool(r, func() {})
	p := &publishedTools{records: map[string]store.ToolRecord{}}
	publishTools(context.Background(), p, nil, r.All(), "tools")

	if !p.records["save_user_memory"].PrivateInput || !p.records["exec"].Sensitive || !p.records["ask_user"].NeedsCallContext {
		t.Errorf("published %+v", p.records)
	}
	if p.records["exec"].Timeout <= tool.DefaultTimeout || p.records["save_user_memory"].Timeout != 0 {
		t.Errorf("timeouts: exec %s, save_user_memory %s", p.records["exec"].Timeout, p.records["save_user_memory"].Timeout)
	}
	if p.records["exec"].PrivateInput || p.records["save_user_memory"].Sensitive {
		t.Errorf("properties leaked between tools: %+v", p.records)
	}
}

// After startup, an MCP server's new tools are published and the ones it
// dropped withdrawn — from this queue only; what could not be withdrawn
// comes back to be tried again.
func TestCatalogPublisher(t *testing.T) {
	p := &publishedTools{records: map[string]store.ToolRecord{
		"gh_old":   {Name: "gh_old", TaskQueue: "tools"},
		"gh_other": {Name: "gh_other", TaskQueue: "elsewhere"},
	}}
	pub := catalogPublisher{st: p, queue: "tools"}

	retry := pub.Publish(context.Background(), []*tool.Tool{{Name: "gh_new", Kind: tool.ToolKindMCP}}, []string{"gh_old", "gh_other"})
	if len(retry) != 0 {
		t.Errorf("retry = %v", retry)
	}
	if got := fmt.Sprintf("%d %s %s", len(p.records), p.records["gh_new"].TaskQueue, p.records["gh_other"].TaskQueue); got != "2 tools elsewhere" {
		t.Errorf("table = %+v", p.records)
	}

	p.failDelete = true
	if retry := pub.Publish(context.Background(), nil, []string{"gh_new"}); fmt.Sprint(retry) != "[gh_new]" {
		t.Errorf("failed withdrawal: retry = %v", retry)
	}
}
