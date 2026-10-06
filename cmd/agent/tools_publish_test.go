package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/tool"
)

// publishedTools is the tools table: one row per name, with its queue.
type publishedTools struct {
	records    map[string]store.ToolRecord
	failDelete bool
}

func (p *publishedTools) ListTools(context.Context) ([]store.ToolRecord, error) {
	var out []store.ToolRecord
	for _, r := range p.records {
		out = append(out, r)
	}
	return out, nil
}
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
	tool.RegisterExecTool(r, t.TempDir(), nil, nil, nil)
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

// A worker withdraws from its queue what it no longer offers (analyze_repo
// left the coding containers), but not another queue's rows, nor its MCP
// servers' (their discovery's to manage).
func TestWithdrawUnoffered(t *testing.T) {
	r := tool.NewRegistry()
	tool.RegisterImplementFeatureTool(r, func() {}, tool.CodingRoute{Fallback: true})
	p := &publishedTools{records: map[string]store.ToolRecord{
		"analyze_repo":      {Name: "analyze_repo", TaskQueue: "coding"},
		"implement_feature": {Name: "implement_feature", TaskQueue: "coding"},
		"gh_search":         {Name: "gh_search", TaskQueue: "coding"},
		"web_fetch":         {Name: "web_fetch", TaskQueue: "agent"},
	}}
	withdrawUnoffered(context.Background(), p, r, "coding", []string{"gh_"})
	if _, ok := p.records["analyze_repo"]; ok {
		t.Error("a tool no longer offered stays")
	}
	for _, name := range []string{"implement_feature", "gh_search", "web_fetch"} {
		if _, ok := p.records[name]; !ok {
			t.Errorf("%s withdrawn", name)
		}
	}
}

// A tool another queue still serves is published once that queue lets it
// go, whatever the order the workers start in.
func TestKeepPublishing_UntilTheOtherQueueLetsGo(t *testing.T) {
	r := tool.NewRegistry()
	tool.RegisterAnalyzeRepoTool(r, func() {}, tool.CodingRoute{Machines: true})
	p := &publishedTools{records: map[string]store.ToolRecord{"analyze_repo": {Name: "analyze_repo", TaskQueue: "coding"}}}
	served := &servedQueues{queues: map[string]bool{"coding": true}}
	left := publishTools(context.Background(), p, served, r.All(), "agent")
	if len(left) != 1 || p.records["analyze_repo"].TaskQueue != "coding" {
		t.Fatalf("published over a served queue: %v %+v", left, p.records)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		keepPublishing(ctx, p, served, r, "agent", left, 10*time.Millisecond)
		close(done)
	}()
	served.set("coding", false)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("never published")
	}
	if p.records["analyze_repo"].TaskQueue != "agent" {
		t.Errorf("record %+v", p.records["analyze_repo"])
	}
}

// servedQueues answers DescribeTaskQueue: a fresh poller on the queues it
// serves.
type servedQueues struct {
	mu     sync.Mutex
	queues map[string]bool
}

func (s *servedQueues) set(queue string, served bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queues[queue] = served
}

func (s *servedQueues) DescribeTaskQueue(_ context.Context, queue string, _ enumspb.TaskQueueType) (*workflowservice.DescribeTaskQueueResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resp := &workflowservice.DescribeTaskQueueResponse{}
	if s.queues[queue] {
		resp.Pollers = []*taskqueuepb.PollerInfo{{Identity: "w", LastAccessTime: timestamppb.Now()}}
	}
	return resp, nil
}
