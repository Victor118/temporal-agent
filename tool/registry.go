package tool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/victor/temporal-agent/provider"
)

type ToolKind string

const (
	ToolKindActivity ToolKind = "activity"
	ToolKindWorkflow ToolKind = "workflow"
	ToolKindMCP      ToolKind = "mcp"
)

type ExecuteFunc func(ctx context.Context, input json.RawMessage) (string, error)

type Tool struct {
	Name          string          `json:"name"`
	Description   string          `json:"description"`
	InputSchema   json.RawMessage `json:"input_schema"`
	Kind          ToolKind        `json:"-"`
	Execute       ExecuteFunc     `json:"-"`
	WorkflowFunc  interface{}     `json:"-"` // Workflow function for ToolKindWorkflow
	TaskQueue     string          `json:"-"` // Target task queue for workflow tools
	FireAndForget bool            `json:"-"` // If true, don't wait for workflow result

	// What the tool is, published with it so that every process — the
	// server included, which runs no tool — knows it from the tools table
	// rather than from a list of names kept somewhere else.

	// Sensitive: it changes the outside world (runs commands, writes files,
	// pushes code, sends mail). Shown in the back-office; the allowlist is
	// what grants or denies it.
	Sensitive bool `json:"-"`
	// PrivateInput: its input is the user's alone, and is not shown to the
	// session's other members, nor put in a summary for someone else's fork.
	// Nor is its result, which may repeat it (DisplayResult).
	PrivateInput bool `json:"-"`
	// NeedsCallContext: the tool receives the caller's context (CallContext):
	// a workflow tool in its input, alongside what the model wrote; an
	// activity tool in its context.Context (CallFromContext).
	NeedsCallContext bool `json:"-"`
	// Timeout bounds one call of an activity or MCP tool: the activity's
	// start-to-close timeout. Zero = DefaultTimeout. A tool with a limit of
	// its own declares that limit plus TimeoutMargin, so that it stops first
	// and returns what it has: stopped by Temporal, it returns nothing.
	// A workflow tool runs as a child workflow, with no timeout: unused.
	Timeout time.Duration `json:"-"`
}

const (
	// DefaultTimeout bounds a call to a tool that declares no Timeout.
	DefaultTimeout = 120 * time.Second
	// TimeoutMargin is what a tool's Timeout leaves beyond its own limit:
	// time to stop the work, end what it started and return.
	TimeoutMargin = 30 * time.Second
)

// WorkflowName returns the function name used by Temporal to identify the workflow.
func (t *Tool) WorkflowName() string {
	if t.WorkflowFunc == nil {
		return ""
	}
	fullName := runtime.FuncForPC(reflect.ValueOf(t.WorkflowFunc).Pointer()).Name()
	// Extract short name (e.g. "AgentWorkflow" from "github.com/.../workflow.AgentWorkflow")
	if idx := strings.LastIndex(fullName, "."); idx >= 0 {
		return fullName[idx+1:]
	}
	return fullName
}

// SchemaHash identifies the tool's contract (description, input schema, kind,
// workflow). Two workers exposing the same tool must produce the same hash.
func (t *Tool) SchemaHash() string {
	h := sha256.New()
	for _, part := range []string{
		t.Description,
		string(t.InputSchema),
		string(t.Kind),
		t.WorkflowName(),
		fmt.Sprint(t.FireAndForget),
		fmt.Sprint(t.Sensitive, t.PrivateInput, t.NeedsCallContext, t.Timeout),
	} {
		h.Write([]byte(part))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// MatchAny reports whether name matches at least one glob (path.Match syntax).
// Invalid patterns never match; validate them when loading config.
func MatchAny(globs []string, name string) bool {
	for _, g := range globs {
		if ok, _ := path.Match(g, name); ok {
			return true
		}
	}
	return false
}

// Registry holds the tools this process runs. MCP servers' tools change
// while activities read it, so every method is safe for concurrent use.
type Registry struct {
	mu    sync.RWMutex
	tools map[string]registered
}

// registered is a tool and where it comes from.
type registered struct {
	tool   *Tool
	source string // "" = built in; otherwise the MCP server that gave it
}

func NewRegistry() *Registry {
	return &Registry{
		tools: make(map[string]registered),
	}
}

// Register adds a built-in tool.
func (r *Registry) Register(t *Tool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tools[t.Name] = registered{tool: t}
}

func (r *Registry) Get(name string) (*Tool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.tools[name]
	return e.tool, ok
}

var emptySchema = json.RawMessage(`{"type":"object","properties":{}}`)

// List returns tool definitions sorted by name. The order must be stable:
// tools are the start of the LLM prompt prefix, so any reordering invalidates
// the prompt cache (tools, system prompt and history).
func (r *Registry) List() []provider.ToolDefinition {
	tools := r.All()
	defs := make([]provider.ToolDefinition, 0, len(tools))
	for _, t := range tools {
		schema := t.InputSchema
		if len(schema) == 0 {
			schema = emptySchema
		}
		defs = append(defs, provider.ToolDefinition{
			Name:        t.Name,
			Description: t.Description,
			InputSchema: schema,
		})
	}
	return defs
}

// All returns the registered tools sorted by name.
func (r *Registry) All() []*Tool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := r.sortedNames()
	tools := make([]*Tool, len(names))
	for i, name := range names {
		tools[i] = r.tools[name].tool
	}
	return tools
}

// Retain removes every tool whose name matches none of the globs and returns
// the removed names, sorted.
func (r *Registry) Retain(globs []string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var removed []string
	for _, name := range r.sortedNames() {
		if !MatchAny(globs, name) {
			delete(r.tools, name)
			removed = append(removed, name)
		}
	}
	return removed
}

// SourceChange is what SyncSource did to the registry.
type SourceChange struct {
	Changed []*Tool   // added, or whose contract changed (SchemaHash)
	Removed []string  // no longer given by the source
	Refused []Refusal // held by another source
}

// Refusal is a tool not registered because its name is already held.
type Refusal struct {
	Name   string
	HeldBy string // "" = a built-in tool; otherwise an MCP server
}

func (r Refusal) String() string {
	if r.HeldBy == "" {
		return fmt.Sprintf("%s (a built-in tool)", r.Name)
	}
	return fmt.Sprintf("%s (held by MCP server %s)", r.Name, r.HeldBy)
}

// SyncSource makes the tools of an MCP server exactly tools: new ones are
// added, the others replaced, and the ones the server no longer gives are
// removed. A name another source holds — a built-in tool, another server —
// is never taken over: it stays its holder's until the holder drops it.
// So no tool silently replaces another, and since startup syncs the
// servers in config order, the earlier server wins there.
func (r *Registry) SyncSource(source string, tools []*Tool) SourceChange {
	if source == "" {
		panic("tool: SyncSource without a source would take built-in tools") // config requires MCP names
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var change SourceChange
	given := make(map[string]bool, len(tools))
	for _, t := range tools {
		if given[t.Name] {
			continue // the server named two tools alike: the first stands
		}
		prev, held := r.tools[t.Name]
		if held && prev.source != source {
			change.Refused = append(change.Refused, Refusal{Name: t.Name, HeldBy: prev.source})
			continue
		}
		given[t.Name] = true
		r.tools[t.Name] = registered{tool: t, source: source}
		if !held || prev.tool.SchemaHash() != t.SchemaHash() {
			change.Changed = append(change.Changed, t)
		}
	}
	for _, name := range r.sortedNames() {
		if e := r.tools[name]; e.source == source && !given[name] {
			delete(r.tools, name)
			change.Removed = append(change.Removed, name)
		}
	}
	sort.Slice(change.Changed, func(i, j int) bool { return change.Changed[i].Name < change.Changed[j].Name })
	sort.Slice(change.Refused, func(i, j int) bool { return change.Refused[i].Name < change.Refused[j].Name })
	return change
}

func (r *Registry) has(name string) bool {
	_, ok := r.Get(name)
	return ok
}

// heldBy returns the tool under name if source holds it.
func (r *Registry) heldBy(name, source string) (*Tool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.tools[name]
	if !ok || e.source != source {
		return nil, false
	}
	return e.tool, true
}

// sortedNames: the caller holds the lock.
func (r *Registry) sortedNames() []string {
	names := make([]string, 0, len(r.tools))
	for name := range r.tools {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// WorkflowTools returns all tools of kind workflow, for registration at worker startup.
func (r *Registry) WorkflowTools() []*Tool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var tools []*Tool
	for _, e := range r.tools {
		if t := e.tool; t.Kind == ToolKindWorkflow && t.WorkflowFunc != nil {
			tools = append(tools, t)
		}
	}
	return tools
}

// Execute runs a tool. The lock is not held while it runs: a tool removed
// meanwhile finishes its call.
func (r *Registry) Execute(ctx context.Context, name string, input json.RawMessage) (string, error) {
	t, ok := r.Get(name)
	if !ok {
		return "", fmt.Errorf("unknown tool: %s", name)
	}
	return t.Execute(ctx, input)
}
