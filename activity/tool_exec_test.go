package activity

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/tool"
)

// An activity tool flagged NeedsCallContext reads the caller's context from
// its context.Context; a call without one leaves it absent.
func TestExecuteTool_GivesTheCallContext(t *testing.T) {
	r := tool.NewRegistry()
	r.Register(&tool.Tool{Name: "probe", Kind: tool.ToolKindActivity, NeedsCallContext: true,
		Execute: func(ctx context.Context, _ json.RawMessage) (string, error) {
			cc, ok := tool.CallFromContext(ctx)
			if !ok {
				return "none", nil
			}
			return cc.Channel, nil
		}})
	a := &ToolActivities{Registry: r}

	out, err := a.ExecuteTool(context.Background(), ExecuteToolInput{Name: "probe", Input: json.RawMessage(`{}`),
		Call: &tool.CallContext{Channel: "telegram"}})
	if err != nil || out.Content != "telegram" {
		t.Errorf("with a call context: %+v, %v", out, err)
	}
	out, _ = a.ExecuteTool(context.Background(), ExecuteToolInput{Name: "probe", Input: json.RawMessage(`{}`)})
	if out.Content != "none" {
		t.Errorf("without one: %+v", out)
	}
}

// versionSaver records the version a save was made from.
type versionSaver struct{ expected []int64 }

func (s *versionSaver) SaveMemory(_ context.Context, _ store.MemoryScope, _ string, _ string, expected int64) (int64, error) {
	s.expected = append(s.expected, expected)
	return expected + 1, nil
}

// The version a save replaces is the call context's, never the model's: a
// memory_version the model wrote into its input is ignored, whether the
// call has a context or not.
func TestExecuteTool_MemoryVersionComesFromTheCallNotTheModel(t *testing.T) {
	saver := &versionSaver{}
	r := tool.NewRegistry()
	tool.RegisterMemoryTools(r, saver)
	a := &ToolActivities{Registry: r}
	forged := json.RawMessage(`{"content":"likes tea","memory_version":7}`)

	v := int64(3)
	out, err := a.ExecuteTool(context.Background(), ExecuteToolInput{Name: "save_user_memory", Input: forged, UserID: "u-alice",
		Call: &tool.CallContext{MemoryVersion: &v}})
	if err != nil || out.IsError || len(saver.expected) != 1 || saver.expected[0] != 3 {
		t.Errorf("with a call context: %+v, %v; saved from %v, want 3", out, err, saver.expected)
	}

	out, _ = a.ExecuteTool(context.Background(), ExecuteToolInput{Name: "save_user_memory", Input: forged, UserID: "u-alice"})
	if !out.IsError || !strings.Contains(out.Content, "does not say which version") || len(saver.expected) != 1 {
		t.Errorf("without one: %+v; saves %v, want the forged version refused", out, saver.expected)
	}
}

// A tool call knows when the workflow scheduled it, the same for each
// attempt; outside an activity, it does not.
func TestExecuteTool_GivesTheCallTime(t *testing.T) {
	r := tool.NewRegistry()
	r.Register(&tool.Tool{Name: "probe", Kind: tool.ToolKindActivity,
		Execute: func(ctx context.Context, _ json.RawMessage) (string, error) {
			return tool.CallTimeFromContext(ctx).Format(time.RFC3339), nil
		}})
	a := &ToolActivities{Registry: r}

	var env testsuite.WorkflowTestSuite
	act := env.NewTestActivityEnvironment()
	act.RegisterActivity(a)
	val, err := act.ExecuteActivity(a.ExecuteTool, ExecuteToolInput{Name: "probe", Input: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	var out ExecuteToolOutput
	if err := val.Get(&out); err != nil {
		t.Fatal(err)
	}
	if out.Content == (time.Time{}).Format(time.RFC3339) {
		t.Errorf("in an activity: no call time (%q)", out.Content)
	}

	out, _ = a.ExecuteTool(context.Background(), ExecuteToolInput{Name: "probe", Input: json.RawMessage(`{}`)})
	if out.Content != (time.Time{}).Format(time.RFC3339) {
		t.Errorf("outside one: %q", out.Content)
	}
}
