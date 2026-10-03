package activity

import (
	"context"
	"encoding/json"
	"testing"

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
