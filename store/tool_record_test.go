package store

import (
	"testing"

	"github.com/victor/temporal-agent/access"
)

// An empty mcp_server is a built-in tool; otherwise the tool is its server's.
func TestToolRecord_AccessTool(t *testing.T) {
	if got := (ToolRecord{Name: "exec"}).AccessTool(); got != access.BuiltIn("exec") {
		t.Errorf("exec: %+v", got)
	}
	if got := (ToolRecord{Name: "crm_search", MCPServer: "crm"}).AccessTool(); got != access.FromMCP("crm", "crm_search") {
		t.Errorf("crm_search: %+v", got)
	}
}
