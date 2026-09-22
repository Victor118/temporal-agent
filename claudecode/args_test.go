package claudecode

import (
	"strings"
	"testing"
)

func argsString(p Params) string { return " " + strings.Join(buildArgs(p), " ") + " " }

func TestBuildArgsAlwaysStreamsAndNeverWaitsOnAPrompt(t *testing.T) {
	got := argsString(Params{})
	for _, want := range []string{
		" -p ",
		" --output-format stream-json ",
		" --verbose ",
		// Nobody is at the terminal: a prompt must be denied, not waited on.
		" --permission-prompts none ",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("args %q missing %q", got, want)
		}
	}
}

func TestBuildArgsOmitsUnsetFlags(t *testing.T) {
	got := argsString(Params{})
	for _, unwanted := range []string{"--model", "--permission-mode", "--allowedTools", "--session-id", "--add-dir"} {
		if strings.Contains(got, unwanted) {
			t.Errorf("args %q should not carry %q when unset", got, unwanted)
		}
	}
}

func TestBuildArgsMapsParams(t *testing.T) {
	got := argsString(Params{
		Model:                "opus",
		PermissionMode:       "plan",
		PermissionPrompts:    "host",
		AllowedTools:         []string{"Read", "Grep"},
		DisallowedTools:      []string{"AskUserQuestion"},
		Tools:                []string{"Read", "Bash"},
		AppendSystemPrompt:   "follow the conventions",
		AddDirs:              []string{"/ref"},
		MCPConfig:            []string{`{"mcpServers":{}}`},
		StrictMCPConfig:      true,
		MaxBudgetUSD:         2.5,
		SessionID:            "0e2f5f4a-0000-4000-8000-000000000000",
		NoSessionPersistence: true,
	})
	for _, want := range []string{
		" --model opus ",
		" --permission-mode plan ",
		" --permission-prompts host ",
		" --allowedTools Read,Grep ",
		" --disallowedTools AskUserQuestion ",
		" --tools Read,Bash ",
		" --append-system-prompt follow the conventions ",
		" --add-dir /ref ",
		" --strict-mcp-config ",
		" --max-budget-usd 2.5 ",
		" --session-id 0e2f5f4a-0000-4000-8000-000000000000 ",
		" --no-session-persistence ",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("args %q missing %q", got, want)
		}
	}
}

func TestTruncateKeepsHeadTailAndValidUTF8(t *testing.T) {
	s := strings.Repeat("é", 2000) // 4000 bytes, multi-byte runes
	out := truncate(s, 300)
	if len(out) <= 300 {
		t.Errorf("truncated length %d: the marker should make it longer than the cap", len(out))
	}
	if !strings.Contains(out, "bytes omitted") {
		t.Error("truncate should say what it dropped")
	}
	if !strings.HasPrefix(out, "é") || !strings.HasSuffix(out, "é") {
		t.Error("cuts should land on rune boundaries")
	}
	if strings.ContainsRune(out, '�') {
		t.Error("truncate produced invalid UTF-8")
	}
	if short := "small"; truncate(short, 300) != short {
		t.Error("truncate should leave a short string alone")
	}
}
