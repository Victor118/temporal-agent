package claudecode

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

func TestBuildArgsPluginDirs(t *testing.T) {
	got := argsString(Params{PluginDirs: []string{"/run/plugin", "/other"}})
	if !strings.Contains(got, " --plugin-dir /run/plugin --plugin-dir /other ") {
		t.Errorf("args %q", got)
	}
	if strings.Contains(argsString(Params{}), "--plugin-dir") {
		t.Error("--plugin-dir without a plugin")
	}
}

func TestRunReadsSlashCommands(t *testing.T) {
	res, err := run(t, `cat <<'EOF'
{"type":"system","subtype":"init","session_id":"s","slash_commands":["compact","temporal-agent:tdd"],"skills":["temporal-agent:tdd"]}
{"type":"result","subtype":"success","is_error":false,"result":"ok","session_id":"s"}
EOF
`, Params{})
	if err != nil || !slices.Equal(res.SlashCommands, []string{"compact", "temporal-agent:tdd"}) {
		t.Errorf("%v %v", res.SlashCommands, err)
	}
}

func TestSupportsFlag(t *testing.T) {
	newer := &Runner{Binary: fakeCLI(t, `[ "$1" = --help ] || exit 2
cat <<'EOF'
Usage: claude [options] [command] [prompt]
  --plugin-dir <path>                   Load a plugin from a directory or .zip
  --plugin-dir-no-mcp <path>            Like --plugin-dir
EOF
`)}
	older := &Runner{Binary: fakeCLI(t, `echo "  --plugin-dir-no-mcp <path>   hidden"; echo "  --add-dir <directories...>"`)}
	broken := &Runner{Binary: fakeCLI(t, `echo "--plugin-dir"; exit 1`)}
	ctx := context.Background()
	if !newer.SupportsFlag(ctx, PluginDirFlag) {
		t.Error("newer: no --plugin-dir")
	}
	if older.SupportsFlag(ctx, PluginDirFlag) {
		t.Error("older: --plugin-dir found in --plugin-dir-no-mcp")
	}
	if broken.SupportsFlag(ctx, PluginDirFlag) {
		t.Error("a CLI that failed: supported")
	}
	if (&Runner{Binary: "/nonexistent/claude"}).SupportsFlag(ctx, PluginDirFlag) {
		t.Error("no CLI: supported")
	}
}

func TestPluginRefusal(t *testing.T) {
	line := "--plugin-dir is disabled by your organization's managed settings (disableSideloadFlags). Plugins, custom agents, and MCP servers can only be loaded from sources your administrator has approved."
	res, err := run(t, `echo "`+line+`" >&2; exit 1`, Params{})
	if got := PluginRefusal(res, err); got != line {
		t.Errorf("refusal %q", got)
	}
	// A run that reported is judged by its report, not by what it printed.
	reported := Result{Subtype: "success", Stderr: line}
	if got := PluginRefusal(reported, nil); got != "" {
		t.Errorf("a reported run: %q", got)
	}
	if got := PluginRefusal(Result{Stderr: "Invalid API key"}, errors.New("exit 1")); got != "" {
		t.Errorf("another failure: %q", got)
	}
}
