#!/usr/bin/env bash
# PreToolUse guard (Write|Edit|MultiEdit|Bash) — test first, enforced mechanically.
#
# Declared in .claude/settings.json (fires for every session and subagent; agent-frontmatter hooks did
# not fire for spawned agents — .claude/eval/results.md, K1), but ACTS only while a TDD agent runs:
# `tdd` and `tdd-auto` create .claude/state/tdd-active at start and delete it when the list is done.
# A marker older than 12 hours is treated as a crashed run and ignored. The engine's §3 fallback and
# §6 PIN mutations pause the guard by renaming the marker to tdd-paused.
#
# - Write/Edit under src/main/: refused when the code is known GREEN (last tdd-run.sh outcome GREEN, or
#   no run since the agent started) and no test file changed since then.
# - Bash writing under src/: refused (RULE C), judged by scripts/bash-writes.sh. Every Bash call, marker
#   or not, stamps the start time for bash-bypass-detector.sh, so edits made while paused are not
#   reported by the command that re-arms the guard.
# Refusals are appended to .claude/state/guard.log.
#
# While the iteration orchestrator restructures a REFACTOR (.claude/state/refactor-active, same 12-hour
# expiry), any Write/Edit/MultiEdit under src/test/ and any Bash command writing there is refused: a
# refactor keeps the suite unchanged, so a test that must change means the move changes behaviour.
set -uo pipefail

input=$(cat)
cwd=$(jq -r '.cwd // empty' <<<"$input")
ROOT="$(git -C "${cwd:-${CLAUDE_PROJECT_DIR:-$PWD}}" rev-parse --show-toplevel 2>/dev/null)" || exit 0
STATE="$ROOT/.claude/state"

. "$ROOT/.claude/scripts/bash-writes.sh"   # bash_writes, SOURCE_PATH, TEST_PATH (shared with eval-metrics.sh)

if [ "$(jq -r '.tool_name' <<<"$input")" = "Bash" ]; then   # detector reference, marker or not
  mkdir -p "$STATE" && touch "$STATE/bash-start"
fi

refuse() {  # $1 = log line, stdin = message for the agent
  echo "$(date '+%F %T') REFUSED $1" >> "$STATE/guard.log"
  cat >&2
  exit 2
}

if [ -f "$STATE/refactor-active" ] && [ -z "$(find "$STATE/refactor-active" -mmin +720 2>/dev/null)" ]; then
  if [ "$(jq -r '.tool_name' <<<"$input")" = "Bash" ]; then
    command=$(jq -r '.tool_input.command // empty' <<<"$input")
    if bash_writes "$command" "$TEST_PATH"; then
      refuse "(refactor-active) bash-write: $(head -c 120 <<<"$command" | tr '\n' ' ')" <<EOF
Refactor guard: refused Bash command that writes under src/test/.
A REFACTOR never edits a test. If a test must change, the move changes behaviour:
set FAILURE "not a refactor: <test>" and go to FAILURE_GATE.
EOF
    fi
  else
    file=$(jq -r '.tool_input.file_path // empty' <<<"$input")
    case "$file" in
      */src/test/*) refuse "(refactor-active): ${file#"$ROOT"/}" <<EOF
Refactor guard: refused write to ${file#"$ROOT"/}.
A REFACTOR never edits a test. If a test must change, the move changes behaviour:
set FAILURE "not a refactor: <test>" and go to FAILURE_GATE.
EOF
      ;;
    esac
  fi
fi

[ -f "$STATE/tdd-active" ] || exit 0
[ -z "$(find "$STATE/tdd-active" -mmin +720 2>/dev/null)" ] || exit 0

if [ "$(jq -r '.tool_name' <<<"$input")" = "Bash" ]; then
  command=$(jq -r '.tool_input.command // empty' <<<"$input")
  if bash_writes "$command" "$SOURCE_PATH"; then
    refuse "bash-write: $(head -c 120 <<<"$command" | tr '\n' ' ')" <<EOF
TDD guard: refused Bash command that writes under src/ (engine §8, RULE C).
Create and modify source files with the Write/Edit tools only. Reading, searching and tdd-run.sh are fine.
EOF
  fi
  exit 0
fi

file=$(jq -r '.tool_input.file_path // empty' <<<"$input")
case "$file" in
  */src/main/*) ;;
  *) exit 0 ;;
esac

if [ -f "$STATE/last-run" ] && [ "$STATE/last-run" -nt "$STATE/tdd-active" ]; then
  [ "$(head -n1 "$STATE/last-run")" = "GREEN" ] || exit 0   # RED/compile/wiring: scaffold or GREEN allowed
  reference="$STATE/last-run"
else
  reference="$STATE/tdd-active"                               # agent just started: treat as green
fi

changed_test=$(find "$ROOT" \( -name target -o -name .git -o -name node_modules \) -prune -o -path '*/src/test/*' -type f -newer "$reference" -print -quit 2>/dev/null)
[ -n "$changed_test" ] && exit 0

refuse "main-under-green: ${file#"$ROOT"/}" <<EOF
TDD guard: refused write to ${file#"$ROOT"/}.
The code is GREEN and no test file changed since the last green run — write the next test first
(tdd-workflow-engine §1, RULE 0).
EOF
