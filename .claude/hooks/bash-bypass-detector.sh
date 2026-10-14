#!/usr/bin/env bash
# PostToolUse (Bash) — detects what the PreToolUse judgement missed: a file under src/ that changed during
# a Bash command while a TDD agent runs, or a file under src/test/ that changed while the orchestrator
# restructures a REFACTOR (refactor-active). Reports it to the agent (exit 2 shows stderr) and logs a
# BYPASS. Build output (target/) is ignored.
set -uo pipefail

input=$(cat)
cwd=$(jq -r '.cwd // empty' <<<"$input")
ROOT="$(git -C "${cwd:-${CLAUDE_PROJECT_DIR:-$PWD}}" rev-parse --show-toplevel 2>/dev/null)" || exit 0
STATE="$ROOT/.claude/state"

[ -f "$STATE/bash-start" ] || exit 0

changed_under() {  # $1 = find -path pattern → up to 5 files changed since the Bash command started
  find "$ROOT" \( -name target -o -name .git -o -name node_modules \) -prune -o -path "$1" -type f \
       -newer "$STATE/bash-start" -print 2>/dev/null | sed "s#^$ROOT/##" | head -5
}

if [ -f "$STATE/refactor-active" ] && [ -z "$(find "$STATE/refactor-active" -mmin +720 2>/dev/null)" ]; then
  tests=$(changed_under '*/src/test/*')
  if [ -n "$tests" ]; then
    echo "$(date '+%F %T') BYPASS (refactor-active) $(tr '\n' ' ' <<<"$tests")" >> "$STATE/guard.log"
    cat >&2 <<EOF
Refactor guard: a Bash command changed test files during a REFACTOR:
$tests
A REFACTOR never edits a test. Report it, set FAILURE "not a refactor: <test>" and go to FAILURE_GATE. This bypass is logged.
EOF
    exit 2
  fi
fi

[ -f "$STATE/tdd-active" ] || exit 0

changed=$(changed_under '*/src/*')
[ -z "$changed" ] && exit 0

echo "$(date '+%F %T') BYPASS $(tr '\n' ' ' <<<"$changed")" >> "$STATE/guard.log"
cat >&2 <<EOF
TDD guard: a Bash command changed source files, bypassing the test-first check (RULE C):
$changed
Redo these changes with Write/Edit, following the TDD sequence. This bypass is logged.
EOF
exit 2
