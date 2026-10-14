#!/usr/bin/env bash
# Deterministic self-test of the mechanics the orchestrator relies on, no agent and no Go run (seconds):
#   - tdd-run.sh --snapshot / --changed-since-snapshot: a change made before the snapshot is not listed;
#     a file added, modified or deleted after it is;
#   - test-first-guard.sh with refactor-active: writes of test files (*_test.go, testdata/; Write/Edit, Bash)
#     refused, production edits and tdd-run.sh accepted; nothing refused without the marker;
#   - bash-bypass-detector.sh with refactor-active: a test file changed by Bash is reported;
#   - test-first-guard.sh with tdd-active: Bash writes of Go files refused (gofmt -w and go fmt included),
#     read-only commands naming a Go file accepted; a production edit under green refused until a test
#     changes; re-arming a paused guard reports no bypass.
# Run after editing tdd-run.sh or a hook. Uses untracked probe files only, in ./zzmechanicsprobe/ (removed at
# the end); restores guard.log and state.
set -uo pipefail
ROOT="$(git -C "${CLAUDE_PROJECT_DIR:-$PWD}" rev-parse --show-toplevel)"
cd "$ROOT" || exit 2
STATE=.claude/state
mkdir -p "$STATE"
for busy in tdd-active tdd-paused refactor-active iteration-base; do
  [ -e "$STATE/$busy" ] && { echo "SKIPPED — $STATE/$busy exists: an iteration or a TDD agent is running"; exit 2; }
done

PROBE=zzmechanicsprobe
[ -e "$PROBE" ] && { echo "SKIPPED — ./$PROBE exists: remove it first"; exit 2; }
mkdir -p "$PROBE/usecase" "$PROBE/adapters/postgres"
MAIN=$PROBE/usecase/probe                         # .go appended below
TEST=$PROBE/usecase/probe_test.go
IT=$PROBE/adapters/postgres/probe_test.go
LOG_SIZE=$( [ -f "$STATE/guard.log" ] && wc -c < "$STATE/guard.log" || echo none)
cleanup() {
  rm -rf "$PROBE"
  rm -f "$STATE/refactor-active" "$STATE/iteration-base" "$STATE/tdd-active" "$STATE/tdd-paused"
  if [ "$LOG_SIZE" = none ]; then rm -f "$STATE/guard.log"; else head -c "$LOG_SIZE" "$STATE/guard.log" > "$STATE/guard.log.tmp" && mv "$STATE/guard.log.tmp" "$STATE/guard.log"; fi
}
trap cleanup EXIT
failures=0
check() {  # $1 = label, $2 = expected, $3 = actual
  if [ "$2" = "$3" ]; then echo "  ✓ $1"; else echo "  ✗ $1 — expected $2, got $3"; failures=$((failures + 1)); fi
}

echo "snapshot"
echo "// leftover" > "$MAIN.go"; echo "// gone" > "${MAIN}_gone.go"
.claude/scripts/tdd-run.sh --snapshot "mechanics" >/dev/null
out=$(.claude/scripts/tdd-run.sh --changed-since-snapshot)
check "nothing listed right after the snapshot" UNCHANGED "$(cut -d' ' -f1 <<<"$out")"
echo "// iteration" >> "$MAIN.go"; echo "// new" > "$TEST"; rm "${MAIN}_gone.go"
out=$(.claude/scripts/tdd-run.sh --changed-since-snapshot)
check "added test listed" 1 "$(grep -c "A $TEST" <<<"$out")"
check "modified file listed" 1 "$(grep -c "M $MAIN.go" <<<"$out")"
check "deleted file listed" 1 "$(grep -c "D ${MAIN}_gone.go" <<<"$out")"
rm -f "$STATE/iteration-base"
check "no snapshot answers NO_SNAPSHOT" NO_SNAPSHOT "$(.claude/scripts/tdd-run.sh --changed-since-snapshot | cut -d' ' -f1)"

guard() {  # $1 = tool, $2 = key, $3 = value → exit code of the guard
  jq -nc --arg t "$1" --arg k "$2" --arg v "$3" --arg c "$ROOT" '{tool_name:$t, cwd:$c, tool_input:{($k):$v}}' \
    | .claude/hooks/test-first-guard.sh >/dev/null 2>&1
  echo $?
}
echo "refactor guard"
touch "$STATE/refactor-active"
check "Edit of a unit test refused" 2 "$(guard Edit file_path "$ROOT/$TEST")"
check "Write of an integration test refused" 2 "$(guard Write file_path "$ROOT/$IT")"
check "Write of a testdata file refused" 2 "$(guard Write file_path "$ROOT/$PROBE/usecase/testdata/zz-probe.json")"
check "Bash sed -i on a test refused" 2 "$(guard Bash command "sed -i s/120/119/ $TEST")"
check "Edit of production accepted" 0 "$(guard Edit file_path "$ROOT/$MAIN.go")"
check "tdd-run.sh on a test accepted" 0 "$(guard Bash command ".claude/scripts/tdd-run.sh ./$PROBE/usecase TestProbe_Works")"
check "note heredoc to /tmp naming a test file, next to a removal, accepted" 0 "$(guard Bash command "rm -rf /tmp/probe-cover && cat > /tmp/probe.txt <<'NOTE'
tests=$TEST
NOTE
.claude/scripts/tdd-run.sh --unit")"
check "heredoc into a test refused" 2 "$(guard Bash command "cat > $TEST <<'NOTE'
x
NOTE")"
check "refusals logged" 5 "$(tail -c +$(( ${LOG_SIZE/none/0} + 1 )) "$STATE/guard.log" | grep -c 'REFUSED (refactor-active)')"

echo "bypass detector"
touch "$STATE/bash-start"; sleep 1; touch "$TEST"
printf '{"cwd":"%s"}' "$ROOT" | .claude/hooks/bash-bypass-detector.sh >/dev/null 2>&1
check "test changed by Bash reported" 2 "$?"
rm "$STATE/refactor-active"
check "without the marker, Edit of a test accepted" 0 "$(guard Edit file_path "$ROOT/$TEST")"

echo "test-first guard"
touch "$STATE/tdd-active"
check "echo into a source file refused" 2 "$(guard Bash command "echo x > $MAIN.go")"
check "heredoc into a source file refused" 2 "$(guard Bash command "cat > $MAIN.go <<'NOTE'
x
NOTE")"
check "redirect into a variable naming a source file refused" 2 "$(guard Bash command "F=$MAIN.go; echo x > \$F")"
check "python heredoc writing a source file refused" 2 "$(guard Bash command "python3 - <<'NOTE'
p='$MAIN.go'
open(p,'w').write('x')
NOTE")"
check "git checkout of a source file refused" 2 "$(guard Bash command "git checkout -- $MAIN.go")"
check "gofmt -w in the container refused" 2 "$(guard Bash command "docker exec -w /app agent-dev gofmt -w $MAIN.go")"
check "go fmt of a package refused" 2 "$(guard Bash command "docker exec -w /app agent-dev go fmt ./$PROBE/...")"
check "git diff of a source file redirected to /tmp accepted" 0 "$(guard Bash command "git diff --stat -- $MAIN.go > /tmp/probe.txt")"
check "git diff of a source file piped to tee /tmp accepted" 0 "$(guard Bash command "git diff -- $MAIN.go | tee /tmp/probe.txt")"
check "guard pause, then git diff of a source file, accepted" 0 "$(guard Bash command "mv $STATE/tdd-active $STATE/tdd-paused && git diff -- $MAIN.go")"
touch "$STATE/tdd-active"; sleep 1
check "Edit of production under green with no test changed refused" 2 "$(guard Edit file_path "$ROOT/$MAIN.go")"
check "Edit of a test under green accepted" 0 "$(guard Edit file_path "$ROOT/$TEST")"
check "Edit of a non-Go file under green accepted" 0 "$(guard Edit file_path "$ROOT/$PROBE/notes.txt")"
touch "$TEST"
check "Edit of production after a test changed accepted" 0 "$(guard Edit file_path "$ROOT/$MAIN.go")"

echo "bypass detector after a guard pause"
mv "$STATE/tdd-active" "$STATE/tdd-paused"
touch "$STATE/bash-start"; sleep 1; touch "$MAIN.go"; sleep 1   # an Edit made while paused
guard Bash command "mv $STATE/tdd-paused $STATE/tdd-active" >/dev/null
mv "$STATE/tdd-paused" "$STATE/tdd-active"
printf '{"cwd":"%s"}' "$ROOT" | .claude/hooks/bash-bypass-detector.sh >/dev/null 2>&1
check "re-arming the guard reports no bypass" 0 "$?"
touch "$STATE/bash-start"; sleep 1; touch "$MAIN.go"
printf '{"cwd":"%s"}' "$ROOT" | .claude/hooks/bash-bypass-detector.sh >/dev/null 2>&1
check "a Go file changed by Bash while armed reported" 2 "$?"
rm -f "$STATE/tdd-active"

[ "$failures" -eq 0 ] && echo "GREEN — orchestrator mechanics" && exit 0
echo "RED — $failures mechanics check(s) failed"
exit 1
