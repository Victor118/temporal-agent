#!/usr/bin/env bash
# Deterministic self-test of the mechanics the orchestrator relies on, no agent and no Maven involved (seconds):
#   - tdd-run.sh --snapshot / --changed-since-snapshot: a change made before the snapshot is not listed;
#     a file added, modified or deleted after it is;
#   - test-first-guard.sh with refactor-active: writes under src/test/ (Write/Edit, Bash) refused,
#     production edits and tdd-run.sh accepted; nothing refused without the marker;
#   - bash-bypass-detector.sh with refactor-active: a test file changed by Bash is reported;
#   - test-first-guard.sh with tdd-active: Bash source writes refused, read-only commands naming a source
#     path accepted; re-arming a paused guard reports no bypass.
# Run after editing tdd-run.sh or a hook. Uses untracked probe files only; restores guard.log and state.
set -uo pipefail
ROOT="$(git -C "${CLAUDE_PROJECT_DIR:-$PWD}" rev-parse --show-toplevel)"
cd "$ROOT" || exit 2
STATE=.claude/state
mkdir -p "$STATE"
for busy in tdd-active tdd-paused refactor-active iteration-base; do
  [ -e "$STATE/$busy" ] && { echo "SKIPPED — $STATE/$busy exists: an iteration or a TDD agent is running"; exit 2; }
done

BASE=boundedcontexts/betting/; PKG=com/wealcome/betting
[ -d "$BASE" ] || { BASE=; PKG=com/wealcome; }   # no bounded context module: probe the root project
MAIN=${BASE}src/main/java/$PKG/ZzMechanicsProbe
TEST=${BASE}src/test/java/$PKG/unit/ZzMechanicsProbeTest.java
IT=${BASE}src/test/java/$PKG/integration/ZzMechanicsProbeIT.java
LOG_SIZE=$( [ -f "$STATE/guard.log" ] && wc -c < "$STATE/guard.log" || echo none)
cleanup() {
  rm -f "$MAIN.java" "${MAIN}Gone.java" "$TEST" "$IT" "$STATE/refactor-active" "$STATE/iteration-base" "$STATE/tdd-active" "$STATE/tdd-paused"
  if [ "$LOG_SIZE" = none ]; then rm -f "$STATE/guard.log"; else head -c "$LOG_SIZE" "$STATE/guard.log" > "$STATE/guard.log.tmp" && mv "$STATE/guard.log.tmp" "$STATE/guard.log"; fi
}
trap cleanup EXIT
failures=0
check() {  # $1 = label, $2 = expected, $3 = actual
  if [ "$2" = "$3" ]; then echo "  ✓ $1"; else echo "  ✗ $1 — expected $2, got $3"; failures=$((failures + 1)); fi
}

echo "snapshot"
echo "// leftover" > "$MAIN.java"; echo "// gone" > "${MAIN}Gone.java"
.claude/scripts/tdd-run.sh --snapshot "mechanics" >/dev/null
out=$(.claude/scripts/tdd-run.sh --changed-since-snapshot)
check "nothing listed right after the snapshot" UNCHANGED "$(cut -d' ' -f1 <<<"$out")"
echo "// iteration" >> "$MAIN.java"; echo "// new" > "$TEST"; rm "${MAIN}Gone.java"
out=$(.claude/scripts/tdd-run.sh --changed-since-snapshot)
check "added test listed" 1 "$(grep -c "A $TEST" <<<"$out")"
check "modified file listed" 1 "$(grep -c "M $MAIN.java" <<<"$out")"
check "deleted file listed" 1 "$(grep -c "D ${MAIN}Gone.java" <<<"$out")"
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
check "Write of a test resource refused" 2 "$(guard Write file_path "$ROOT/${BASE}src/test/resources/zz-probe.yml")"
check "Bash sed -i on a test refused" 2 "$(guard Bash command "sed -i '' s/120/119/ $TEST")"
check "Edit of production accepted" 0 "$(guard Edit file_path "$ROOT/$MAIN.java")"
check "tdd-run.sh on a test class accepted" 0 "$(guard Bash command ".claude/scripts/tdd-run.sh ZzMechanicsProbeTest")"
check "config heredoc to /tmp naming src/test, next to a removal, accepted" 0 "$(guard Bash command "rm -rf target/pit-reports && cat > /tmp/probe.properties <<'EOF'
tests=boundedcontexts/betting/src/test/java
EOF
mvn -q -pl boundedcontexts/betting -Ppitest test")"
check "heredoc into a test refused" 2 "$(guard Bash command "cat > $TEST <<'EOF'
x
EOF")"
check "refusals logged" 5 "$(tail -c +$(( ${LOG_SIZE/none/0} + 1 )) "$STATE/guard.log" | grep -c 'REFUSED (refactor-active)')"

echo "bypass detector"
touch "$STATE/bash-start"; sleep 1; touch "$TEST"
printf '{"cwd":"%s"}' "$ROOT" | .claude/hooks/bash-bypass-detector.sh >/dev/null 2>&1
check "test changed by Bash reported" 2 "$?"
rm "$STATE/refactor-active"
check "without the marker, Edit of a test accepted" 0 "$(guard Edit file_path "$ROOT/$TEST")"

echo "test-first guard, Bash"
touch "$STATE/tdd-active"
check "echo into a source file refused" 2 "$(guard Bash command "echo x > $MAIN.java")"
check "heredoc into a source file refused" 2 "$(guard Bash command "cat > $MAIN.java <<'EOF'
x
EOF")"
check "redirect into a variable naming a source file refused" 2 "$(guard Bash command "F=$MAIN.java; echo x > \$F")"
check "python heredoc writing a source file refused" 2 "$(guard Bash command "python3 - <<'EOF'
p='$MAIN.java'
open(p,'w').write('x')
EOF")"
check "git checkout of a source file refused" 2 "$(guard Bash command "git checkout -- $MAIN.java")"
check "git diff of a source file redirected to /tmp accepted" 0 "$(guard Bash command "git diff --stat -- $MAIN.java > /tmp/probe.txt")"
check "git diff of a source file piped to tee /tmp accepted" 0 "$(guard Bash command "git diff -- $MAIN.java | tee /tmp/probe.txt")"
check "guard pause, then git diff of a source file, accepted" 0 "$(guard Bash command "mv $STATE/tdd-active $STATE/tdd-paused && git diff -- $MAIN.java")"
check "Edit of production under green with no test changed refused" 2 "$(touch "$STATE/tdd-active"; guard Edit file_path "$ROOT/$MAIN.java")"

echo "bypass detector after a guard pause"
mv "$STATE/tdd-active" "$STATE/tdd-paused"
touch "$STATE/bash-start"; sleep 1; touch "$MAIN.java"; sleep 1   # an Edit made while paused
guard Bash command "mv $STATE/tdd-paused $STATE/tdd-active" >/dev/null
mv "$STATE/tdd-paused" "$STATE/tdd-active"
printf '{"cwd":"%s"}' "$ROOT" | .claude/hooks/bash-bypass-detector.sh >/dev/null 2>&1
check "re-arming the guard reports no bypass" 0 "$?"
rm -f "$STATE/tdd-active"

[ "$failures" -eq 0 ] && echo "GREEN — orchestrator mechanics" && exit 0
echo "RED — $failures mechanics check(s) failed"
exit 1
