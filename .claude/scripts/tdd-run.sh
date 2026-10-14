#!/usr/bin/env bash
# Runs tests and prints ONE classified status line plus only the lines that explain it.
#
#   tdd-run.sh <TestClass>   one class — *Test (unit), *IT (integration), *E2ETest (e2e)
#   tdd-run.sh --unit        whole unit suite of the module (boundedcontexts/$TDD_MODULE, default betting; the root project when absent)
#   tdd-run.sh --all         full `mvn verify` (unit + integration + e2e, every module)
#   tdd-run.sh --verify      --all, unless the last run was a GREEN --all on the very same source tree:
#                            then it replays that result without running anything
#   tdd-run.sh --snapshot <label>        records the content of every source file (tracked or not) as the
#                                        start of an iteration, in .claude/state/iteration-base
#   tdd-run.sh --changed-since-snapshot  lists the source files added (A), modified (M) or deleted (D) since
#                                        that snapshot: what the iteration changed, whatever was committed before it
# For a class, the module is the one whose src/test/java holds <TestClass>.java.
# A module-scoped run also builds the reactor modules it depends on (-am), e.g. a shared kernel.
#
# Status: GREEN | RED_ASSERTION | RED_EXCEPTION | COMPILE_ERROR | WIRING_ERROR | NO_TESTS_RAN | INFRA_ERROR
# Only RED_* is a valid RED. Full Maven log: .claude/state/last-run.log
# Records the outcome in .claude/state/last-run (read by hooks/test-first-guard.sh),
# except for INFRA_ERROR and NO_TESTS_RAN, which say nothing about the code. A GREEN --all also records
# its scope, the fingerprint of the source tree it ran on and its status line (lines 2-4), which --verify
# reads; the guard only reads line 1.
set -uo pipefail

ROOT="$(git -C "${CLAUDE_PROJECT_DIR:-$PWD}" rev-parse --show-toplevel 2>/dev/null || pwd)"
MODULE="boundedcontexts/${TDD_MODULE:-betting}"
[ -d "$ROOT/$MODULE" ] || MODULE="."   # no bounded context module: the root project is the module
STATE="$ROOT/.claude/state"
SELF="$ROOT/.claude/scripts/tdd-run.sh"
TARGET="${1:?usage: tdd-run.sh <TestClass> | --unit | --all | --verify | --snapshot <label> | --changed-since-snapshot}"
mkdir -p "$STATE"
cd "$ROOT" || exit 2

tree_fingerprint() {  # content hash of everything a build reads: every module's sources, tests and poms
  source_hashes | shasum -a 256 | cut -d' ' -f1
}

# What an iteration can change: production and test trees of every module, the poms, the documented endpoints.
SOURCE_ROOTS=(':(glob)**/src/main/**' ':(glob)**/src/test/**' ':(glob)**/pom.xml' ':(glob)http/**')

source_hashes() {  # "<hash> <path>" for every tracked or untracked (not ignored) source file on disk, sorted by path
  local paths
  paths="$(git ls-files -co --exclude-standard -- "${SOURCE_ROOTS[@]}" | sort -u | while IFS= read -r path; do
    [ -f "$path" ] && echo "$path"
  done)"
  [ -n "$paths" ] && paste -d' ' <(git hash-object --stdin-paths <<<"$paths") <(echo "$paths")
}

case "$TARGET" in
  --snapshot)
    label="${2:?usage: tdd-run.sh --snapshot <label>}"
    { echo "# $label"; source_hashes; } > "$STATE/iteration-base"
    echo "SNAPSHOT — $(( $(wc -l < "$STATE/iteration-base") - 1 )) source file(s) recorded for $label"
    exit 0 ;;

  --changed-since-snapshot)
    BASE="$STATE/iteration-base"
    if [ ! -f "$BASE" ]; then
      echo "NO_SNAPSHOT — .claude/state/iteration-base is missing: what the iteration changed cannot be told"
      exit 2
    fi
    changes=$(python3 - "$BASE" <(source_hashes) <<'PY'
import sys
def load(path):
    return {l.rstrip("\n").split(" ", 1)[1]: l.split(" ", 1)[0] for l in open(path) if l.strip() and not l.startswith("# ")}
before, after = load(sys.argv[1]), load(sys.argv[2])
for path in sorted(before.keys() | after.keys()):
    if path not in before: print(f"A {path}")
    elif path not in after: print(f"D {path}")
    elif before[path] != after[path]: print(f"M {path}")
PY
)
    label="$(sed -n '1s/^# //p' "$BASE")"
    if [ -z "$changes" ]; then echo "UNCHANGED — no source file changed since the snapshot of $label"; exit 0; fi
    echo "CHANGED — $(grep -c . <<<"$changes") source file(s) since the snapshot of $label"
    sed 's/^/  /' <<<"$changes"
    exit 0 ;;

  --verify)
    if [ -f "$STATE/last-run" ] && [ "$(sed -n 1p "$STATE/last-run")" = GREEN ] \
       && [ "$(sed -n 2p "$STATE/last-run")" = "--all" ] \
       && [ "$(sed -n 3p "$STATE/last-run")" = "$(tree_fingerprint)" ]; then
      # Replayed, never rewritten: advancing last-run's mtime would hide a newer test from the guard.
      echo "$(sed -n 4p "$STATE/last-run") — replayed: unchanged source tree since that run"
      exit 0
    fi
    exec "$SELF" --all ;;
esac

if [[ "$TARGET" != --* ]]; then
  FOUND=$(find . -path ./node_modules -prune -o -path "*/src/test/java/*" -name "$TARGET.java" -print 2>/dev/null | head -1)
  [ -n "$FOUND" ] && { MODULE="${FOUND%%/src/test/java/*}"; MODULE="${MODULE#./}"; }
fi

FINGERPRINT=""
case "$TARGET" in
  --unit)        CMD=(mvn -q -pl "$MODULE" -am test)
                 DIRS=("$MODULE/target/surefire-reports"); GLOB="TEST-*.xml" ;;
  --all)         CMD=(mvn -q verify)
                 FINGERPRINT="$(tree_fingerprint)"
                 # Patterns, expanded after the run: a report directory the build creates must be counted too.
                 DIRS=('target/surefire-reports' 'target/failsafe-reports' '*/target/surefire-reports' '*/target/failsafe-reports' 'boundedcontexts/*/target/surefire-reports' 'boundedcontexts/*/target/failsafe-reports'); GLOB="TEST-*.xml" ;;
  --*)           echo "usage: tdd-run.sh <TestClass> | --unit | --all | --verify | --snapshot <label> | --changed-since-snapshot" >&2
                 exit 2 ;;
  *IT|*E2ETest)  CMD=(mvn -q -pl "$MODULE" -am verify "-Dit.test=$TARGET" -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false -Dfailsafe.failIfNoSpecifiedTests=false)
                 DIRS=("$MODULE/target/failsafe-reports"); GLOB="TEST-*.$TARGET*.xml" ;;
  *)             CMD=(mvn -q -pl "$MODULE" -am test "-Dtest=$TARGET" -Dsurefire.failIfNoSpecifiedTests=false)
                 DIRS=("$MODULE/target/surefire-reports"); GLOB="TEST-*.$TARGET*.xml" ;;
esac

# Stale reports would be read as this run's result.
for pattern in "${DIRS[@]}"; do for d in $pattern; do [ -d "$d" ] && find "$d" -name "$GLOB" -delete; done; done

"${CMD[@]}" >"$STATE/last-run.log" 2>&1
MVN_EXIT=$?

OUTPUT=$(python3 - "$STATE" "$TARGET" "$MVN_EXIT" "$GLOB" "${DIRS[@]}" <<'PY'
import glob, os, re, sys, xml.etree.ElementTree as ET

state, target, mvn_exit, pattern, dirs = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4], sys.argv[5:]
log = open(os.path.join(state, "last-run.log"), errors="replace").read()

INFRA = re.compile(r"Could not find a valid Docker|Cannot connect to the Docker|ContainerLaunchException|"
                   r"OutOfMemoryError|Could not resolve dependencies|Connection refused", re.I)
WIRING = re.compile(r"Failed to load ApplicationContext|NoSuchBeanDefinition|UnsatisfiedDependency|"
                    r"BeanCreationException|NoUniqueBeanDefinition|FlywayValidateException|SchemaManagementException")

def finish(status, headline, details=()):
    print(f"{status} — {headline}")
    for line in list(details)[:12]:
        print(f"  {line}")
    if status not in ("INFRA_ERROR", "NO_TESTS_RAN"):
        with open(os.path.join(state, "last-run"), "w") as f:
            f.write(status + "\n")
    sys.exit(0 if status == "GREEN" else 1 if status.startswith("RED_") else 2)

compile_errors = list(dict.fromkeys(re.sub(r"^\[ERROR\]\s*\S*/src/", "src/", l) for l in log.splitlines()
                                    if re.search(r"\.java:\[\d+", l)))
if "COMPILATION ERROR" in log or compile_errors:
    finish("COMPILE_ERROR", f"{len(compile_errors)} compilation error(s)", compile_errors)

reports = [p for d in dirs for p in glob.glob(os.path.join(d, pattern))]
if not reports:
    noise = re.compile(r"^\[ERROR\]\s*(\[Help|To see the full|Re-run Maven|For more information|$)")
    tail = [l for l in log.splitlines() if l.startswith("[ERROR]") and not noise.match(l)][-6:]
    if INFRA.search(log):
        finish("INFRA_ERROR", "infrastructure failure, not a test result", tail)
    if mvn_exit == 0 or re.search(r"No tests (matching|were executed)", log):
        finish("NO_TESTS_RAN", f"no test report for {target} — check the class name suffix (*Test, *IT, *E2ETest)", tail)
    finish("INFRA_ERROR", "build failed before any test ran", tail)

tests = failures = errors = 0
failed, kinds = [], set()
for path in reports:
    suite = ET.parse(path).getroot()
    for case in suite.iter("testcase"):
        tests += 1
        for tag in ("failure", "error"):
            node = case.find(tag)
            if node is None:
                continue
            text = f"{node.get('type', '')} {node.get('message', '')} {node.text or ''}"
            if INFRA.search(text):
                kinds.add("INFRA_ERROR")
            elif WIRING.search(text):
                kinds.add("WIRING_ERROR")
            elif tag == "failure":
                kinds.add("RED_ASSERTION")
            else:
                kinds.add("RED_EXCEPTION")
            failures += tag == "failure"
            errors += tag == "error"
            message = (node.get("message") or (node.text or "").strip().splitlines()[0:1] or [""])
            message = message if isinstance(message, str) else message[0]
            short_type = node.get("type", "").rsplit(".", 1)[-1]
            failed.append(f"✗ {case.get('classname', '').rsplit('.', 1)[-1]}.{case.get('name')}: "
                          f"{short_type}: {' '.join(message.split())[:220]}")

headline = f"{tests} test(s), {failures} failed, {errors} error(s) — {target}"
if tests == 0:
    finish("NO_TESTS_RAN", headline)
for status in ("INFRA_ERROR", "WIRING_ERROR", "RED_ASSERTION", "RED_EXCEPTION"):
    if status in kinds:
        finish(status, headline, failed)
finish("GREEN", headline)
PY
)
CODE=$?
echo "$OUTPUT"
if [ "$TARGET" = --all ] && [ $CODE -eq 0 ]; then   # what --verify may replay
  printf '%s\n' GREEN "--all" "$FINGERPRINT" "$(head -n1 <<<"$OUTPUT")" > "$STATE/last-run"
fi
exit $CODE
