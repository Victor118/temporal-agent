#!/usr/bin/env bash
# Runs Go tests and prints ONE classified status line plus only the lines that explain it.
#
#   tdd-run.sh <package> [<Test>[/<subtest>]]   one package, or one test of it: tdd-run.sh ./access/usecase TestOpenTools_ClosedToolsLeft
#                                               (the test name is anchored: ^TestX$, each /subtest part too)
#   tdd-run.sh <Test>                           the same, the package found by the test's `func <Test>(` declaration
#   tdd-run.sh --unit                           unit suite of the module $TDD_MODULE (default access): every package
#                                               under ./$TDD_MODULE/ except those under adapters/ (domain, usecase…)
#   tdd-run.sh --all                            go vet ./... then go test ./... (whole repository; the store and
#                                               adapter tests that need a database skip without TEST_DATABASE_URL)
#   tdd-run.sh --verify                         --all, unless the last run was a GREEN --all on the very same source tree:
#                                               then it replays that result without running anything
#   tdd-run.sh --snapshot <label>               records the content of every source file (tracked or not) as the
#                                               start of an iteration, in .claude/state/iteration-base
#   tdd-run.sh --changed-since-snapshot         lists the source files added (A), modified (M) or deleted (D) since
#                                               that snapshot: what the iteration changed, whatever was committed before it
#
# Where Go runs (Go is not installed on the host): `go` if it is in the PATH (devcontainer); otherwise
# `docker exec agent-dev go …` when the agent-dev container runs with this repository mounted on /app;
# otherwise `docker compose -f ../docker-compose.build.yml run --rm -T agent go …` (slower: a fresh container).
# TDD_GO overrides it with a whole command prefix. TEST_DATABASE_URL and TEMPORAL_SMOKE_HOST, when set, are
# passed into the container.
#
# Integration tests on a throwaway database (never the `agent` database), from the parent directory, with the
# compose stack up (CLAUDE.md, « Build & Run »):
#   docker compose exec -T postgres createdb -U agent agent_dev_tmp
#   TDD_GO="docker compose -f ../docker-compose.yml exec -T -e TEST_DATABASE_URL=postgres://agent:agent@postgres:5432/agent_dev_tmp?sslmode=disable agent go" \
#     .claude/scripts/tdd-run.sh ./access/adapters/postgres
#   docker compose exec -T postgres dropdb -U agent agent_dev_tmp
#
# Status: GREEN | RED_ASSERTION | RED_EXCEPTION | COMPILE_ERROR | WIRING_ERROR | NO_TESTS_RAN | INFRA_ERROR
#   RED_ASSERTION  a test failed (t.Error, t.Fatal…)          RED_EXCEPTION  a test panicked
#   COMPILE_ERROR  build failed, or go vet found a problem    WIRING_ERROR   the test binary failed outside any test
#                                                                            (TestMain, init, a package-level setup)
#   NO_TESTS_RAN   no package, no test file, or no test matches the name
#   INFRA_ERROR    Go, Docker or a database unreachable, a timeout (a hang is not a RED), or a target whose tests
#                  all skipped (they need TEST_DATABASE_URL, TEMPORAL_SMOKE_HOST…): not a test result
# Only RED_* is a valid RED. Full log (go test -json): .claude/state/last-run.log
# Records the outcome in .claude/state/last-run (read by hooks/test-first-guard.sh),
# except for INFRA_ERROR and NO_TESTS_RAN, which say nothing about the code. A GREEN --all also records
# its scope, the fingerprint of the source tree it ran on and its status line (lines 2-4), which --verify
# reads; the guard only reads line 1.
set -uo pipefail

ROOT="$(git -C "${CLAUDE_PROJECT_DIR:-$PWD}" rev-parse --show-toplevel 2>/dev/null || pwd)"
MODULE="${TDD_MODULE:-access}"
STATE="$ROOT/.claude/state"
SELF="$ROOT/.claude/scripts/tdd-run.sh"
USAGE="usage: tdd-run.sh <package> [<Test>] | <Test> | --unit | --all | --verify | --snapshot <label> | --changed-since-snapshot"
TARGET="${1:?$USAGE}"
mkdir -p "$STATE"
cd "$ROOT" || exit 2

tree_fingerprint() {  # content hash of everything a build reads
  source_hashes | shasum -a 256 | cut -d' ' -f1
}

# What an iteration can change: every file of the repository but the kit, the agents' workspace and the docs
# (Go sources and tests, go.mod / go.sum, embedded templates and assets, YAML configuration).
SOURCE_ROOTS=(. ':(exclude).claude' ':(exclude)workspace' ':(exclude)docs' ':(exclude,glob)**/*.md')

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

# Statuses that need no Go run: printed the same way as the classifier's.
early() {  # $1 = status, $2 = headline, rest = details
  echo "$1 — $2"; shift 2
  for line in "$@"; do echo "  $line"; done
  exit 2
}

# --- Where Go runs ------------------------------------------------------------------------------------------
PASS_ENV=()
for var in TEST_DATABASE_URL TEMPORAL_SMOKE_HOST; do
  [ -n "${!var:-}" ] && PASS_ENV+=(-e "$var")
done
if [ -n "${TDD_GO:-}" ]; then
  read -r -a GO <<<"$TDD_GO"
elif command -v go >/dev/null 2>&1; then
  GO=(go)
elif [ "$(docker inspect agent-dev --format '{{.State.Running}} {{range .Mounts}}{{if eq .Destination "/app"}}{{.Source}}{{end}}{{end}}' 2>/dev/null)" = "true $ROOT" ]; then
  GO=(docker exec -w /app "${PASS_ENV[@]}" agent-dev go)
elif [ -f "$ROOT/../docker-compose.build.yml" ] && [ "$(basename "$ROOT")" = agent ] && command -v docker >/dev/null 2>&1; then
  GO=(docker compose -f "$ROOT/../docker-compose.build.yml" run --rm -T -w /app "${PASS_ENV[@]}" agent go)
else
  early INFRA_ERROR "no Go: not in the PATH, no agent-dev container running on $ROOT, no ../docker-compose.build.yml" \
    "start it: (cd .. && docker compose -f docker-compose.build.yml up -d), or set TDD_GO"
fi

# --- What to run --------------------------------------------------------------------------------------------
anchored() {  # TestX/row name → ^TestX$/^row_name$ (go test -run splits on /, subtest names have _ for spaces)
  local part out=""
  IFS='/' read -r -a parts <<<"$1"
  for part in "${parts[@]}"; do
    part=$(sed -e 's/ /_/g' -e 's/[][\.^$*+?(){}|]/\\&/g' <<<"$part")
    out="${out:+$out/}^$part\$"
  done
  echo "$out"
}

TIMEOUT="${TDD_TIMEOUT:-5m}"
VET=""
case "$TARGET" in
  --unit)
    [ -d "$MODULE" ] || early INFRA_ERROR "no module ./$MODULE (TDD_MODULE)"
    mapfile -t PKGS < <("${GO[@]}" list -e "./$MODULE/..." 2>>"$STATE/last-run.list" | grep -v '/adapters/')
    [ "${#PKGS[@]}" -gt 0 ] || early NO_TESTS_RAN "no package under ./$MODULE outside adapters/" "$(tail -n 3 "$STATE/last-run.list" 2>/dev/null)"
    rm -f "$STATE/last-run.list"
    ARGS=("${PKGS[@]}") ;;
  --all)
    TIMEOUT="${TDD_TIMEOUT:-20m}"
    VET=yes
    ARGS=(./...) ;;
  --*)
    echo "$USAGE" >&2
    exit 2 ;;
  *)
    PKG="$TARGET"; TEST="${2:-}"
    if [[ "$PKG" =~ ^Test[A-Z0-9_] || "$PKG" =~ ^Test$ ]] && [ ! -d "$PKG" ]; then   # a test name alone: find its package
      TEST="$PKG"
      mapfile -t FOUND < <(grep -rlE --include='*_test.go' --exclude-dir=.git --exclude-dir=.claude "^func ${TEST%%/*}\(" . | xargs -r -n1 dirname | sort -u)
      [ "${#FOUND[@]}" -eq 0 ] && early NO_TESTS_RAN "no test named ${TEST%%/*} in any *_test.go"
      [ "${#FOUND[@]}" -gt 1 ] && early NO_TESTS_RAN "${TEST%%/*} is declared in several packages: name the package" "${FOUND[@]}"
      PKG="${FOUND[0]}"
    fi
    [[ "$PKG" == *.go ]] && PKG="$(dirname "$PKG")"     # a file names its package
    PKG="${PKG%/}"; PKG="./${PKG#./}"
    [ -d "$PKG" ] || early NO_TESTS_RAN "no package directory $PKG"
    ARGS=("$PKG")
    [ -n "$TEST" ] && ARGS=(-run "$(anchored "$TEST")" "$PKG") ;;
esac

# --- Run ----------------------------------------------------------------------------------------------------
: > "$STATE/last-run.log"
VET_EXIT=0
if [ -n "$VET" ]; then
  echo "### go vet ./..." >> "$STATE/last-run.log"
  "${GO[@]}" vet ./... >>"$STATE/last-run.log" 2>&1
  VET_EXIT=$?
  echo "### go test" >> "$STATE/last-run.log"
fi
GO_EXIT=0
if [ "$VET_EXIT" -eq 0 ]; then
  "${GO[@]}" test -json -count=1 -timeout "$TIMEOUT" "${ARGS[@]}" >>"$STATE/last-run.log" 2>&1
  GO_EXIT=$?
fi

MODE=target
case "$TARGET" in --unit|--all) MODE=suite ;; esac
LABEL="$TARGET${TEST:+ $TEST}"
[ "$MODE" = target ] && LABEL="${PKG}${TEST:+ $TEST}"
[ "$TARGET" = --unit ] && LABEL="--unit ./$MODULE"

OUTPUT=$(python3 - "$STATE" "$LABEL" "$MODE" "$VET_EXIT" "$GO_EXIT" <<'PY'
import json, os, re, sys

state, label, mode, vet_exit, go_exit = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), int(sys.argv[5])
log = open(os.path.join(state, "last-run.log"), errors="replace").read()

INFRA = re.compile(r"Cannot connect to the Docker daemon|No such container|is not running|connection refused|"
                   r"no such host|dial tcp|i/o timeout|too many clients|go: downloading|"
                   r"executable file not found|exec: \"go\"|OCI runtime", re.I)
NO_PACKAGE = re.compile(r"matched no packages|directory not found|no Go files in|does not contain main module|"
                        r"cannot find package|is not in std|no such file or directory")
SOURCE_LINE = re.compile(r"(\S+\.go):(\d+)(?::\d+)?: ")

events, raw = [], []
for line in log.splitlines():
    if line.startswith("{"):
        try:
            events.append(json.loads(line))
            continue
        except ValueError:
            pass
    if not line.startswith("### "):
        raw.append(line)

def clean(line):
    line = line.strip()
    for prefix in ("/app/", "./"):
        line = line.replace(prefix, "", 1) if line.startswith(prefix) else line
    return line

def finish(status, headline, details=()):
    print(f"{status} — {headline}")
    for line in list(dict.fromkeys(details))[:12]:
        print(f"  {line[:240]}")
    if status not in ("INFRA_ERROR", "NO_TESTS_RAN"):
        with open(os.path.join(state, "last-run"), "w") as f:
            f.write(status + "\n")
    sys.exit(0 if status == "GREEN" else 1 if status.startswith("RED_") else 2)

def tail(lines, n=6):
    return [clean(l) for l in lines if l.strip()][-n:]

# go vet (--all only): a finding is a static check that failed; compile errors show here first.
if vet_exit != 0:
    findings = [clean(l) for l in raw if SOURCE_LINE.search(l)]
    if findings:
        finish("COMPILE_ERROR", f"go vet: {len(findings)} problem(s)", findings)
    if INFRA.search(log):
        finish("INFRA_ERROR", "go vet could not run, not a test result", tail(raw))
    finish("INFRA_ERROR", "go vet failed without a finding", tail(raw))

# Build failures: build-output / build-fail events (Go ≥ 1.24), or plain text from older toolchains.
build_output = [e.get("Output", "") for e in events if e.get("Action") == "build-output"]
build_failed = (any(e.get("Action") == "build-fail" or e.get("FailedBuild") for e in events)
                or any("[build failed]" in l or "[setup failed]" in l for l in raw)
                or any("[build failed]" in e.get("Output", "") or "[setup failed]" in e.get("Output", "")
                       for e in events if e.get("Action") == "output"))
if build_failed:
    errors = [clean(l) for chunk in build_output for l in chunk.splitlines() if SOURCE_LINE.search(l)]
    errors += [clean(l) for l in raw if SOURCE_LINE.search(l)]
    errors = list(dict.fromkeys(errors))
    finish("COMPILE_ERROR", f"{len(errors)} compilation error(s) — {label}", errors or tail(build_output + raw))

if not events:
    text = "\n".join(raw)
    if NO_PACKAGE.search(text):
        finish("NO_TESTS_RAN", f"no Go package for {label}", tail(raw, 3))
    if INFRA.search(text) or go_exit != 0:
        finish("INFRA_ERROR", "go test did not run, not a test result", tail(raw))
    finish("NO_TESTS_RAN", f"no test ran for {label}", tail(raw, 3))

results, output, failed_packages = {}, {}, set()
for e in events:
    action, package, test = e.get("Action"), e.get("Package"), e.get("Test")
    if action == "output":
        output.setdefault((package, test), []).append(e.get("Output", "").rstrip("\n"))
    elif test and action in ("pass", "fail", "skip"):
        results[(package, test)] = action
    elif not test and action == "fail":
        failed_packages.add(package)

package_text = "\n".join(l for (p, t), lines in output.items() if t is None for l in lines)
if "test timed out after" in package_text:
    stuck = [l.strip() for l in package_text.splitlines() if re.match(r"\s+Test\S+ \(", l)]
    finish("INFRA_ERROR", f"timed out: a hang is not a RED (goroutine dump in .claude/state/last-run.log) — {label}",
           [l for l in package_text.splitlines() if "timed out" in l][:1] + stuck)

top = {k: v for k, v in results.items() if "/" not in k[1]}
failed = [k for k, v in results.items() if v == "fail"]
leaves = [k for k in failed if not any(o[0] == k[0] and o[1].startswith(k[1] + "/") for o in failed)]
skipped = [k for k, v in top.items() if v == "skip"]
headline = (f"{len(top)} test(s), {sum(v == 'fail' for v in top.values())} failed, "
            f"{len(skipped)} skipped — {label}")

def lines_of(key):
    return output.get(key, []) + [l for (p, t), ls in output.items() if p == key[0] and t is None for l in ls]

def explain(key):
    lines = output.get(key, [])
    panic = next((l.strip() for l in lines_of(key) if l.startswith("panic: ")), None)
    if panic:
        frames = (re.search(r"(?:/app/|^\s+\./)(\S+\.go:\d+)", l) for l in lines_of(key))
        frame = next((m.group(1) for m in frames if m), "")
        return f"✗ {key[1]}: {panic}" + (f" at {frame}" if frame else "")
    said = [l.strip() for l in lines if SOURCE_LINE.search(l.strip() + " ") and not l.strip().startswith("---")]
    return f"✗ {key[1]}: " + (" | ".join(said) or "failed")

if failed:
    details = [explain(k) for k in leaves]
    text = "\n".join(l for k in leaves for l in lines_of(k))
    if INFRA.search(text):
        finish("INFRA_ERROR", f"infrastructure failure inside the tests, not a test result — {label}", details)
    if any(l.startswith("panic: ") for k in leaves for l in lines_of(k)):
        finish("RED_EXCEPTION", headline, details)
    finish("RED_ASSERTION", headline, details)

if failed_packages:   # the binary failed although no test did: TestMain, init, a panic outside a test
    details = [f"✗ {p}: " + (" | ".join(l.strip() for l in output.get((p, None), []) if l.strip()
                                         and not l.startswith(("FAIL", "ok "))) or "failed")[:200]
               for p in sorted(failed_packages)]
    finish("WIRING_ERROR", f"the test binary failed outside any test — {label}", details)

if not top:
    said = "\n".join(l for lines in output.values() for l in lines)
    if "no tests to run" in said:
        finish("NO_TESTS_RAN", f"no test matches {label}")
    finish("NO_TESTS_RAN", f"no test file in {label}")

if mode == "target" and len(skipped) == len(top):
    reasons = [l.strip() for k in skipped for l in output.get(k, []) if SOURCE_LINE.search(l.strip() + " ")]
    finish("INFRA_ERROR", f"every test skipped, not a test result — {label}", reasons or ["skipped"])

finish("GREEN", headline)
PY
)
CODE=$?
echo "$OUTPUT"
if [ "$TARGET" = --all ] && [ $CODE -eq 0 ]; then   # what --verify may replay
  printf '%s\n' GREEN "--all" "$(tree_fingerprint)" "$(head -n1 <<<"$OUTPUT")" > "$STATE/last-run"
fi
exit $CODE
