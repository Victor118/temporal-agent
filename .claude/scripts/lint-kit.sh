#!/usr/bin/env bash
# Consistency check for the TDD kit (.claude/agents, skills, commands, rules, eval).
# Run after editing anything under .claude/. Exit 1 when a problem is found.
set -uo pipefail
ROOT="$(git -C "${CLAUDE_PROJECT_DIR:-$PWD}" rev-parse --show-toplevel)"
exec python3 - "$ROOT" <<'PY'
import os, re, sys, glob

root = sys.argv[1]
kit = os.path.join(root, ".claude")
files = sorted(p for pattern in ("agents/*.md", "skills/*/SKILL.md", "commands/*.md", "rules/*.md",
                                 "eval/**/*.md", "README.md")
               for p in glob.glob(os.path.join(kit, pattern), recursive=True))
problems = []

def report(path, line_no, message):
    problems.append(f"{os.path.relpath(path, root)}:{line_no}: {message}")

# Leftovers from other projects and retired vocabulary.
STALE = [
    (r"mealbooking|com\.wealcome\.wealdoc|(?<![\w/.])wealdoc/|-pl wealdoc", "old module name (appointments)"), (r"hexagon/", "old test path"),
    (r"#\[Test\]|PHPUnit|composer\.json|services\.yaml|qualiopi", "PHP leftover"),
    (r"\bpnpm\b|vitest|\.spec\.tsx?\b", "TypeScript leftover"),
    (r"\bRider\b|\bTrainee\b|VehicleType", "example from another domain"),
    (r"🔴 RED|🟢|⚡|EARLY GREEN —|RULE 6b|DROPPED|allow-main|(?<!O-)RULE [1-68]\b", "retired status / rule / escape"),
    (r"early green: keep|\bkeep / delete\b|keep — pins", "early green kept (user decision: delete, strengthen, reorder, or pin a threshold side)"),
    (r"(?<!overrides `)should_\[?\w", "old test naming (devrait_…)"),
]
STATUSES = {"⏳ Pending", "✅ GREEN", "🧬 PINNED", "➕ PROPOSED"}
STATUS_RE = re.compile(r"((?:[☀-➿\U0001F300-\U0001FAFF]️?)+)\s"
                       r"(EARLY GREEN|Early Green|GREEN|RED|Pending|PINNED|Pinned|DROPPED|Dropped|PROPOSED)\b")

def headings(path):
    text = open(path).read() if os.path.exists(path) else ""
    return set(re.findall(r"^## (\d+)\.", text, re.M)), text

engine_path = os.path.join(kit, "skills/tdd-workflow-engine/SKILL.md")
analysis_path = os.path.join(kit, "skills/tdd-analysis/SKILL.md")
engine_sections, engine_text = headings(engine_path)
analysis_sections, _ = headings(analysis_path)
engine_rules = set(re.findall(r"\*\*RULE (\d+b?|[A-Z])\*\*", engine_text))

for path in files:
    text = open(path).read()
    own_sections = set(re.findall(r"^## (\d+)\.", text, re.M))
    own_rules = set(re.findall(r"\*\*RULE (\d+b?)\*\*", text))
    for no, line in enumerate(text.splitlines(), 1):
        for pattern, why in STALE:
            if re.search(pattern, line):
                report(path, no, f"{why}: {re.search(pattern, line).group(0)!r}")
        for emoji, label in STATUS_RE.findall(line):
            if f"{emoji} {label}" not in STATUSES:
                report(path, no, f"unknown status {emoji} {label!r}")
        for m in re.finditer(r"(engine|`?tdd-workflow-engine`?|`?tdd-analysis`?)?\s*§(\d+)", line):
            owner, number = m.group(1) or "", m.group(2)
            if "analysis" in owner:
                known, target = analysis_sections, "tdd-analysis"
            elif owner:
                known, target = engine_sections, "tdd-workflow-engine"
            elif path in (engine_path, analysis_path):
                known, target = own_sections, "this file"
            else:
                continue
            if number not in known:
                report(path, no, f"§{number} does not exist in {target}")
        for number in re.findall(r"(?<!O-)RULE (\d+b?)\b", line):
            if number not in engine_rules and number not in own_rules:
                report(path, no, f"RULE {number} is defined neither here nor in the engine")
        for m in re.findall(r"workspace/[\w./<>-]*tdd[\w.<>-]*\.md", line):
            if not m.startswith("workspace/current/"):
                report(path, no, f"analysis file outside workspace/current/: {m}")
        for m in re.findall(r"\.claude/(?:scripts|hooks)/[\w.-]+\.sh", line):
            script = os.path.join(root, m)
            if not os.access(script, os.X_OK):
                report(path, no, f"missing or non-executable script: {m}")

    front = re.match(r"---\n(.*?)\n---", text, re.S)
    if front and "/agents/" in path:
        skills = re.search(r"^skills:\n((?:\s+- .+\n?)+)", front.group(1), re.M)
        for name in re.findall(r"- (\S+)", skills.group(1)) if skills else []:
            if not os.path.exists(os.path.join(kit, "skills", name, "SKILL.md")):
                report(path, 1, f"preloaded skill not found: {name}")
        for name in re.findall(r"Agent\(([^)]*)\)", front.group(1)):
            for agent in (a.strip() for a in name.split(",")):
                if not os.path.exists(os.path.join(kit, "agents", agent + ".md")):
                    report(path, 1, f"delegated agent not found: {agent}")

# Settings must wire both hooks.
settings = open(os.path.join(kit, "settings.json")).read() if os.path.exists(os.path.join(kit, "settings.json")) else ""
for hook in ("test-first-guard.sh", "bash-bypass-detector.sh"):
    if hook not in settings:
        report(os.path.join(kit, "settings.json"), 1, f"hook not wired: {hook}")

# Size budget: preloaded files may not silently grow (complexity creep). Update eval/budget.txt on purpose.
budget_path = os.path.join(kit, "eval/budget.txt")
if os.path.exists(budget_path):
    for entry in open(budget_path):
        if not entry.strip() or entry.startswith("#"):
            continue
        name, allowed = entry.split()
        path = os.path.join(kit, name)
        if os.path.exists(path) and os.path.getsize(path) > int(allowed) * 1.15:
            report(path, 1, f"size {os.path.getsize(path)} bytes exceeds budget {allowed} by more than 15% — "
                            "simplify, or raise the budget on purpose in eval/budget.txt")

for problem in problems:
    print(problem)
print(f"lint-kit: {len(files)} files, {len(problems)} problem(s)")
sys.exit(1 if problems else 0)
PY
