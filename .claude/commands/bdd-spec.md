---
description: Produce a specification-by-example (.md in workspace/) via a capped BDD workshop
argument-hint: <input-file-path> <max-questions>
---

# /bdd-spec -> Specification by Example (BDD-workshop Agent)

Generate a complete **specification-by-example** document by running the
**`BDD-workshop` custom agent** against a source requirement file, with a hard
cap on how many questions the workshop may ask.

## Arguments (positional)

`/bdd-spec $ARGUMENTS`

1. **`$1` — input file path** (required): a file describing the requirement /
   feature / context to elaborate. The parent agent MUST read this file first.
2. **`$2` — max questions** (required, integer): the **hard cap** on the total
   number of questions the workshop is allowed to ask the user across the whole
   session. Once this budget is exhausted, the workshop MUST synthesize the
   specification with whatever information it has gathered.

If either argument is missing, STOP and ask the user to supply it — do not guess.

## Steps for the parent agent

1. **Read the input file** (`$1`) yourself, in full, to confirm it exists and to
   capture the feature name for the output filename. Do NOT pre-digest or
   summarize its business rules into the agent prompt — the agent will read it
   itself.

2. **Delegate to the `BDD-workshop` agent** with a short prompt. The prompt MUST:
   - Point the agent at the input file: `Facilitate a specification-by-example
     workshop based on the requirement in: {input_file_path}`
   - Instruct the agent to gather every clarification **exclusively through the
     `AskUserQuestion` tool** — one call per question, never plain-text prose
     questions — so each question counts against the budget and is surfaced to
     the user as a proper prompt.
   - State the **hard question cap explicitly**: `You may ask the user AT MOST
     {X} questions in total across this entire session. Track your remaining
     budget. When the budget reaches zero, stop asking and synthesize the final
     specification with whatever you have gathered — do NOT exceed the cap under
     any circumstance.`
   - Let the agent drive the interaction via `AskUserQuestion` — it decides how
     to spend its question budget.
   - The prompt MUST NOT ask the agent to write the file, summarize the spec for
     it, or otherwise bypass its interactive nature.

   **CRITICAL:** The `BDD-workshop` agent is interactive by design and uses
   `AskUserQuestion`. When it returns questions, relay them to the user and wait
   — never fabricate the user's answers to save budget.

3. **Write the specification** to a `.md` file in the `workspace/` folder once the
   workshop concludes. Derive the filename from the **input filename**:
   `workspace/{input-basename}-spec-by-example.md`
   (e.g. input `register-entry.md` → `workspace/register-entry-spec-by-example.md`).

## What the output file contains

A complete specification by example:

- **Feature** — title and narrative (As a… I want… so that…)
- **Business rules** — the invariants uncovered during the workshop
- **Scenarios** — concrete `Given / When / Then` examples, one per rule and per
  edge case, using named **personas** and **accurate, realistic example data**
- **Edge cases & open questions** — anything surfaced but left unresolved within
  the question budget

## Example

```
/bdd-spec README.md 5
```

Reads `README.md`, runs the BDD workshop asking at most **5**
questions, then writes `workspace/README-spec-by-example.md`.
