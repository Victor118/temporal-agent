---
name: tdd
description: Implement changes test-first, in RED, GREEN, REFACTOR steps, one commit per step
runs: true
---
# Test-Driven Development

Follow this for every behavior you add or change.

## The loop

1. **RED**: write one test for the next small piece of behavior. Run it and
   watch it fail, for the reason you expect (an assertion, not a compile error
   in an unrelated file). Commit: `test: <behavior> (red)`.
2. **GREEN**: write the least code that makes it pass. Run the whole suite of
   the package you touched: everything passes. Commit: `feat: <behavior>`
   (or `fix:` for a bug).
3. **REFACTOR**: with the tests green, remove duplication and clarify names.
   Run the suite again. Commit: `refactor: <what>` — or skip this step if
   there is nothing to improve.

Then take the next piece of behavior.

## Rules

- No production code without a failing test that asks for it.
- One behavior per loop: a test that needs a dozen lines of new code is too
  big; split it.
- A bug fix starts with a test that reproduces the bug.
- Follow the repository's test conventions (framework, file layout, naming):
  read a few existing tests before writing yours.
- Never weaken or delete an existing test to make your change pass. If one is
  wrong, say so in your report.
- If you cannot run the tests (a missing toolchain, a command refused), say so
  in your report rather than claiming they pass.

## In a read-only run

An analysis cannot commit: do not try. Report instead how the code you were
asked about measures against these rules: behavior without a test, tests
that would not have failed without the code they cover.

## Report

List the loops you completed (behavior, commits), and anything left red.
