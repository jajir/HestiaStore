---
name: investigate-test-flake
description: Use when a Hestia Store test fails intermittently and the goal is to identify the source of nondeterminism, reproduce it repeatedly, make the smallest safe fix, and verify stability.
---

# Investigate Test Flake

Use this skill when the task is to diagnose and stabilize an intermittent test failure in Hestia Store.

## Rules

- Treat flakes as nondeterminism problems first.
- Prefer removing the source of instability over hiding it with retries.
- Keep the fix minimal and behavior-preserving.
- Stop if the failure cannot be reproduced after reasonable attempts and report the current evidence.

## Workflow

1. Gather the failing test evidence.
   - Accept CI logs, test names, failure output, or a user-provided flaky test list.
2. Reproduce the flake locally.
   - Run the affected test or module repeatedly.
   - Keep the reproduction command narrow before widening scope.
3. Investigate common flake sources.
   - Concurrency and timing
   - Shared mutable static state
   - Test order dependence
   - Temporary file and directory collisions
   - Clock or random-seed assumptions
   - Port allocation and external resource leakage
4. Apply the smallest safe fix.
   - Prefer deterministic setup and teardown.
   - Prefer explicit synchronization or state isolation when justified.
   - Avoid blanket sleeps or retries unless the user explicitly accepts that tradeoff.
5. Verify stability.
   - Re-run the flaky test repeatedly.
   - Re-run the affected module.
   - Run `mvn clean verify` before finishing non-trivial fixes.
6. Summarize the result.
   - State the suspected root cause.
   - State the reproduction approach.
   - State the fix and verification evidence.

## Stop Conditions

- Stop if the flaky behavior cannot be reproduced after reasonable attempts.
- Stop if the next fix would require redesign rather than stabilization.
- Stop if the issue depends on external infrastructure outside the repository.
