---
name: fix-ci-failure
description: Use when a GitHub Actions job, Maven build, or test run is failing in Hestia Store and the goal is to identify the first real failure, reproduce it locally, apply the smallest safe fix, and verify the result.
---

# Fix CI Failure

Use this skill when the task is to diagnose and fix a failing CI run in Hestia Store.

## Rules

- Focus on the first real failure, not downstream noise.
- Keep the fix minimal and directly tied to the failure.
- Do not mix in unrelated refactors or dependency upgrades unless they are required to resolve the failure.
- Stop and report clearly if the failure cannot be reproduced, the logs are incomplete, or the problem is an external service outage.

## Workflow

1. Gather the failure source.
   - Accept a GitHub Actions URL, pasted logs, a failing Maven command, or a local test failure.
   - If the failure source is missing, stop and ask for it.
2. Identify the first actionable failure.
   - Ignore later cascading failures until the earliest real error is understood.
   - Note the module, Maven phase, test class, and error message.
3. Reproduce the failure locally with the narrowest useful command.
   - Start with the affected module or test when possible.
   - Widen to `mvn clean verify` only after the local root cause is addressed or when the failure spans modules.
4. Fix the root cause.
   - Prefer behavior-preserving changes.
   - Keep the diff small and easy to review.
5. Verify the fix.
   - Re-run the narrow reproduction command first.
   - Run `mvn clean verify` before finishing non-trivial fixes.
6. Summarize the result.
   - State the root cause.
   - State the fix.
   - State the verification commands and outcomes.
   - Call out any remaining risk or follow-up.

## Stop Conditions

- Stop if the failure source is unavailable.
- Stop if the failure is not reproducible after reasonable local attempts.
- Stop if the failure is caused by external infrastructure rather than repository code.
- Stop if the next step requires a broad refactor rather than a targeted fix.
