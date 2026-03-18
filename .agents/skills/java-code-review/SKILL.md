---
name: java-code-review
description: Use when reviewing Java or Maven changes in Hestia Store. Focus on bugs, regressions, API risk, concurrency issues, persistence concerns, and missing tests before style or cosmetic feedback.
---

# Java Code Review

Use this skill when the task is to review a diff, branch, or pull request in Hestia Store.

## Rules

- Findings come first.
- Prioritize correctness, regressions, API stability, concurrency, persistence, and missing tests.
- Keep summaries brief and secondary.
- If no findings are present, state that explicitly and mention any remaining verification gaps.

## Review Workflow

1. Inspect the target diff, branch, or pull request.
2. Understand the intended behavior change before judging the implementation.
3. Review for the highest-risk issues first.
   - Behavioral regressions
   - Public API changes
   - Persistence or serialization compatibility
   - Concurrency or resource lifecycle problems
   - Missing or weak tests
4. Review maintainability next.
   - Duplicated logic
   - Unclear control flow
   - Fragile assumptions
   - Configuration drift across modules
5. Write findings in priority order.
   - Include file and line references when possible.
   - Explain the concrete risk, not just the code style concern.
6. End with brief open questions or residual risks if needed.

## Output

- Findings
- Open questions or assumptions
- Residual risk or testing gaps

## Stop Conditions

- Stop if the review target is unavailable.
- Stop if the task requires implementation instead of review and the user did not ask for code changes.
