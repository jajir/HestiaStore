---
name: java-code-review
description: Use when reviewing Java or Maven changes in Hestia Store, including actionable PMD and CPD findings from module site reports. Focus on bugs, regressions, API risk, concurrency issues, persistence concerns, and missing tests before style or cosmetic feedback.
---

# Java Code Review

Use this skill when the task is to review a diff, branch, or pull request in Hestia Store.

## Rules

- Findings come first.
- Prioritize correctness, regressions, API stability, concurrency, persistence, and missing tests.
- When module site artifacts exist, inspect PMD and CPD reports for the affected modules and include actionable report findings in the review.
- Prefer code shapes where each class has one clear concern and one main lifecycle.
- Treat mixed responsibilities in one class as maintainability findings when they increase coupling, blur persistence/runtime boundaries, or make testing harder.
- Prefer separate top-level or package-private helper classes over `private static final` nested classes when the nested type has non-trivial behavior, domain meaning, or test value outside one tiny local helper use.
- Do not flag every nested class by default; flag them when they hide meaningful logic, grow beyond a tiny scoped helper, or make the host class harder to understand.
- Keep summaries brief and secondary.
- If no findings are present, state that explicitly and mention any remaining verification gaps.

## Review Workflow

1. Inspect the target diff, branch, or pull request.
2. Understand the intended behavior change before judging the implementation.
3. Gather related report inputs when available.
   - For each affected module, look for `target/site/<module>/pmd.html` and `target/site/<module>/cpd.html`.
   - If only module-local output exists, also check `<module>/target/site/pmd.html` and `<module>/target/site/cpd.html`.
   - Prioritize findings in changed files or obvious duplication introduced by the current change.
4. Review for the highest-risk issues first.
   - Behavioral regressions
   - Public API changes
   - Persistence or serialization compatibility
   - Concurrency or resource lifecycle problems
   - Missing or weak tests
5. Review maintainability next.
   - Duplicated logic
   - Unclear control flow
   - Fragile assumptions
   - Configuration drift across modules
   - Classes carrying multiple unrelated concerns
   - Nested helper classes that should likely be extracted
   - Runtime wiring mixed into value/config/persistence classes
   - Constructors or builders that accumulate too many responsibilities
6. Write findings in priority order.
   - Include file and line references when possible.
   - Explain the concrete risk, not just the code style concern.
   - Call out which issues came from PMD or CPD when that affects prioritization.
7. End with brief open questions or residual risks if needed.

## Output

- Findings
- Open questions or assumptions
- Residual risk or testing gaps

## Stop Conditions

- Stop if the review target is unavailable.
- Stop if the task requires implementation instead of review and the user did not ask for code changes.
