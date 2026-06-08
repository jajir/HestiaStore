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
- Do not use generic Java functional interfaces such as `Function`, `Consumer`, `Supplier`, `Predicate`, or `Runnable` to mask a real domain dependency or lifecycle relationship between production classes. Flag this as a design finding and prefer an explicit domain collaborator, direct method call, or named interface that makes the dependency visible.
- Do not replace visible return values or constructor arguments with one-shot callback-style factory methods such as `create(OpenedXConsumer)` when the callback receives many newly built collaborators. This hides a dependency bag behind control flow, obscures lifecycle ownership, and weakens package access-point rules. Prefer returning an explicit domain result object, calling a concrete collaborator directly, or introducing a named lifecycle interface that exposes the real dependency.
- Prefer separate top-level or package-private helper classes over `private static final` nested classes when the nested type has non-trivial behavior, domain meaning, or test value outside one tiny local helper use.
- Verify that name of classes, method variables and other identifiers are meaningful and consistent with their behavior and domain meaning. Avoid generic names like `Helper`, `Util`, `Manager`, or `Processor` without clear justification.
- Make sure that algorithms logic is clear and straightforward, and that control flow is easy to follow. Flag complex or nested logic that could be simplified or clarified.
- Do not flag every nested class by default; flag them when they hide meaningful logic, grow beyond a tiny scoped helper, or make the host class harder to understand.
- Do not flag a class for having only one constructor. One clear constructor is fine, and is usually preferred; flag constructors only when they accumulate unrelated responsibilities, duplicate initialization paths, or make lifecycle/state choices ambiguous.
- Keep summaries brief and secondary.
- If no findings are present, state that explicitly and mention any remaining verification gaps.
- Limit visibility of classes, methods, and variables to the minimum needed for their intended use.
- For test-only access to package-private production internals, prefer same-package test sources under `src/test/java` over widening production visibility. Do not make package-private production types public only for tests.
- When tests in another package need package-private behavior, add a same-package test-support or fixture class under `src/test/java` that exposes only the needed shielding method. Prefer that over adding `*ForTests` methods or public test seams to production code.
- Production classes should contain production behavior only. Keep test doubles, fake implementations, and test-only `NO_OPERATION` implementations under `src/test/java`, unless the no-op is a real production null-object behavior for a disabled feature or supported runtime mode.
- Avoid putting feature-enabled switches such as `enabled` or `isEnabled` inside service/runtime/resource objects to make their methods silently no-op. Let the calling assembly code decide whether to create the real implementation or a separate production null-object/no-op implementation behind the same interface.
- Remove all unused code, including imports, variables, methods, and classes.
- Check static factory method names against `docs/development/code-quality-charter.md`:
  use `of(...)` for direct value assembly, `fromXxx(...)` for conversion from
  another representation, and `createXxx(...)` for non-trivial assembly or
  explicit lifecycle/state distinctions.
- When builder pattern is used variables should be set with methods like `withVariableName`.


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
