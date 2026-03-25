# General Rules

- Be honest.
- When it makes sense, structure responses as numbered lists.

# Hestia Store Repository Rules

- This repository is a Maven-based Java library project with a multi-module parent build.
- Prefer focused changes that stay within the requested scope.
- Avoid unnecessary API-breaking changes.
- Run `mvn clean verify` after non-trivial changes.
- Use the `fix-ci-failure` skill for failing CI jobs and broken local verification runs.
- Use the `update-dependencies` skill for Maven dependency and plugin refresh work.
- Use the `java-code-review` skill for reviewing diffs, branches, and pull requests.
- Use the `investigate-test-flake` skill for intermittent or nondeterministic test failures.
- Use the `benchmark-regression-check` skill for benchmark drift and performance regression investigation.
- Use the `release-maven-library` skill for release preparation, versioning, and post-release snapshot tasks.
- Use the `sonar-smell-cleanup` skill for small, safe SonarCloud code smell cleanup batches.

## Project Structure & Module Organization

- Core library lives in `src/main/java/org/hestiastore/index`, with cache, segment, and IO helpers under subpackages.
- Unit tests are in `src/test/java` and follow the same package layout.
- Integration tests live in `src/integration-test/java`, and Failsafe picks up `*IT` classes.
- Documentation for architecture, operations, and usage is under `docs/` and is served via `mkdocs.yml`; build outputs land in `target/`.
- For documentation changes, follow `docs/development/documentation-guidelines.md`.
- In the parent POM, keep shared library versions in dependency management so versions stay aligned across submodules.

## Build, Test, and Development Commands

- `mvn clean test` runs unit tests with Surefire.
- `mvn verify` runs the full pipeline: unit tests, integration tests, JaCoCo coverage checks, and dependency checks.
- `mvn clean site` generates static analysis reports, coverage reports, and site documentation.
- `mvn clean package -DskipTests` builds artifacts for fast local iteration, but avoid using it as final verification for PRs.
- The `benchmarks` module sets `skipTests=true` by default; use `-DskipTests=false` when you need benchmark module tests or Python script smoke tests to actually execute.

## Coding Style & Naming Conventions

- Use Java 17 and the repository `eclipse-formatter.xml` profile with 4-space indentation.
- Keep packages under `org.hestiastore.index...`.
- Avoid inner enums and exception classes when a separate file is clearer.
- Prefer clear, descriptive class names such as `*Adapter`, `*Cache`, and `*Descriptor`.
- Add Javadoc for public types and non-trivial logic.
- Do not use fully qualified class names in code; use explicit imports instead.
- Remove unused imports in every touched file before finishing.
- Benchmark sources under `benchmarks/src/...` are especially prone to stale imports after refactors, so do a final unused-import pass there before updating a PR.
- Try to avoid creating class instances in constructors.
- Prefer centralizing object creation in builder classes when practical.
- Do not use Java records.
- Keep one constructor per class when possible.
- Use `Vldtn` as the primary validation helper.
- Extract duplicated string literals with 3 or more occurrences into `private static final` constants.
- Do not use `catch (Throwable)`; catch `Exception` or a more specific exception type.
- When handling `InterruptedException`, always call `Thread.currentThread().interrupt()` before returning or rethrowing.
- In tests, do not use `Thread.sleep()`. Prefer `CountDownLatch`, timeout-based polling, `LockSupport.parkNanos`, or await helpers.
- In `assertThrows(...)` lambdas, keep only one potentially throwing invocation.
- Remove unnecessary `throws Exception` or `throws IOException` declarations from tests when the body cannot throw them.
- In Mockito, remove unnecessary `eq(...)` wrappers when all arguments are direct values.
- Prefer `Stream.toList()` over `collect(Collectors.toList())` when list mutability is not required.
- Minimize `break` and `continue` in a single loop; prefer guard conditions and helper methods.
- Replace simple boolean `if/else` return blocks with a single return expression.
- Remove unused generic type parameters.
- In tests, prefer package-private classes and methods over `public` when framework visibility does not require `public`.
- Frontend: prefer `.dataset` over `getAttribute("data-*")`.
- Frontend: enforce minimum text/background contrast, especially for muted and status colors.
- Try to avoid returning `null`; prefer an exception or a null-value object when that makes sense.

## Testing Guidelines

- Write JUnit 5 tests with Mockito for mocks and keep fast, deterministic tests in `src/test/java`.
- Use annotations and static imports in test classes.
- Integration scenarios that touch the filesystem or concurrency should go in `src/integration-test/java` with the `*IT` suffix.
- Use in-memory `MemDirectory` for unit tests to avoid disk coupling, and clean up temp files when touching the filesystem.
- New code must satisfy the JaCoCo gate: 80% instruction coverage and no missed classes.
- Test corner cases.
- For each changed production class, ensure there is a corresponding `*Test` class in the matching package.
- Apply the matching `*Test` expectation to helper, value, registry, SPI, codec, and factory classes too, not only to large service classes.
- If a matching `*Test` class is intentionally not added for a changed production class, explicitly explain in the final response which existing tests cover it and which corner cases remain uncovered.
- Changes in persistence, configuration, or codec layers must include direct tests for round-trip behavior, backward compatibility, and invalid-input handling.
- Changes in factory, lifecycle, or transaction layers must include direct tests for open/close/commit ordering, repeated calls, and rollback or cleanup on failure paths.
- Changes in registry, provider, supplier, or spec-mapping layers must include direct tests for unknown IDs, duplicate registration, canonicalization, and negative-path resolution failures.
- New public classes, public overloads, and new persistence/runtime adaptation paths must have direct tests for the main happy path and at least one representative failure path.
- If a JUnit test class uses mocks, annotate it with `@ExtendWith(MockitoExtension.class)`.
- Create one private field per mocked dependency.
- Instantiate the class under test in `setUp()` and clean up in `tearDown()` when needed.
- Prefer try-with-resources over `try`/`finally`.
- Each test method should contain at least one assertion.

## Refactoring Guidelines

- The source of truth for target code shape, refactoring workflow, and performance gates lives in `docs/development/code-quality-charter.md`.
- The refactoring backlog lives in `docs/refactor-backlog.md`.
- Each refactoring step should have a unique ID and checkbox marker.
- Completed tasks move to `## Done (Archive)` and are marked done.
- Tasks with IDs starting with `M` are maintenance tasks; they remain repeatable and should not be archived after one pass.

## Commit & Pull Request Guidelines

- Follow the existing log style: short, imperative titles under roughly 72 characters.
- Open or reference an issue before sizable work.
- In PRs, include a concise summary, linked issue, and test results such as `mvn verify` output.
- Add screenshots only when UI or documentation visuals change.
- Keep PRs focused and small, and call out breaking changes or config flags in the description.
