# Repository Guidelines

## Project Structure & Module Organization
- Core library lives in `src/main/java/org/hestiastore/index`, with cache, segment, and IO helpers under subpackages.
- Unit tests are in `src/test/java` and follow the same package layout.
- Integration tests live in `src/integration-test/java` (Failsafe picks up `*IT` classes).
- Documentation for architecture, operations, and usage is under `docs/` (served via `mkdocs.yml`); build outputs land in `target/`.

## Build, Test, and Development Commands
- `mvn clean test` — run unit tests (JUnit 5) via Surefire.
- `mvn verify` — full pipeline: unit + integration tests, JaCoCo coverage check (80% bundle minimum, zero missed classes), and dependency check.
- `mvn clean site` — generates static analysis reports (PMD, Checkstyle, SpotBugs) plus coverage/site docs; use before submitting PRs.
- `mvn clean package -DskipTests` — build the jar when you need a fast local iteration (avoid for PRs).

## Coding Style & Naming Conventions
- Java 17; use the repository’s `eclipse-formatter.xml` profile (4-space indentation, no tabs).
- Package namespace is `org.hestiastore.index...`; keep new modules under this root.
- Prefer clear, descriptive class names (e.g., `*Adapter`, `*Cache`, `*Descriptor`) and follow existing suffixes for test doubles (`*Test`, `*IT`).
- Add Javadoc for public types and non-trivial logic; keep methods small and side-effect aware.

## Testing Guidelines
- Write JUnit 5 tests with Mockito for mocks; place fast, deterministic cases in `src/test/java`.
- Use annotations ans static imports in test classes.
- Integration scenarios that touch filesystem or concurrency should go to `src/integration-test/java` with `*IT` suffix.
- Use in-memory `MemDirectory` for unit tests to avoid disk coupling; clean up temp files when touching the filesystem.
- New code must satisfy the JaCoCo gate (80% instruction coverage and no missed classes) and keep tests isolated/parallel-safe.

## Commit & Pull Request Guidelines
- Follow the existing log style: short, imperative titles that describe the change (e.g., “Improve segment cache eviction logic”); stay under ~72 characters.
- Open or reference an issue before sizable work. In PRs, include a concise summary, linked issue, and test results (`mvn verify` output). Add screenshots only when UI/docs visuals change.
- Keep PRs focused and small; note any breaking changes or config flags in the description.
