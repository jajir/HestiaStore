# Code Quality Charter

This page defines the target shape of production code in HestiaStore and the
refactoring workflow used to get there without losing performance.

The goal is simple: when someone opens an arbitrary class, they should be able
to understand its responsibility, collaborators, state model, and hot path
without reverse-engineering the whole package first.

This charter applies to both human-written changes and agent-assisted changes.

## Desired Outcome

- Each class has one dominant reason to change.
- Public methods read as orchestration of named steps, not as long mixed
  control flow.
- Stateful behavior is explicit: lifecycle, retry, locking, async work, and
  persistence are visible and separated.
- Tests describe externally visible behavior and failure modes, not private
  field layout.
- Performance-sensitive code changes are validated with repeatable benchmark
  comparisons.

## Code Shape Rules

### Class Responsibility

- Keep one class focused on one responsibility or one cohesive orchestration
  role.
- When a class mixes persistence, lifecycle, scheduling, retry, and runtime
  management, split by responsibility before adding more features.
- Large orchestrators are acceptable only when they delegate real work to small
  collaborators with clear names.

### Method Shape

- Keep public methods as short orchestration flows over named private methods or
  collaborators.
- Separate domain decisions from I/O, logging, retry, and state transitions.
- Avoid methods that both compute a decision and perform multiple side effects.

### Dependencies and Construction

- Prefer explicit collaborators over hidden instantiation in constructors.
- If an object graph is complex, move assembly into a builder or factory.
- Introduce narrow interfaces when a dependency must be substituted in tests or
  split into smaller responsibilities.

### Static Factory Method Names

Use static factory method names to describe how the instance is obtained.

- Use `of(...)` when the object is assembled directly from simple values and no
  conversion or lifecycle behavior is involved.
- Use `fromXxx(...)` when the object is converted from another representation,
  such as a DTO, serialized form, configuration view, snapshot, or external
  type.
- Use `createXxx(...)` when the factory performs non-trivial assembly, when no
  better domain-specific name exists, or when the method should make an
  important lifecycle or state distinction explicit, for example
  `createStarted(...)` or `createOpening(...)`.

### Stateful and Concurrent Code

- Keep state transitions explicit and named.
- Isolate retry loops, background scheduling, and coordination code from core
  business operations.
- Concurrency rules must be visible in code and mirrored in tests:
  - what blocks
  - what is retried
  - what is immutable
  - what is bounded

## Testability Rules

- Write characterization tests against public behavior before moving logic out
  of a hotspot.
- Prefer tests that exercise contracts over tests that inspect internals.
- Do not add new reflection-based tests against private fields when a small seam
  or collaborator extraction would make behavior testable.
- Keep concurrency tests deterministic: use latches, await helpers, and bounded
  polling instead of sleeps.
- When extracting a collaborator from a large class, add focused tests for the
  new type immediately.

## Refactoring Workflow

Refactor in small bounded cycles. Do not attempt a big-bang rewrite of a core
module.

1. Pick one hotspot and one responsibility to improve.
2. Capture current behavior with characterization tests.
3. Capture current performance baseline for affected hot paths.
4. Extract one collaborator or one stateful concern.
5. Re-run tests and benchmark comparison.
6. Merge only if readability improved and performance stayed within budget.

Good first refactoring slices are:

- open/close/recovery
- put/delete with WAL interaction
- flush/drain/compaction orchestration
- split planner and split execution admission
- runtime tuning and control-plane logic

## Performance Gates

Changes touching hot paths in `engine`, especially `segment`, `segmentindex`,
`chunkstore`, `sorteddatafile`, or WAL code, must be checked against benchmark
profiles before they are considered done.

Primary references:

- benchmark runner and compare flow in `benchmarks/README.md`
- [Performance Model & Sizing](../architecture/segmentindex/performance.md)
- [Rollout Quality Gates](rollout-quality-gates.md)

Recommended per-change workflow:

```sh
mvn -pl benchmarks -am package
python3 benchmarks/scripts/run_jmh_profile.py \
  --repo-root . \
  --profile segment-index-pr-smoke \
  --output-dir /tmp/hestia-bench/current
python3 benchmarks/scripts/compare_jmh_profile.py \
  --baseline /tmp/hestia-bench/base/summary.json \
  --candidate /tmp/hestia-bench/current/summary.json \
  --markdown-out /tmp/hestia-bench/comparison.md \
  --json-out /tmp/hestia-bench/comparison.json
```

Interpretation rules:

- `neutral`: acceptable by default
- `warning`: requires review and explanation
- `worse`: do not merge without an explicit decision that the tradeoff is worth
  it

If a change introduces a new hot path or a new execution mode, add or extend a
canonical benchmark profile instead of relying on anecdotal measurements.

## Review Checklist

Before merging a refactoring change, confirm:

- the class reads more clearly than before
- responsibilities moved in one direction instead of being renamed in place
- tests cover behavior, error paths, and state transitions
- no new reflection test was introduced without a strong reason
- package boundaries still hold
- benchmark delta is documented for hot-path changes
- the refactor backlog or architecture docs were updated when the design moved

## Common Smells to Fix Early

- classes above roughly 400 lines that keep growing
- constructors that assemble many concrete collaborators directly
- methods that combine retry, I/O, state mutation, and logging
- tests that require reflection to manipulate private state
- package-level cycles or unclear ownership between orchestration and storage
- performance discussions that are not backed by a benchmark profile

## Relationship to Other Project Docs

- Use [Refactor Backlog](../refactor-backlog.md) to track the sequence of
  refactoring work.
- Use [Rollout Quality Gates](rollout-quality-gates.md) for staged release
  constraints.
- Use architecture pages as source-of-truth for stable design, not as a place
  for temporary refactoring notes.
