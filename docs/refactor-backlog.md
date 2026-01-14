# Refactor backlog

## Active (segmentindex refactor plan - class level)

### Segmentindex design issues (review backlog)

[ ] Make segment replacement atomic (Risk: HIGH)
    - Define crash-safe swap protocol (temp names, fsync, rename order).
    - Implement rename-then-delete flow and keep old files until swap commits.
    - Add recovery logic for partial swaps and tests with crash/failure hooks.
[ ] Tie `index.map` updates to segment file swaps (Risk: HIGH)
    - Introduce a marker/txn file with PREPARED/COMMITTED states.
    - Write map changes to temp + fsync, then atomically swap on COMMIT.
    - Reconcile marker on startup to roll forward/backward; add tests.
[ ] Implement a real registry lock (Risk: MEDIUM)
    - Add an explicit lock around registry mutations + file ops.
    - Replace/rename `executeWithRegistryLock` to actually serialize callers.
    - Add tests for split/compact interleaving and segment visibility.
[ ] Replace common-pool async with dedicated executor + backpressure (Risk: MEDIUM)
    - Add/configure a dedicated executor for async API calls.
    - Track in-flight tasks and wait on close; add queue/backpressure limits.
    - Add tests for saturation, cancellation, and close ordering.
[ ] Ensure `IndexAsyncAdapter.close()` waits for in-flight async ops (Risk: MEDIUM)
    - Reintroduce in-flight tracking with a wait/timeout policy.
    - Prevent close from async thread or document/guard with exceptions.
    - Add tests for close during concurrent async puts/gets.
[ ] Replace busy-spin loops with retry+backoff+timeout (Risk: MEDIUM)
    - Use `IndexRetryPolicy` in `SegmentsIterator` and split iterator open.
    - Add interrupt handling and timeout paths with clear error messaging.
    - Add tests for BUSY loops and timeout behavior.
[ ] Stop returning `null` on CLOSED in `SegmentIndexImpl.get` (Risk: MEDIUM)
    - Decide API surface (exception vs status/Optional).
    - Update callers and docs to distinguish "missing" vs "closed".
    - Add tests for CLOSED/ERROR paths.
[ ] Provide index-level FULL_ISOLATION streaming (Risk: MEDIUM)
    - Add overload or option to request FULL_ISOLATION on index iterators.
    - Implement iterator that holds exclusivity across segments safely.
    - Add tests for long-running scans during maintenance.
[ ] Stream without full materialization in `IndexInternalConcurrent.getStream` (Risk: MEDIUM)
    - Replace list snapshot with streaming spliterator tied to iterator close.
    - Ensure iterator invalidation semantics are preserved.
    - Add tests for large datasets and early close.
[ ] Propagate MDC context to async ops and stream consumption (Risk: LOW)
    - Capture MDC context on submit and reapply in async tasks.
    - Wrap stream/iterator consumption with MDC scope; clear on close.
    - Add tests asserting `index.name` appears in async logs.

## Ready

- (move items here when they are scoped and ready to execute)

## Deferred (segment scope, do not touch now)

[ ] - segment: from segment index do not call flush; only user or segment decides.
[ ] - segment: add SegmentSyncAdapters wrapper to retry BUSY with backoff until OK or throw on ERROR/CLOSED.
[ ] - segment: add configurable BUSY timeout to avoid infinite wait (split waits).
[ ] - segment: avoid file rename for flush/compact switching; point index to new version.
[ ] - segment: consider segment per directory.

## In Progress

- (move items here when actively working)

## Done (Archive)

- (keep completed items here; do not delete)
