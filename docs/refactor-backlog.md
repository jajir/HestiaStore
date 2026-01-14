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

### Segment memory growth (JMH load test)

[ ] Bound delta cache growth in `SegmentCache` (Risk: HIGH)
    - Problem: delta cache uses `maxNumberOfKeysInSegmentCache` only as initial
      capacity; it grows without a hard cap.
    - Fix: enforce a hard limit; on overflow trigger compaction or spill delta
      to disk and evict in-memory entries; add metrics + tests.
[ ] Remove duplicate in-memory delta caches (Risk: HIGH)
    - Problem: `SegmentCache` keeps a delta cache while `SegmentDeltaCache`
      keeps its own, effectively doubling memory.
    - Fix: unify into a single delta cache implementation; ensure write path
      and read path reference the same structure.
[ ] Avoid eager full load of delta files (Risk: HIGH)
    - Problem: `SegmentDeltaCache` loads all delta files into memory on open.
    - Fix: lazy/streamed delta access or bounded cache; add LRU eviction or
      on-disk lookup path; add stress tests.
[ ] Add eviction for heavy segment resources (Risk: MEDIUM)
    - Problem: `SegmentResourcesImpl` caches delta/bloom/scarce forever.
    - Fix: tie resource lifetime to segment eviction or add per-resource LRU;
      ensure invalidate/close releases memory.
[ ] Enforce `maxNumberOfSegmentsInCache` in `SegmentRegistry` (Risk: MEDIUM)
    - Problem: segments are cached unbounded; memory grows as segments grow.
    - Fix: implement LRU or size-bounded cache; evict + close segments and
      invalidate resources on eviction.
[ ] Stop materializing merged cache lists on read (Risk: MEDIUM)
    - Problem: `SegmentReadPath.openIterator` calls `getAsSortedList`, building
      full merged lists for each iterator.
    - Fix: provide streaming merge iterator over delta/write caches without
      full list materialization.
[ ] Stream compaction without full cache snapshot (Risk: MEDIUM)
    - Problem: compaction snapshots the full cache list in memory.
    - Fix: stream from iterators or chunk snapshot to bounded buffers.
[ ] Stream split without full cache snapshot (Risk: MEDIUM)
    - Problem: split uses FULL_ISOLATION iterator backed by full list snapshot.
    - Fix: use streaming iterator or chunked splitting to cap memory.
[ ] Avoid full materialization in `IndexInternalConcurrent.getStream` (Risk: MEDIUM)
    - Problem: method loads all entries into a list before returning a stream.
    - Fix: return a streaming spliterator tied to iterator close.
[ ] Allow cache shrink after peaks (Risk: LOW)
    - Problem: `UniqueCache.clear()` keeps underlying `HashMap` capacity.
    - Fix: rebuild map on clear when size exceeds a threshold; add tests.
[ ] Add hard backpressure when maintenance is off or slow (Risk: MEDIUM)
    - Problem: if auto maintenance is disabled or too slow, delta caches grow
      indefinitely.
    - Fix: enforce hard caps (block or reject writes) and surface metrics.

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
