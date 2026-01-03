# Segment Split Lock Minimization

## Principle

The split path should keep the segment write lock only for short, deterministic
operations (swapping references and reading counters). All heavy work
collecting entries, sorting, building new segment files, and writing them to
disk must happen outside the lock. This minimizes blocking for writers and
reduces tail latency during high write load.

Scope note: this document targets process (3) *split* only. Flush and compaction
already can be implemented with short locks or background work; the split path
is the current long‑lock bottleneck.

The recommended strategy is "freeze + redirect + replay":

1) Freeze the current write cache under a short write lock, record a version,
   and redirect new writes to a fresh write cache.
2) Release the lock and perform the split using the frozen snapshot.
3) Before swapping in the split result, replay the writes that arrived after
   the freeze into the new segments and then perform a short swap under lock.

This is the pattern used by LSM based systems where memtables are rotated and
flushed in the background.

## Proposed Segment API Surface

Keep the public `Segment` API minimal. Splitting should not be part of the
public segment contract. Instead, introduce an internal split support contract
inside the `segment` package that `SegmentImpl` implements and the split
coordinator uses.

Split is initiated from the segment-index layer (not from the segment):

- `SegmentIndexImpl` decides *when* a split is needed based on write cache
  thresholds and total key counts.
- `SegmentSplitCoordinator` orchestrates the split pipeline and calls the
  internal `SegmentSplitSupport` methods on the target segment.
- `SegmentImpl` provides the split snapshot and applies the final swap, but
  never decides *when* to split.

Suggested internal API (package-private):

- `SegmentSplitSnapshot<K,V> freezeForSplit()`
  - Captures a version and the frozen write cache snapshot.
  - Swaps to a fresh write cache for ongoing writes.
- `List<Entry<K,V>> getPostSplitWrites()`
  - Returns the current write cache entries that arrived after the freeze.
- `boolean applySplit(SegmentSplitSnapshot<K,V> snapshot, SegmentSplitResult<K,V> result)`
  - Verifies the split version and swaps in the new segment files.
  - Returns `false` if the version changed and the split must be retried.

Split versioning should reuse the same monotonic counter used by
`OptimisticLock`/`VersionController` so both iterators and splits observe the
same mutation boundaries.

## Major Code Changes Required

- Add `SegmentSplitSnapshot` and `SegmentSplitResult` types in the `segment`
  package.
- Add a package-private `SegmentSplitSupport` interface in `segment` and have
  `SegmentImpl` implement it.
- Add `SegmentCache.freezeWriteCache()` to atomically swap the write cache and
  return a snapshot for splitting.
- Update `SegmentSplitCoordinator` to use `freezeForSplit()` and
  `applySplit(...)` rather than calling split logic on the public `Segment`
  interface.
- Ensure split version increments whenever a mutation can change split input
  (writes, compaction, file swaps).

## Required Steps (High Level)

- Add a "split state" to each segment that can freeze the write cache and
  record a split version.
- Ensure new writes are redirected to a new write cache while a split runs.
- Run split pipeline from the frozen snapshot outside the lock.
- Replay post-freeze writes into the new segments before final swap.
- Keep the lock hold time bounded to cache swaps and metadata updates.

## Implementation Details

### 1) Freeze and Versioning

Add a new method on the segment cache that returns a frozen snapshot and
replaces the write cache with a fresh one:

- `SegmentCache.freezeWriteCache()` -> returns `List<Entry<K, V>>` or a
  `UniqueCache<K, V>` snapshot.
- Update a per-segment `splitVersion` counter when freezing.
- Writers continue to use the new write cache.

Use the existing version controller or optimistic lock (if present) to capture
the split version and to detect a conflicting mutation before final swap.

### 2) Split Pipeline Changes

Update `SegmentSplitCoordinator` to:

- Acquire segment write lock.
- Call `freezeWriteCache()` and capture `splitVersion`.
- Release lock.
- Run split pipeline using the frozen snapshot outside the lock.
- Build new segments using `SegmentBuilder.openWriterTx()` and
  `SegmentFullWriterTx` (already available).

### 3) Replay Writes After Freeze

Collect writes that arrived after the freeze and replay them into the new
segments before the final swap:

- A "post-split" write cache is naturally the new write cache created during
  freeze. Use its contents to replay.
- Replay must use the split boundaries produced by the pipeline so each key
  is routed to the correct new segment.
- After replay, clear the post-split cache.

If the segment version changed in a way that invalidates the snapshot, retry
the split using a new freeze.

### 4) Short Swap and Cleanup

Acquire the segment write lock to:

- Validate the split version (no unexpected conflicts).
- Swap the old segment files with the newly built files.
- Update registry and in-memory references.
- Release lock quickly.

Only after the swap succeeds should old segment files be removed.

### 5) Observability and Backpressure

Add metrics and logging for:

- Split duration (total and lock-held time).
- Number of retries.
- Size of frozen snapshot and replay cache.

Backpressure policy should use snapshot size and replay backlog rather than
blocking on the split work itself.

## Implementation Plan (Phased)

Phase 0: Instrumentation

- Add timing counters for split phases and lock hold time.
- Add debug logs guarded by a feature flag.

Phase 1: Cache Freeze API

- Implement `SegmentCache.freezeWriteCache()`.
- Add a `splitVersion` counter to the segment.
- Adjust segment writes to use the new write cache after freeze.

Phase 2: Split Pipeline Refactor

- Update `SegmentSplitCoordinator` to freeze, release lock, and run split
  outside the lock.
- Build new segments using `SegmentBuilder.openWriterTx()`.
 - Use internal `SegmentSplitSupport` instead of public `Segment` methods.

Phase 3: Replay and Swap

- Replay post-freeze writes into the split result.
- Short lock for final swap.
- Add conflict detection and retry.

Phase 4: Tests

- Unit tests for freeze behavior, replay routing, and version conflicts.
- Concurrency tests that split while writes continue.
- Integration tests that verify durability after reopen.

Phase 5: Documentation

- Update write path and segment docs if the design changes are accepted.

## Implementation Checklist

- [ ] Add `SegmentSplitSnapshot` and `SegmentSplitResult` types in `segment`.
- [ ] Add internal `SegmentSplitSupport` interface and implement in `SegmentImpl`.
- [ ] Add split version counter per segment and expose it for split coordination.
- [ ] Add `SegmentCache.freezeWriteCache()` and ensure writers redirect to a new cache.
- [ ] Update `SegmentSplitCoordinator` to freeze under lock and run the split outside the lock.
- [ ] Build new segments via `SegmentBuilder.openWriterTx()` using the frozen snapshot.
- [ ] Replay post-freeze writes into the split result before swap.
- [ ] Add conflict detection and retry on version mismatch.
- [ ] Short lock swap for file/reference replacement and registry updates.
- [ ] Metrics/logging for split duration, lock hold time, and retries.
- [ ] Unit tests for freeze/redirect, replay routing, and version conflicts.
- [ ] Concurrency tests for split while writes continue.
- [ ] Integration tests for close/reopen durability after split.

## Complexity and Testing Effort

- Implementation complexity: medium to high.
- Testing complexity: high (requires concurrency and retry tests).

The upside is sustained write throughput under heavy splits and predictable
lock hold time. This approach is the most aligned with long term performance
and correctness goals.
