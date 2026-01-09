# Segment API: Flush, Compact, and Split

## Scope and assumptions

This document describes how to use the Segment API for maintenance operations.
The public Segment interface exposes `flush()` and `compact()`. Split is
orchestrated by the segment-index layer (`SegmentSplitCoordinator` +
`SegmentSplitter`) using `Segment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION)`.
Split behavior is documented here because it affects locking and iterator
semantics.

`Segment` is thread-safe by contract. `SegmentImplSynchronizationAdapter` is a
legacy lock-based wrapper that can be used for conservative serialization.
This document calls out the lock-based path explicitly where it applies.

This document has two parts: AS-IS (current behavior) and Target state (lock
minimization for throughput).

## AS-IS (legacy lock-based adapter)

### Lock model

Per-segment `ReentrantReadWriteLock` (via `SegmentImplSynchronizationAdapter`):

- Read lock: `get()`, `openIterator(FAIL_FAST)` per `hasNext()`/`next()` call.
- Write lock: `put()`, `flush()`, `compact()`, `invalidateIterators()`, `close()`.
- `openIterator(FULL_ISOLATION)` holds the write lock for its entire lifetime.

Optimistic iterator invalidation is driven by `VersionController` and used by
`openIterator(FAIL_FAST)`.

### A) Flush

What it does today (SegmentImpl):

- Snapshot write cache via `SegmentCache.getWriteCacheAsSortedList()`.
- Write entries to a delta cache file (`SegmentDeltaCacheWriter`).
- Merge write cache into delta cache and clear write cache.
- Bump version (via `openDeltaCacheWriter()`), invalidating fail-fast iterators.
- No-op when the write cache is empty.

When a new write arrives while `flush()` is running:

- The new write blocks on the write lock.

### B) Compact

What it does today (SegmentCompacter):

- Resets searchers and bumps the version (invalidates fail-fast iterators).
- Rewrites the full segment using a merged view (index + delta + write cache).
- `SegmentFullWriterTx` clears delta cache and write cache on commit.

When a new write arrives while `compact()` is running:

- The new write blocks on the write lock.

### C) Split (SegmentIndex)

What it does today (SegmentSplitCoordinator + SegmentSplitter):

- Triggered when `getNumberOfKeysInCache()` exceeds
  `maxNumberOfKeysInSegmentCache` (or the coordinator threshold).
- May run `segment.compact()` first when the policy says the segment is small
  or likely full of tombstones, then re-evaluates the plan.
- Acquires the segment write lock for the entire split when the segment is a
  `SegmentImplSynchronizationAdapter`.
- Calls `segment.invalidateIterators()` to terminate fail-fast iterators.
- Opens `openIterator(FULL_ISOLATION)` and streams a merged view
  (index + delta cache + write cache). Tombstones are filtered out by the
  iterator (`MergeDeltaCacheWithIndexIterator`).
- Writes the first half into a new lower segment. Remaining entries go into a
  new upper segment. If there are no remaining entries, the result is a
  compaction: the lower segment replaces the current segment.
- Replaces on-disk files and updates `KeySegmentCache`, then closes the old
  segment instance.

When a new write arrives while split is running:

- The new write blocks on the write lock for the full split duration.

When `get()` or iterators run during split:

- `get()` and fail-fast iterators block on the write lock while split holds it.
- Fail-fast iterators are explicitly invalidated at split start.
- FULL_ISOLATION iterators hold the write lock, so split waits until they
  close.

### Backpressure / overload

The segment itself does not enforce a hard size cap. `UniqueCache` is
unbounded, so if writes outpace maintenance, memory can grow.

Current index-level handling (SegmentIndexImpl):

- When `getNumberOfKeysInWriteCache()` reaches
  `maxNumberOfKeysInSegmentWriteCache`, the index triggers `flush()`.
- When `getNumberOfKeysInCache()` exceeds
  `maxNumberOfKeysInSegmentCache`, the index tries `split()`, and falls back
  to `flush()` if splitting is not possible.

If write load still exceeds flush/compact throughput, pick one:

- Apply backpressure at the index level (for example, the bounded queue in
  `IndexInternalSynchronized`).
- Lower thresholds to flush/split earlier.
- Add explicit write-cache caps or throttling around `Segment.put()`.

### Parallel calls (flush vs compact vs split)

Same segment:

- `flush()`, `compact()`, and split are exclusive. The write lock serializes
  them, so parallel calls run one after another.
- Split uses `FULL_ISOLATION` iterators and is wrapped in the write lock by
  `SegmentSplitCoordinator` when the segment is a
  `SegmentImplSynchronizationAdapter`.

Different segments:

- Maintenance can run in parallel (each segment has its own lock).
- Split takes a registry lock only during file rename; it does not block
  other segments otherwise.

### Reads, get(), and iterators

- Multiple `get()` calls can run concurrently.
- `openIterator(FAIL_FAST)` can run concurrently with `get()` and other
  fail-fast iterators, but any mutation (`put`, `flush`, `compact`, `split`)
  invalidates it via the version counter.
- `openIterator(FULL_ISOLATION)` holds the write lock until closed, blocking
  writes, flush, compact, and split on that segment.

### Corner cases to call out

- Fail-fast iterators must be expected to end early after any mutation.
- FULL_ISOLATION iterators must always be closed; otherwise writers can stall.
- `flush()` is a no-op with an empty write cache.
- `compact()` clears both delta cache and write cache; callers should not
  expect pending writes to remain after compaction.
- Split can degrade to compaction when no entries remain for the upper segment.
- Split throws if the plan is not feasible (estimated keys too low).
- Split can be skipped when `hasLiveEntries()` finds no live entries (empty or
  tombstone-only segment).
- Split closes and replaces the current segment instance; stale references to
  the old segment must not be reused.
- Calls on a closed segment should no-op (the adapter checks `wasClosed()`).
- Version overflow throws in `VersionController`; long-running services should
  monitor for it.

### Lock summary (how many locks are needed)

- One per-segment read/write lock for API calls
  (`SegmentImplSynchronizationAdapter`).
- One optimistic version counter for fail-fast iterator invalidation
  (`VersionController`).
- Segment registry exposes `executeWithRegistryLock()` for split file
  replacement, but the current `SegmentRegistry` does not implement a real
  registry lock.

## Target state (lock-minimized, high throughput)

### Goals

- Keep write locks only for short, deterministic swaps and version bumps.
- Run IO-heavy work (sorting, building files) outside segment locks.
- Allow writes and most reads to continue during flush/compact/split.
- Bound memory growth under sustained write load.

### Target lock model

- Short write lock for cache rotation, version capture, and file/reference
  swap.
- Background maintenance uses frozen snapshots; no long-held
  `FULL_ISOLATION` locks for heavy work.
- A per-segment maintenance state (or mutex) serializes flush/compact/split
  without blocking normal writes.

### A) Flush (target)

- Under write lock: swap (freeze) the write cache and capture a version.
- Release lock: sort and write the frozen snapshot to the delta cache file.
- Under write lock: merge metadata and drop the frozen snapshot handle.
- Writes continue into the new write cache throughout the flush.

### B) Compact (target)

- Under write lock: freeze the merged in-memory view (delta + write cache),
  capture a version, and redirect writes to a fresh cache.
- Release lock: rebuild new on-disk files from the snapshot plus current index.
- Optionally replay post-freeze writes before swap (or keep them in the active
  write cache) to avoid data loss.
- Under write lock: validate version and swap files; clear obsolete delta files
  and reset searchers.

### C) Split (target)

- Use the freeze + redirect + replay pattern (see
  `docs/development/segment-splitting-lock-minimization.md`).
- Under write lock: freeze the write cache, record a split version, and
  redirect writes to a fresh cache.
- Release lock: run the split pipeline against the frozen snapshot.
- Replay post-freeze writes into the new lower/upper segments.
- Under write lock: verify version and swap references; on conflict, retry.

### Backpressure / overload (target)

- Bound the number/size of frozen snapshots per segment.
- If backlog exceeds limits, apply throttling or spill to disk.
- Run maintenance on a background executor and prioritize older snapshots.

### Parallel calls (target)

- One maintenance task per segment at a time; other requests coalesce or wait.
- Flush requests during split should attach to replay or run after split.
- Maintenance can still run in parallel across different segments.

### Reads, get(), and iterators (target)

- `get()` should read from current caches and files without blocking on long
  maintenance tasks.
- Fail-fast iterators continue to invalidate on version changes.
- Prefer snapshot-based iterators for long scans instead of holding write
  locks for `FULL_ISOLATION`.

### Corner cases (target)

- Version mismatch at swap time should trigger retry or fallback.
- Large post-freeze write backlog should trigger backpressure or replay caps.
- Segment replacement must be atomic in the registry to avoid stale reads.
- Iterators opened before a swap should either complete on their snapshot or
  fail fast.

### Lock summary (target)

- Per-segment write lock for short cache/file swaps.
- Per-segment maintenance state to serialize flush/compact/split work.
- Optimistic version counter for iterator invalidation.
- Registry lock for file replacement if added in the future.
