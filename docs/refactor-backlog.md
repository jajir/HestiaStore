# Refactor backlog

## Active

## Planned

### OOM-related (sorted by severity)

#### High


#### Medium
[ ] 5 Stop materializing merged cache lists on read (Risk: MEDIUM)
    - Problem: `SegmentReadPath.openIterator` calls `getAsSortedList`, building
      full merged lists for each iterator.
    - Fix: provide streaming merge iterator over delta/write caches without
      full list materialization.
    - Options:
      - Option A (recommended): switch `UniqueCache` to `TreeMap` /
        `ConcurrentSkipListMap`, add a sorted iterator API, and merge cache
        iterators (write/frozen/delta) with `MergedEntryIterator` in the
        FULL_ISOLATION path.
      - Option B: keep `HashMap` / `ConcurrentHashMap` for get/put and maintain
        a sorted key index (`TreeSet` / `ConcurrentSkipListSet`) for iteration;
        expose a sorted iterator over keys + map values and merge like Option A.
[ ] 6 Stream compaction without full cache snapshot (Risk: MEDIUM)
    - Problem: compaction snapshots the full cache list in memory.
    - Fix: stream from iterators or chunk snapshot to bounded buffers.
[ ] 7 Stream split without full cache snapshot (Risk: MEDIUM)
    - Problem: split uses FULL_ISOLATION iterator backed by full list snapshot.
    - Fix: use streaming iterator or chunked splitting to cap memory.
[ ] 8 Avoid full materialization in `IndexInternalConcurrent.getStream` (Risk: MEDIUM)
    - Problem: method loads all entries into a list before returning a stream.
    - Fix: return a streaming spliterator tied to iterator close.
[ ] 9 Add eviction for heavy segment resources (Risk: MEDIUM)
    - Problem: `SegmentResourcesImpl` caches bloom/scarce forever.
    - Fix: tie resource lifetime to segment eviction or add per-resource LRU;
      ensure invalidate/close releases memory.

#### Low

[ ] 10 Allow cache shrink after peaks (Risk: LOW)
    - Problem: `UniqueCache.clear()` keeps underlying `HashMap` capacity.
    - Fix: rebuild map on clear when size exceeds a threshold; add tests.

### Other refactors (non-OOM)

[ ] 11 Make segment replacement atomic (Risk: HIGH)
    - Define crash-safe swap protocol (temp names, fsync, rename order).
    - Implement rename-then-delete flow and keep old files until swap commits.
    - Add recovery logic for partial swaps and tests with crash/failure hooks.
[ ] 12 Tie `index.map` updates to segment file swaps (Risk: HIGH)
    - Introduce a marker/txn file with PREPARED/COMMITTED states.
    - Write map changes to temp + fsync, then atomically swap on COMMIT.
    - Reconcile marker on startup to roll forward/backward; add tests.
[ ] 13 Implement a real registry lock (Risk: MEDIUM)
    - Add an explicit lock around registry mutations + file ops.
    - Replace/rename `executeWithRegistryLock` to actually serialize callers.
    - Add tests for split/compact interleaving and segment visibility.
[ ] 14 Replace common-pool async with dedicated executor + backpressure (Risk: MEDIUM)
    - Add/configure a dedicated executor for async API calls.
    - Track in-flight tasks and wait on close; add queue/backpressure limits.
    - Add tests for saturation, cancellation, and close ordering.
[ ] 15 Define `IndexAsyncAdapter.close()` behavior (Risk: MEDIUM)
    - Decide on wait vs non-blocking close and document it.
    - Add tests that match the chosen contract.
[ ] 16 Replace busy-spin loops with retry+backoff+timeout (Risk: MEDIUM)
    - Use `IndexRetryPolicy` in `SegmentsIterator` and split iterator open.
    - Add interrupt handling and timeout paths with clear error messaging.
    - Add tests for BUSY loops and timeout behavior.
[ ] 17 Stop returning `null` on CLOSED in `SegmentIndexImpl.get` (Risk: MEDIUM)
    - Decide API surface (exception vs status/Optional).
    - Update callers and docs to distinguish "missing" vs "closed".
    - Add tests for CLOSED/ERROR paths.
[ ] 19 Propagate MDC context to async ops and stream consumption (Risk: LOW)
    - Capture MDC context on submit and reapply in async tasks.
    - Wrap stream/iterator consumption with MDC scope; clear on close.
    - Add tests asserting `index.name` appears in async logs.

### Segment-per-directory refactor plan (small steps)

Goal: each segment lives under its own folder (e.g. `segment-00001/`). Keep
steps small and introduce feature flags/migration where needed.

## Ready

- (move items here when they are scoped and ready to execute)

## Deferred (segment scope, do not touch now)

[ ] 20 - segment: from segment index do not call flush; only user or segment decides.
[ ] 21 - segment: add SegmentSyncAdapters wrapper to retry BUSY with backoff until OK or throw on ERROR/CLOSED.
[ ] 22 - segment: add configurable BUSY timeout to avoid infinite wait (split waits).
[ ] 23 - segment: avoid file rename for flush/compact switching; point index to new version.
[ ] 24 - segment: consider segment per directory.

## In Progress

- (move items here when actively working)

## Done (Archive)

- (keep completed items here; do not delete)




[x] 1 everiwhere rename maxNumberOfKeysInSegmentWriteCacheDuringFlush to maxNumberOfKeysInSegmentWriteCacheDuringMaintenance including all configurations setter getter all all posssible usages.
[x] 2 Wnen write cache reach size as maxNumberOfKeysInSegmentWriteCacheDuringMaintenance than response to put with BUSY.
[x] 3 UniqueCache should not use read/write reentrant lock. It's property of concurrent hash map.
[x] 4 Enforce `maxNumberOfSegmentsInCache` in `SegmentRegistry` (Risk: MEDIUM)
    - Problem: segments are cached unbounded; memory grows as segments grow.
    - Fix: implement LRU or size-bounded cache; evict + close segments and
      invalidate resources on eviction.
[x] 18 Provide index-level FULL_ISOLATION streaming (Risk: MEDIUM)
    - Add overload or option to request FULL_ISOLATION on index iterators.
    - Implement iterator that holds exclusivity across segments safely.
    - Add tests for long-running scans during maintenance.
[x] 25 Create directory API and layout helpers (Risk: HIGH)
    - Add `Directory.openSubDirectory(String)` + `AsyncDirectory.openSubDirectory(String)`
      and lifecycle helpers `Directory.mkdir(String)` / `Directory.rmdir(String)`.
    - Implement in `FsDirectory`, `AsyncDirectoryAdapter`, and in-memory
      `MemDirectory` equivalents; define semantics for non-empty rmdir.
    - Add `SegmentDirectoryLayout` (or similar) that builds names for:
      index, scarce, bloom, delta, properties, and lock files.
    - Add tests for directory creation and layout mapping.

[x] 26 Introduce segment-rooted `SegmentFiles` (Risk: HIGH)
    - Add a `SegmentFiles` constructor that accepts a segment root
      `AsyncDirectory` (instead of a flat base directory + id).
    - Keep legacy flat layout working (auto-detect existing files, or flag in
      `SegmentBuilder`).
    - Update `SegmentBuilder` to create/use the segment root directory.
    - Add tests that both layouts open the same data correctly.

[x] 27 Add per-segment `.lock` file (Risk: MEDIUM)
    - Add `segment.lock` (or `.lock`) inside the segment directory.
    - Acquire lock on segment open; release on close. Fail fast on lock held.
    - Add stale-lock recovery policy (manual delete or metadata timestamp).
    - Add tests for lock contention and cleanup.

[x] 28 Shared properties file structure (Risk: MEDIUM)
    - Introduce a common property schema used by segment + segmentindex
      packages (e.g. `IndexPropertiesSchema`).
    - Store schema version and required keys; add migration helpers.
    - Update `SegmentPropertiesManager` and `IndexConfiguratonStorage`
      to use the shared schema.

[x] 29 Compact flow for directory layout (publish protocol) (Risk: HIGH)
    - IO phase (`MAINTENANCE_RUNNING`):
      - Create a new directory, e.g. `segment-00001.next/` or versioned
        `segment-00001/v2/`.
      - Write new index/scarce/bloom/cache files there.
      - Write properties with state `PREPARED` + metadata.
    - Publish phase (short `FREEZE`):
      - Mark new directory as `ACTIVE` in properties (or update a pointer
        file `segment-00001.active`).
      - Reload `SegmentFiles`/`SegmentResources` to the new root.
      - Bump version and return to `READY`.
    - Cleanup:
      - Delete old directory only after publish and resource reload.
      - Add startup recovery for `PREPARED` without `ACTIVE`.
    - Align with items 11/12 (atomic swaps + map updates).

[x] 30 Split + replace updates (Risk: HIGH)
    - Update split/rename logic to use directory swaps or pointer updates.
    - Ensure registry + `segmentindex` metadata remain consistent.
    - Add tests for crash recovery and partial swaps.
