# Refactor backlog

## Active

[x] 11 Remove `segmentState` from segment properties schema (Risk: MEDIUM)
    - Remove `SegmentKeys.SEGMENT_STATE` from `IndexPropertiesSchema`.
    - Update `SegmentPropertiesManager` to drop `getState`/`setState` usage.
    - Decide migration behavior for existing properties files.
[x] 12 Add `getMaxNumberOfDeltaCacheFiles()` to `Segment` (Risk: LOW)
    - Implement in `SegmentImpl`.
    - Update any callers/tests that need the accessor.
[x] 13 Add `maxNumberOfDeltaCacheFiles` to `IndexConfiguration` + builder (Risk: MEDIUM)
    - Add config property, validation, defaults, and persistence.
    - Plumb through `SegmentBuilder`/`SegmentConf` as needed.
[x] 14 Wire delta cache file cap into `SegmentMaintenancePolicyThreshold` (Risk: MEDIUM)
    - Add the max file count to policy constructor/state.
    - Pass the value from configuration.
[x] 15 Enforce delta cache file cap in policy (Risk: MEDIUM)
    - In `SegmentMaintenancePolicyThreshold` (~line 44), trigger maintenance
      when delta cache file count exceeds the cap.
[ ] 16 Enforce segment lock test on open (Risk: MEDIUM)
    - Add a test that opening a segment with an existing `.lock` fails.
    - Cover both in-memory and filesystem-backed directories.
[ ] 17 Document locked-directory behavior in `SegmentBuilder` (Risk: LOW)
    - Clarify how builder reacts when the segment directory is already locked.
[ ] 18 Acquire segment lock before `prepareBuildContext()` (Risk: MEDIUM)
    - Move lock acquisition to the start of `SegmentBuilder.build()`.
    - Ensure failures release locks and leave no partial state.

## Planned

### High

### Medium

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
[ ] 41 Unify async execution for segment index (Risk: MEDIUM)
    - Route `SegmentIndexImpl.runAsyncTracked` and `IndexAsyncAdapter.runAsyncTracked`
      through a shared, dedicated executor (no common pool).
    - Decide whether to keep both async layers or make one delegate to the other.
    - Align async close behavior and document rejection/backpressure outcomes.
[ ] 42 Revisit `SegmentAsyncExecutor` rejection policy (Risk: MEDIUM)
    - Ensure maintenance IO never runs on caller threads.
    - Choose `AbortPolicy` + BUSY/error mapping or custom handler.
    - Update docs and metrics if behavior changes.
[ ] 43 Replace registry close polling with completion signal (Risk: MEDIUM)
    - Add a close completion handle or signal in `Segment`.
    - Update `SegmentRegistry.closeSegmentIfNeeded` to wait on completion rather
      than polling `getState()`.
    - Ensure close-from-maintenance thread does not deadlock.
[ ] 44 Normalize split close/eviction flow (Risk: MEDIUM)
    - Centralize segment close/eviction in `SegmentRegistry`.
    - Remove direct `segment.close()` calls from split coordinator.
    - Ensure split outcome updates mapping, eviction, and close are ordered.
[ ] 45 Replace spin-wait in `SegmentConcurrencyGate.awaitNoInFlight` (Risk: LOW)
    - Use `wait/notify` or `ManagedBlocker` with timeout.
    - Preserve FREEZE semantics and early exit on state change.
    - Add tests for drain behavior under load.
[ ] 46 Align iterator isolation naming and semantics (Risk: LOW)
    - Choose between `FAIL_FAST`/`FULL_ISOLATION` and the legacy
      `INTERRUPT_FAST`/`STOP_FAST` terminology.
    - Update docs, comments, and any mapping code consistently.
[ ] 47 Consolidate BUSY/CLOSED retry loops (Risk: LOW)
    - Extract shared retry helper for segmentindex operations.
    - Replace ad-hoc loops in `SegmentRegistry`, `SegmentSplitCoordinator`,
      and `SegmentIndexImpl`.
    - Keep backoff/timeout semantics and error messages consistent.

### Testing/Quality

[ ] 48 Test executor saturation and backpressure paths (Risk: MEDIUM)
    - Add tests for `SegmentAsyncExecutor` queue saturation and rejection handling.
    - Add tests for `SplitAsyncExecutor` rejection and in-flight cleanup.
    - Verify maintenance IO never runs on caller threads.
[ ] 49 Test close path interactions (Risk: MEDIUM)
    - Close while segment is `MAINTENANCE_RUNNING` and ensure backoff/timeout works.
    - Close during async operations should fail fast with clear error.
    - Assert no deadlock when waiting for segment READY/CLOSED.
[ ] 50 Test split failure cleanup (Risk: MEDIUM)
    - Force exceptions in split steps and assert `splitsInFlight` clears.
    - Validate directory swap and key-to-segment map remain consistent.
    - Ensure resources/locks are released on failure.
[ ] 51 Test maintenance failure transitions (Risk: MEDIUM)
    - Inject failures in maintenance IO and publish phases.
    - Assert segment moves to `ERROR` and callers see ERROR status.
    - Verify rejection handling does not leave the segment in FREEZE.

## Ready

- (move items here when they are scoped and ready to execute)

## Deferred (segment scope, do not touch now)

[ ] 20 - segment: from segment index do not call flush; only user or segment decides.
[ ] 21 - segment: add SegmentSyncAdapters wrapper to retry BUSY with backoff until OK or throw on ERROR/CLOSED.
[ ] 22 - segment: add configurable BUSY timeout to avoid infinite wait (split waits).

## In Progress

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
[x] 23 Refactor `Segment.close()` to async fire-and-forget with READY-only entry (Risk: MEDIUM)
    - Change `Segment` to drop `CloseableResource` and return
      `SegmentResult<Void>` from `close()`.
    - Close starts only in `READY`: transition to `FREEZE`, drain, optionally
      flush write cache, then run close work on maintenance thread.
    - Completion marks `CLOSED`, releases locks/resources, and stops admissions.
    - Move close-state tracking into segment index (avoid `Segment.wasClosed()`).
    - Update state machine/gate/docs/tests to match the new close lifecycle.
[x] 24 Add integration test: in-memory segment lock prevents double-open (Risk: LOW)
    - Create an integration test that opens a segment in a directory and
      asserts a second open in the same directory fails (lock enforcement).
[x] 25 Simplify `Segment.flush()`/`compact()` to return status only (Risk: MEDIUM)
    - Remove `CompletionStage` return values from `flush()` and `compact()`.
    - Operation completion is observable when segment state returns to `READY`.
    - Update callers, docs, and tests that wait on completion stages.
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
[x] 31 Segment layout uses versioned file names in a single directory (Risk: HIGH)
    - Name index/scarce/bloom/delta as `segment-00000-vN.*` (v0 keeps legacy).
    - Store the active version in `segment-00000.properties` (no `.active` pointer).
    - Keep legacy unversioned files readable as version 0.
[x] 32 Builder/files treat the provided directory as the segment home (Risk: HIGH)
    - Require `Segment.builder(AsyncDirectory)` for construction.
    - Lock + properties live inside the segment directory.
    - Resolve active version from properties or detected index files.
[x] 33 Compaction/flush publish is memory-only (Risk: HIGH)
    - IO phase writes versioned files and property updates.
    - Publish swaps in-memory version/resources and bumps iterator version.
    - Cleanup old version files asynchronously.
[x] 34 Registry/tests align with single-directory versioning (Risk: MEDIUM)
    - Registry passes segment directories; no active-directory switching.
    - Update tests to accept versioned names and per-segment directories.
[x] 35 Remove unused close monitor in `SegmentConcurrencyGate` (Risk: LOW)
    - Remove `closeMonitor` and `signalCloseMonitor` since nothing waits on it.
    - Keep drain behavior in `awaitNoInFlight()` unchanged.
[x] 36 Consolidate in-flight read/write counters in `SegmentConcurrencyGate` (Risk: LOW)
    - Replace `inFlightReads`/`inFlightWrites` with a single counter.
    - Keep admission rules and drain behavior unchanged.
    - Update any stats or tests that rely on read/write split (if introduced).
[x] 37 Audit `segment` package for unused or test-only code (Risk: LOW)
    - Identify unused classes/methods/fields.
    - Remove code only referenced by tests or move test helpers into test scope.
    - Ensure public API docs and tests remain consistent after cleanup.
[x] 38 Audit `segmentindex` package for unused or test-only code (Risk: LOW)
    - Identify unused classes/methods/fields.
    - Remove code only referenced by tests or move test helpers into test scope.
    - Ensure public API docs and tests remain consistent after cleanup.
[x] 39 Review `segment` package for test and Javadoc coverage (Risk: LOW)
    - Ensure each class has a JUnit test or document why coverage is excluded.
    - Ensure each public class/method has Javadoc; add missing docs.
[x] 40 Review `segmentindex` package for test and Javadoc coverage (Risk: LOW)
    - Ensure each class has a JUnit test or document why coverage is excluded.
    - Ensure each public class/method has Javadoc; add missing docs.
