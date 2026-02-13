# Refactor backlog

## Active

[ ] 59.1 Concurrency: remove lock-order inversion in core ops (Risk: HIGH)
    - `SegmentIndexCore.get/put`: avoid holding key-map read lock while calling
      `SegmentRegistry.getSegment` or touching segments.
    - Use key-map snapshot + version re-check on retry/BUSY paths.
    - Tests: `IntegrationSegmentIndexConcurrencyTest` + new split/put stress.

## Planned

### High

### Medium

[ ] 54 Dedicated executor for index async ops (Risk: MEDIUM)
    - Use a dedicated, bounded executor for `SegmentIndexImpl.runAsyncTracked`
      (no common pool).
    - Define rejection policy: map saturation to BUSY/error with clear message.
    - Ensure close waits for in‑flight async work or cancels safely.
    - Tests: saturation/backpressure, close ordering, no caller‑thread IO.

[ ] 55 Replace busy spin loops with retry + jitter (Risk: MEDIUM)
    - Replace `Thread.onSpinWait`/busy loops in split iterator open and other
      retry paths with `IndexRetryPolicy` + jitter.
    - Make timeouts explicit and surface `IndexException` with operation name.
    - Tests: BUSY retry exits on READY, timeout path, interrupt handling.

[ ] 56 Key‑to‑segment map read contention reduction (Risk: MEDIUM)
    - Evaluate snapshot‑based reads or `StampedLock` for high‑read workloads.
    - Keep version validation semantics intact for split/extend paths.
    - Tests: concurrent get/put under splits, no missing mappings, no deadlocks.

[ ] 57 Streaming iterators without full materialization (Risk: MEDIUM)
    - Replace list materialization in `getStream`/FULL_ISOLATION with streaming
      merge iterators over write/delta caches and segment files.
    - Ensure iterator close releases resources and does not leak locks.
    - Tests: large data set memory profile, iterator isolation correctness.

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

## Maintenance tasks

[ ] M37 Audit `segment` package for unused or test-only code (Risk: LOW)
    - Limit class, method and variables visiblity
    - Identify unused classes/methods/fields.
    - Remove code only referenced by tests or move test helpers into test scope.
    - Ensure public API docs and tests remain consistent after cleanup.
[ ] M38 Review `segment` package for test and Javadoc coverage (Risk: LOW)
    - Ensure each class has a JUnit test or document why coverage is excluded.
    - Ensure each public class/method has Javadoc; add missing docs.
[ ] M39 Audit `segmentindex` package for unused or test-only code (Risk: LOW)
    - Limit class, method and variables visiblity
    - Identify unused classes/methods/fields.
    - Remove code only referenced by tests or move test helpers into test scope.
    - Ensure public API docs and tests remain consistent after cleanup.
[ ] M40 Review `segmentindex` package for test and Javadoc coverage (Risk: LOW)
    - Ensure each class has a JUnit test or document why coverage is excluded.
    - Ensure each public class/method has Javadoc; add missing docs.
[ ] M41 Audit `segmentregistry` package for unused or test-only code (Risk: LOW)
    - Limit class, method and variables visiblity
    - Identify unused classes/methods/fields.
    - Remove code only referenced by tests or move test helpers into test scope.
    - Ensure public API docs and tests remain consistent after cleanup.
[ ] M42 Review `segmentregistry` package for test and Javadoc coverage (Risk: LOW)
    - Ensure each class has a JUnit test or document why coverage is excluded.
    - Ensure each public class/method has Javadoc; add missing docs.
    - See `docs/development/segmentregistry-audit.md` for audit notes.

## Done (Archive)

- (keep completed items here; do not delete)

[x] 61.1 Wire `SegmentHandler` into key-to-segment map usage (Risk: HIGH)
    - Replace direct segment references in key-to-segment map paths with
      `SegmentHandler` usage.
    - Ensure handlers are used consistently for segment access in index flows.

[x] 61.2 Refactor split algorithm around handler locks (Risk: HIGH)
    - When a segment is eligible for split: acquire handler lock, re-check
      eligibility under lock, then either unlock or proceed with split.
    - Split apply ordering: update map on disk first, then in-memory map,
      then close old segment, delete files, and finally unlock.
    - Ensure failures unlock the handler and clean up temporary segments.
    - Update `docs/architecture/registry.md` to reflect handler-based locking.

[x] 61.3 Simplify `SegmentHandler` lock API (Risk: MEDIUM)
    - Keep internal handler state as `READY`/`LOCKED`.
    - `lock()` returns `SegmentHandlerLockStatus` with `OK` or `BUSY`.
    - Replace token-based lock/unlock usage across registry + split flows.
    - Update handler-related tests to match the new API.

[x] 60 Move registry implementation to `segmentregistry` package (Risk: MEDIUM)
    - Move `SegmentRegistryImpl`, `SegmentRegystryState`, `SegmentRegistryCache`,
      `SegmentRegistryState`, and `SegmentRegistryResult`
      to `org.hestiastore.index.segmentregistry`.
    - Update imports/usages in `segmentindex` and tests.
    - Keep public API surface the same; verify no package-private access leaks.

[x] M41 Audit `segmentregistry` package for unused or test-only code (Risk: LOW)
    - Limit class, method and variables visiblity
    - Identify unused classes/methods/fields.
    - Remove code only referenced by tests or move test helpers into test scope.
    - Ensure public API docs and tests remain consistent after cleanup.
[x] M42 Review `segmentregistry` package for test and Javadoc coverage (Risk: LOW)
    - Ensure each class has a JUnit test or document why coverage is excluded.
    - Ensure each public class/method has Javadoc; add missing docs.

[x] 59 Introduce `SegmentHandler` lock gate in segmentindex (Risk: HIGH)
    - Add `SegmentHandler` with `getSegment()` returning `SegmentHandlerResult`:
      `OK` (segment), `LOCKED`, and handler states `READY`/`LOCKED`.
    - `lock()` returns a privileged handle/token that allows access to the
      underlying segment while handler state is `LOCKED`.
    - `getSegment()` must return `LOCKED` while locked for all non-privileged
      callers (no segment exposure during lock).
    - Wire split flow to lock via handler before opening `FULL_ISOLATION`
      iterator, then unlock after apply/cleanup.
    - Add tests: `LOCKED` is returned during lock; lock holder can operate;
      unlock restores `OK`.

[x] 59.2 Concurrency: reduce redundant key-map read locks (Risk: MEDIUM)
    - Make `KeyToSegmentMapSynchronizedAdapter.snapshot()` lock-free
      (volatile snapshot + AtomicLong version).
    - Keep read locks only for map-only operations; do not wrap segment calls.
    - Tests: snapshot consistency + existing `KeyToSegmentMapTest`.

[x] 59.3 Concurrency: limit registry FREEZE to split apply (Risk: MEDIUM)
    - Remove `FreezeGuard` usage from `SegmentRegistryImpl.getSegment` create/
      eviction path; keep cache lock for LRU safety.
    - Reserve registry `FREEZE` for split apply only.
    - Tests: split + eviction concurrency (`SegmentRegistryCacheTest`,
      `SegmentSplitCoordinatorConcurrencyTest`, integration stress).

[x] 52 Remove automatic compaction from `segmentindex` (Risk: MEDIUM)
    - Drop pre-split compaction in `SegmentSplitCoordinator` and remove
      `SegmentSplitterPolicy.shouldBeCompactedBeforeSplitting` + related retry
      logic.
    - Simplify split planning to use estimated key counts directly (remove
      compaction/tombstone hints from `SegmentSplitterPolicy` or replace with a
      minimal estimate helper).
    - Keep `SegmentIndex.compact` / `compactAndWait` as the only
      segmentindex-triggered compaction entry point; update Javadocs to reflect
      compaction being handled inside the segment package otherwise.
    - Update tests that construct `SegmentSplitterPolicy` and add coverage that
      split does not call `Segment.compact` while user-invoked compaction still
      does.

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
[x] 16 Enforce segment lock test on open (Risk: MEDIUM)
    - Add a test that opening a segment with an existing `.lock` fails.
    - Cover both in-memory and filesystem-backed directories.
[x] 17 Document locked-directory behavior in `SegmentBuilder` (Risk: LOW)
    - Clarify how builder reacts when the segment directory is already locked.
[x] 18 Acquire segment lock before `prepareBuildContext()` (Risk: MEDIUM)
[x] 19 Add `SegmentRegistryResult` + status + adapters (Risk: MEDIUM)
    - Define result/status types and adapters to/from `SegmentResult`.
    - Unit tests only; no wiring.
[x] 20 Add registry state enum + gate (Risk: MEDIUM)
    - Define `SegmentRegistryState` and a small gate/state holder.
    - Unit tests only; no integration.
[x] 21 Introduce `SegmentRegistry` interface + `SegmentRegistryImpl` (Risk: MEDIUM)
    - Keep interface minimal and keep `SegmentResult` returns for now.
    - Rename existing class to impl and update call sites in same step.
[x] 22 Add `SegmentRegistrySyncAdapter` with BUSY retry (Risk: MEDIUM)
    - Wrap `SegmentRegistry` and retry BUSY (use `IndexRetryPolicy`).
[x] 23 Wire state gate into impl (Risk: HIGH)
    - BUSY only from registry state; FREEZE only around map changes.
    - Keep `SegmentResult` API to avoid broad changes.
[x] 24 Switch registry API to `SegmentRegistryResult` (Risk: HIGH)
    - Introduce `SegmentRegistryLegacyAdapter` to keep old callers working.
    - Migrate call sites/tests, then remove legacy adapter.
[x] 53.1 Split “apply” DTO (Risk: LOW)
    - Introduce a small DTO for split apply (oldId, lowerId, upperId,
      min/max keys, status).
    - Unit tests for DTO invariants.
[x] 53.2 Split worker extraction (Risk: MEDIUM)
    - Refactor split execution to: open FULL_ISOLATION iterator, run split on
      maintenance executor, return DTO without touching registry or map.
    - Ensure iterator is closed in all paths.
    - Unit tests for result wiring.
[x] 53.3 Registry apply entry point (Risk: MEDIUM)
    - Add registry apply method that (a) FREEZE, (b) update cache
      (remove old, add new ids), (c) exit FREEZE.
    - Keep key‑map lock separate.
    - Unit tests for cache mutation under FREEZE.
[x] 53.4 Key‑map persistence (Risk: MEDIUM)
    - Update key‑to‑segment map using its own lock/adapter.
    - Persist map file after in‑memory registry apply.
    - Tests that map persistence order is enforced.
[x] 53.5 Old segment deletion (Risk: MEDIUM)
    - Delete old segment directory only after map persistence and after
      iterator/segment locks are released.
    - Tests that deletion never happens before map persistence.
[x] 53.6 Lock order contract (Risk: LOW)
    - Enforce lock order (segment → registry → map; release map → registry
      → segment) and document in code.
    - Add a small test or assertion helper to catch order violations.
[x] 53.7 Split concurrency scenarios (Risk: HIGH)
    - Tests:
      - split does not run under registry FREEZE (short window)
      - split returns BUSY on lock conflict and retries safely
      - concurrent get/put during split never sees missing segment mapping
[x] 58.1 Split: keep split IO outside registry freeze (Risk: HIGH)
    - `SegmentSplitCoordinator.split(...)`: ensure all IO (iterator open, writes)
      happens before any registry `FREEZE`.
    - `SegmentSplitStepOpenIterator`: keep `FULL_ISOLATION` acquisition once per split.
    - `SegmentSplitCoordinator.hasLiveEntries(...)`: now uses `FAIL_FAST` to
      avoid a second `FULL_ISOLATION` lock.
    - Tests may fail if ordering assumptions change; fix after step 58.4.
[x] 58.2 Split: invert lock order for apply phase (Risk: HIGH)
    - `SegmentSplitCoordinator.applySplitPlan(...)`: remove outer
      `keyToSegmentMap.withWriteLock(...)`.
    - `SegmentRegistryImpl.applySplitPlan(...)`: acquire registry freeze first,
      then call `onApplied` which acquires key-map write lock.
    - Update lock-order enforcement flags to match registry -> key-map.
[x] 58.3 Split: propagate lock-order flags into key-map adapter (Risk: MEDIUM)
    - `KeyToSegmentMapSynchronizedAdapter`: set/clear `keyMapLockHeld` around
      write-lock acquisition when enforcement is enabled.
    - Ensure registry checks validate `registryLockHeld` before key-map lock.
[x] 58.4 Split: finalize apply/cleanup ordering (Risk: MEDIUM)
    - Ensure apply evicts old segment instance and closes it via
      `SegmentRegistryImpl.closeSegmentInstance(...)`.
    - Keep key-map flush outside registry freeze:
      `keyToSegmentMap.optionalyFlush()` only after apply OK.
    - Delete old segment files only after apply succeeds and locks released.
[x] 58.5 Split: test alignment (Risk: MEDIUM)
    - Add/update tests to assert no directory swap in split flow.
    - Add tests for enforced lock order (registry -> key-map).
    - Add tests for split failure cleanup of new segments.
[x] 63 SegmentIdAllocator in segmentregistry (Risk: MEDIUM)
    - Add `SegmentIdAllocator` interface and directory-backed implementation.
    - Scan `AsyncDirectory.getFileNamesAsync()` for segment directories named
      `segment-00001` (prefix `segment-` + 5 digits) and initialize next id
      to max+1 (or 1 when none found).
    - Allocate ids with thread-safe counter.

[x] 64 Include directories in `Directory.getFileNames()` (Risk: LOW)
    - Ensure `Directory.getFileNames()` returns subdirectory names as well.
    - Update `MemDirectory` to include subdirectory names in its stream.
    - Verify no tests rely on file-only behavior.

[x] 65 Remove id allocation from key-to-segment map (Risk: MEDIUM)
    - Remove `nextSegmentId` and `findNewSegmentId()` from `KeyToSegmentMap`
      and its synchronized adapter.
    - Remove updates to `nextSegmentId` in `tryExtendMaxKey`/`updateMaxKey`.

[x] 66 Wire allocator into registry + index (Risk: MEDIUM)
    - Update `SegmentRegistryImpl` to use `SegmentIdAllocator` instead of
      supplier.
    - Update `SegmentIndexImpl` wiring and split coordinator to use registry
      allocation only.
    - Update tests to stub allocator or use directory-backed allocator.

[x] 67 Tests + docs for allocator move (Risk: LOW)
    - Add allocator tests (empty dir, max id, thread-safety).
    - Update `docs/architecture/registry.md` to reflect registry allocator.

[x] 62 Add `SegmentRegistryBuilder` modeled after `Segment.builder(...)` (Risk: MEDIUM)
    - Add `SegmentRegistryBuilder` in `segmentregistry` with required inputs
      (directory, type descriptors, config, maintenance executor).
    - Provide optional setters for `SegmentIdAllocator` and `SegmentFactory`.
    - Add static factory `SegmentRegistry.builder(...)` (or on impl) to return builder.
    - Move default wiring (factory + allocator creation) into builder.
    - Keep `SegmentRegistryImpl` constructor with full DI for tests.
    - Update `SegmentIndexImpl` (and other callers) to use the builder.
    - Add unit tests for missing required fields and default wiring.

[x] 68 Align split apply with registry FREEZE + lock-order enforcement (Risk: MEDIUM)
    - Expose registry FREEZE in `SegmentRegistryAccess` (or equivalent) so
      split apply can run under FREEZE while holding handler + key-map locks.
    - While FREEZE is active, set `hestiastore.registryLockHeld=true` so
      key-map lock order enforcement can be enabled safely.
    - Wrap key-map apply + cache eviction inside the FREEZE window.

[x] 69 Separate cache eviction from file deletion in split apply (Risk: MEDIUM)
    - Add registry operation to evict a specific segment from cache while the
      handler lock is held (no file deletion).
    - After apply: evict old segment under handler+FREEZE, release iterator,
      unlock handler, then delete old segment files via registry helper.
    - Keep `deleteSegment` behavior for general callers unchanged.

[x] 70 Apply-failure should mark registry ERROR (Risk: LOW)
    - When split apply fails mid-update, set registry gate to ERROR and
      surface the failure (avoid silent BUSY loops).
    - Add tests for apply-failure transitions.

[x] 71 SegmentRegistry: expose NOT_FOUND for missing segments (Risk: LOW)
    - Add `NOT_FOUND` to `SegmentRegistryResultStatus` + factory method.
    - Return NOT_FOUND when `getSegment` targets a missing directory.
    - Keep `createSegment` creating new segments even when others exist.
    - Tests: missing-segment lookup, status plumbing.

[x] 72 SegmentRegistryBuilder: configure only via `with*` methods (Risk: LOW)
    - Remove constructor parameters from `SegmentRegistryBuilder`.
    - Ensure all required inputs are set via `with...` methods.
    - Update call sites and tests to use the builder setters.

[x] 73 SegmentRegistry handler-backed cache (Risk: MEDIUM)
    - Make `SegmentRegistryCache` store `SegmentHandler` per `SegmentId`
      (segment + lock state as one entry).
    - Keep `SegmentRegistry.getSegment` returning `SegmentRegistryResult`
      to signal registry state; map LOCKED to BUSY.
    - Add internal accessors for handler-only flows (split/evict) without
      exposing handler in the public registry API.
    - Update eviction logic to skip LOCKED handlers and keep cache/handler
      in sync.
    - Tests: locked entry not evicted, handler/segment consistency, BUSY
      returned when handler locked.

[x] 74 RegistryAccess: lock via `SegmentHandler` (Risk: MEDIUM)
    - Add internal accessor that returns the `SegmentHandler` for a
      `segmentId` + expected segment instance (BUSY/ERROR when mismatch).
    - Remove `lockSegmentHandler`/`unlockSegmentHandler` from
      `SegmentRegistryLocking` and `SegmentRegistryAccess`.
    - Update `SegmentRegistryAccessAdapter` to expose handler instead of
      lock/unlock methods.

[x] 75 Split flow: use handler lock directly (Risk: MEDIUM)
    - In `SegmentSplitCoordinator`, acquire handler via registry access and
      call `handler.lock()`/`handler.unlock()` directly.
    - Keep BUSY mapping when handler is locked.
    - Ensure eviction path still validates handler instance + state.

[x] 76 Tests + cleanup for handler locking (Risk: LOW)
    - Update tests that currently call registry lock/unlock to use handler
      locking instead.
    - Remove unused lock methods from `SegmentRegistryImpl`.
    - Verify eviction skips locked handlers and BUSY is returned when locked.

[x] 77 SegmentRegistry target-state rollout from `docs/architecture/registry.md` (Risk: HIGH)
    - Goal: make implementation fully match the documented registry model
      (state gate + per-key `Entry` state machine + single-flight load +
      bounded cache eviction + unload semantics).
    - Global rule: every step in 77.x must preserve behavioral parity with
      `docs/architecture/registry.md`. If behavior must change, update
      `registry.md` and diagrams first in the same PR before code changes.
    - Hard constraints:
      - no global lock in `get` hot path
      - unrelated keys must not block each other
      - per-key wait only on the same `Entry`
      - `LOADING` waits, `UNLOADING` maps to `BUSY`
      - load/open failures are exception-driven
    - Exit criteria:
      - behavior parity with `docs/architecture/registry.md` and
        `docs/architecture/images/registry-seq*.plantuml`
      - all new/updated tests green
      - no flakiness in repeated concurrency runs

[x] 77.1 Freeze target contract and remove ambiguity (Risk: HIGH)
    - Pin `docs/architecture/registry.md` + diagrams as source of truth.
    - Explicitly list non-negotiable runtime rules in code comments/Javadocs:
      - state gate mapping: `READY` normal, `FREEZE` -> `BUSY`,
        `CLOSED` -> `CLOSED`, `ERROR` -> `ERROR`
      - cache state mapping: `LOADING` wait, `UNLOADING` -> `BUSY`
      - failed unload leaves `UNLOADING` (documented behavior)
    - Acceptance:
      - no contradictory comments/Javadocs in `segmentregistry` package
      - docs and code contracts use same method names

[x] 77.2 Implement/align per-key `Entry` API contract (Risk: HIGH)
    - Ensure `SegmentRegistryCache.Entry` exposes and follows:
      - `tryStartLoad()`
      - `waitWhileLoading(currentAccessCx)`
      - `finishLoad(value)`
      - `fail(exception)`
      - `tryStartUnload()`
      - `finishUnload()`
      - `getEvictionOrder()`
    - Ensure lock/condition is strictly per-entry (no cross-key monitor).
    - Acceptance:
      - transitions only: `MISSING->LOADING->READY->UNLOADING->MISSING`
      - invalid transitions return fast/fail predictably

[x] 77.3 Align `get(key)` miss path to single-flight semantics (Risk: HIGH)
    - Use `putIfAbsent` race semantics correctly:
      - winner: `entryInMap == null` then load
      - loser: wait on the existing entry from map
    - Ensure wait target is the entry stored in map, not a local temporary.
    - Ensure load failure path calls `fail(exception)`, wakes waiters, and
      removes the expected entry from map.
    - Acceptance:
      - exactly one loader execution per key under high contention
      - all losers observe winner result or propagated exception

[x] 77.4 Align `get(key)` hit path semantics (Risk: HIGH)
    - READY: immediate return + recency update.
    - LOADING: block only on same entry until READY/failure.
    - UNLOADING: do not wait; return BUSY to caller.
    - Acceptance:
      - no waiting on keys in `UNLOADING`
      - no blocking between unrelated keys

[x] 77.5 Implement bounded eviction flow per docs (Risk: HIGH)
    - Keep capacity enforcement in cache layer.
    - Candidate selection:
      - LRU by `accessCx`
      - exclude requested key in `removeLastRecentUsedSegment(exceptSegmentId)`
      - only READY candidates can move to UNLOADING
    - Start close asynchronously, remove only after close success.
    - Acceptance:
      - eviction never unloads `exceptSegmentId`
      - failed `tryStartUnload` retries candidate selection without global stall

[x] 77.6 Lifecycle executor behavior and failure handling (Risk: HIGH)
    - Verify load/open and close/unload execution contexts follow design:
      - load for seq03 scenario in caller thread
      - close/unload on lifecycle executor thread
    - Define exact reaction to close failure:
      - keep entry `UNLOADING`
      - subsequent `get` returns BUSY
      - do not remove cache entry
    - Acceptance:
      - no caller-thread close IO
      - failed close path is deterministic and test-covered

[x] 77.7 Registry gate lifecycle alignment (Risk: MEDIUM)
    - Ensure startup: `FREEZE -> READY`.
    - Ensure close flow: `READY -> FREEZE -> CLOSED`.
    - Ensure idempotent close and terminal ERROR semantics.
    - Acceptance:
      - gate transitions are atomic and race-safe under concurrent calls
      - status mapping is consistent for all operations

[x] 77.8 API/status cleanup to match exception-driven load policy (Risk: MEDIUM)
    - Preserve `SegmentRegistryAccess` for status-oriented flows.
    - Keep load/open failure as propagated runtime exception from registry
      load paths (per docs).
    - Remove or deprecate status branches that conflict with this policy.
    - Acceptance:
      - no mixed behavior where same failure is sometimes status, sometimes throw

[x] 77.9 Unit tests for Entry/cache state machine (Risk: HIGH)
    - Extend `SegmentRegistryCacheTest` with deterministic tests:
      - single-flight: same key, many threads -> loader called once
      - wait-on-loading: loser threads block and then return same value
      - load failure wakeup: all waiters receive same failure
      - unloading maps to BUSY (no waiting)
      - eviction excludes `exceptSegmentId`
      - close failure leaves `UNLOADING`
    - Use `CountDownLatch`/`CyclicBarrier` to force races.
    - Add `@Timeout` to every concurrency-sensitive test.

[x] 77.10 Registry-level behavior tests (Risk: HIGH)
    - Update/add tests in:
      - `SegmentRegistryImplTest`
      - `SegmentRegistryStateMachineTest`
      - `SegmentRegistryAccessImplTest`
    - Verify:
      - gate mapping (`FREEZE/BUSY`, `CLOSED/CLOSED`, `ERROR/ERROR`)
      - startup transition (`FREEZE->READY`)
      - `getSegment` behavior across READY/LOADING/UNLOADING
      - exception propagation on load/open failure

[x] 77.11 High-concurrency integration verification (Risk: HIGH)
    - Extend/execute:
      - `IntegrationSegmentIndexConcurrencyTest`
      - `SegmentIndexImplConcurrencyTest`
      - `SegmentSplitCoordinatorConcurrencyTest`
    - Add focused registry stress tests (new class):
      - many threads on same key (single-flight proof)
      - many threads on different keys (independence proof)
      - eviction + concurrent gets + split coordinator interaction
    - Run repeated stress cycles to catch flakes.
    - Completed:
      - Added and executed
        `src/test/java/org/hestiastore/index/segmentindex/SegmentRegistryConcurrencyStressTest.java`.
      - Passed:
        `mvn -q -Dtest=IntegrationSegmentIndexConcurrencyTest,SegmentIndexImplConcurrencyTest,SegmentSplitCoordinatorConcurrencyTest,SegmentRegistryConcurrencyStressTest test`
      - Flake gate passed: 20/20 repeated runs with 0 failures.

[x] 77.12 Quality gates and release checklist (Risk: HIGH)
    - Mandatory local gates before merge:
      - targeted unit tests:
        `mvn -q -Dtest=SegmentRegistryCacheTest,SegmentRegistryImplTest,SegmentRegistryStateMachineTest test`
      - concurrency/integration tests:
        `mvn -q -Dtest=IntegrationSegmentIndexConcurrencyTest,SegmentIndexImplConcurrencyTest,SegmentSplitCoordinatorConcurrencyTest test`
      - full verification:
        `mvn verify`
    - Flake gate:
      - rerun concurrency suite N times (recommended N=20) and require 0 flakes.
    - Code quality gate:
      - no TODO/FIXME left in touched files
      - Javadocs reflect final behavior
      - diagrams and `registry.md` updated if behavior changed
    - Completed:
      - Passed targeted unit tests:
        `mvn -q -Dtest=SegmentRegistryCacheTest,SegmentRegistryImplTest,SegmentRegistryStateMachineTest test`
      - Passed concurrency/integration tests:
        `mvn -q -Dtest=IntegrationSegmentIndexConcurrencyTest,SegmentIndexImplConcurrencyTest,SegmentSplitCoordinatorConcurrencyTest,SegmentRegistryConcurrencyStressTest test`
      - Passed full verification:
        `mvn verify`
      - `TODO/FIXME` scan on touched files: none found.

[x] 77.13 Rollout and fallback plan (Risk: MEDIUM)
    - Deliver in small PRs matching 77.1-77.12 order.
    - After each PR:
      - run targeted regression suite
      - update `docs/architecture/registry.md` if contract changed
    - Keep a temporary feature flag only if needed for safe migration.
    - Remove fallback/compatibility code when final parity is reached.
    - Completed:
      - Work delivered incrementally following 77.1 -> 77.12 sequence.
      - Regression suites executed after key steps and before final merge gate.
      - No temporary feature flag required for this rollout.
