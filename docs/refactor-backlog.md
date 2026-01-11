# Refactor backlog

## Active (segmentindex refactor plan - class level)
[x] - SegmentIndex.java: openIndex() uses IndexInternalConcurrent by default; remove IndexInternalSynchronized as an opt-in adapter.
[x] - IndexInternalConcurrent (new class): extends SegmentIndexImpl; overrides sync methods (put/get/delete/flush/compact/compactAndWait/flushAndWait/getStream) to call super directly; keep async methods inherited from SegmentIndexImpl.
[x] - IndexInternalSynchronized removed; tests/docs updated to reflect concurrent-only behavior.
[x] - SegmentIndexState enum (new): OPENING/READY/ERROR/CLOSED; add SegmentIndex.getState() method.
[x] - IndexStateNew -> IndexStateOpening: rename class and update constructor usages; update error messages to mention OPENING.
[x] - IndexStateClose -> IndexStateClosed: rename class and update usages.
[x] - IndexStateError (new): reject all operations; store failure reason if needed.
[x] - SegmentIndexImpl: add field SegmentIndexState state; set OPENING at start of constructor, READY after keySegmentCache + segmentRegistry init, CLOSED in doClose(); add failWithError(Throwable) to set ERROR and block operations.
[x] - IndexConfigurationContract: add DEFAULT_SEGMENT_INDEX_MAINTENANCE_THREADS = 10; add DEFAULT_INDEX_BUSY_BACKOFF_MILLIS and DEFAULT_INDEX_BUSY_TIMEOUT_MILLIS.
[x] - IndexConfiguration: add fields + getters for segmentIndexMaintenanceThreads, indexBusyBackoffMillis, indexBusyTimeoutMillis.
[x] - IndexConfigurationBuilder: add fields + methods withNumberOfSegmentIndexMaintenanceThreads(Integer), withIndexBusyBackoffMillis(Integer), withIndexBusyTimeoutMillis(Integer); include in build().
[x] - IndexConfiguratonStorage: add property keys segmentIndexMaintenanceThreads, indexBusyBackoffMillis, indexBusyTimeoutMillis; read/write in load() and save().
[x] - IndexConfigurationManager: applyDefaults for new fields, mergeWithStored overrides, validate >0 for threads and timeouts.
[x] - SegmentAsyncExecutor: replace Executors.newFixedThreadPool with ThreadPoolExecutor + bounded ArrayBlockingQueue; add constants MIN_QUEUE_CAPACITY and QUEUE_CAPACITY_MULTIPLIER; use CallerRunsPolicy for backpressure.
[x] - SegmentRegistry: create SegmentAsyncExecutor with segmentIndexMaintenanceThreads; stop using numberOfThreads for maintenance.
[x] - SegmentMaintenanceCoordinator: make auto flush/compact optional via new config flag (default false); remove implicit maintenance when disabled.
[x] - SegmentAsyncSplitCoordinator: ensure split scheduling uses the shared maintenance executor only; no per-segment executors.
[x] - SegmentIndexCore (new): move mapping resolution + registry access + segment selection here; return IndexResult/IndexResultStatus (OK/BUSY/CLOSED/ERROR).
[x] - IndexResult/IndexResultStatus (new): value + status wrapper for core operations; no public BUSY exposure.
[x] - SegmentIndexImpl: use SegmentIndexCore + IndexRetryPolicy for get/put/delete/openIterator/flush/compact loops; retry BUSY with backoff; throw IndexException on timeout.
[x] - IndexRetryPolicy (new): busyBackoffMillis + busyTimeoutMillis; used by SegmentIndexImpl and SegmentSplitCoordinator busy loops.
[x] - SegmentSplitCoordinator: apply IndexRetryPolicy in compactSegment() and hasLiveEntries() loops.
[x] - Tests: IndexInternalConcurrentTest (no executor hop), SegmentIndexStateTest (OPENING/READY/ERROR/CLOSED), SegmentAsyncExecutorTest (bounded queue), SegmentRegistryTest (shared executor), SegmentIndexImplConcurrencyTest (parallel get/put without serialization), IndexRetryPolicyTest (timeout).
[x] - Docs: update docs/architecture/segment-index-concurrency.md to reflect new state, executor config, and retry policy.
[x] - IndexContextLoggingAdapter should be responsible for one think. Please move async operations to separate adapter class

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
