# Refactor backlog

## Active (segmentindex refactor plan - class level)
[ ] - SegmentIndex.java: change openIndex() to instantiate IndexInternalConcurrent by default; keep IndexInternalSynchronized as opt-in adapter (add static openSynchronized(...) or document direct construction).
[ ] - IndexInternalConcurrent (new class): extends SegmentIndexImpl; overrides sync methods (put/get/delete/flush/compact/compactAndWait/flushAndWait/getStream) to call super directly; keep async methods inherited from SegmentIndexImpl.
[ ] - IndexInternalSynchronized: mark as serialized/legacy in Javadoc; keep numberOfThreads usage only for this adapter; update tests to target the new default implementation.
[ ] - SegmentIndexState enum (new): OPENING/READY/ERROR/CLOSED; add SegmentIndex.getState() method.
[ ] - IndexStateNew -> IndexStateOpening: rename class and update constructor usages; update error messages to mention OPENING.
[ ] - IndexStateClose -> IndexStateClosed: rename class and update usages.
[ ] - IndexStateError (new): reject all operations; store failure reason if needed.
[ ] - SegmentIndexImpl: add field SegmentIndexState state; set OPENING at start of constructor, READY after keySegmentCache + segmentRegistry init, CLOSED in doClose(); add failWithError(Throwable) to set ERROR and block operations.
[ ] - IndexConfigurationContract: add DEFAULT_SEGMENT_INDEX_MAINTENANCE_THREADS = 10; add DEFAULT_INDEX_BUSY_BACKOFF_MILLIS and DEFAULT_INDEX_BUSY_TIMEOUT_MILLIS.
[ ] - IndexConfiguration: add fields + getters for segmentIndexMaintenanceThreads, indexBusyBackoffMillis, indexBusyTimeoutMillis.
[ ] - IndexConfigurationBuilder: add fields + methods withNumberOfSegmentIndexMaintenanceThreads(Integer), withIndexBusyBackoffMillis(Integer), withIndexBusyTimeoutMillis(Integer); include in build().
[ ] - IndexConfiguratonStorage: add property keys segmentIndexMaintenanceThreads, indexBusyBackoffMillis, indexBusyTimeoutMillis; read/write in load() and save().
[ ] - IndexConfigurationManager: applyDefaults for new fields, mergeWithStored overrides, validate >0 for threads and timeouts.
[ ] - SegmentAsyncExecutor: replace Executors.newFixedThreadPool with ThreadPoolExecutor + bounded ArrayBlockingQueue; add constants MIN_QUEUE_CAPACITY and QUEUE_CAPACITY_MULTIPLIER; use CallerRunsPolicy for backpressure.
[ ] - SegmentRegistry: create SegmentAsyncExecutor with segmentIndexMaintenanceThreads (or use maintenanceExecutor override); stop using numberOfThreads for maintenance.
[ ] - SegmentMaintenanceCoordinator: make auto flush/compact optional via new config flag (default false); remove implicit maintenance when disabled.
[ ] - SegmentAsyncSplitCoordinator: ensure split scheduling uses the shared maintenance executor only; no per-segment executors.
[ ] - SegmentIndexCore (new): move mapping resolution + registry access + segment selection here; return IndexResult/IndexResultStatus (OK/BUSY/CLOSED/ERROR).
[ ] - IndexResult/IndexResultStatus (new): value + status wrapper for core operations; no public BUSY exposure.
[ ] - SegmentIndexImpl: use SegmentIndexCore + IndexRetryPolicy for get/put/delete/openIterator/flush/compact loops; retry BUSY with backoff; throw IndexException on timeout.
[ ] - IndexRetryPolicy (new): busyBackoffMillis + busyTimeoutMillis; used by SegmentIndexImpl and SegmentSplitCoordinator busy loops.
[ ] - SegmentSplitCoordinator: apply IndexRetryPolicy in compactSegment() and hasLiveEntries() loops.
[ ] - Tests: IndexInternalConcurrentTest (no executor hop), SegmentIndexStateTest (OPENING/READY/ERROR/CLOSED), SegmentAsyncExecutorTest (bounded queue), SegmentRegistryTest (shared executor), SegmentIndexImplConcurrencyTest (parallel get/put without serialization), SegmentIndexRetryPolicyTest (timeout).
[ ] - Docs: update docs/architecture/segment-index-concurrency.md to reflect new state, executor config, and retry policy.

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
