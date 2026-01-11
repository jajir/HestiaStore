# Segment Index Concurrency & Lifecycle

## Glossary
- Segment index: top-level API that routes operations to segments.
- Key-segment mapping: map of max key -> SegmentId (KeySegmentCache).
- Mapping version: monotonically increasing counter for optimistic mapping
  checks.
- Segment registry: cache of Segment instances plus the maintenance executor.
- Maintenance coordinator: decides compact/split after writes.
- Split: replace one segment with a new segment (or two) and update the
  mapping.

## Core Rules
- SegmentIndex is thread-safe by contract; calls may be concurrent.
- Target: highly concurrent SegmentIndex API; avoid global synchronization and
  only protect minimal shared structures (mapping updates, split swaps).
- Index operations are not globally serialized; concurrency is bounded by
  shared caches, mapping updates, and per-segment state machines.
- Segment maintenance IO runs on the segment maintenance executor.
- The maintenance executor is always created by SegmentRegistry from
  IndexConfiguration.numberOfSegmentIndexMaintenanceThreads (default 10).
- Automatic post-write flush/compact is optional and enabled by default.
- Segment BUSY is treated as transient and retried internally; callers do not
  see BUSY.
- Mapping changes are applied atomically and validated by version checks.

## Thread Safety Mechanisms
- IndexInternalConcurrent executes sync operations on caller threads without
  a global executor.
- SegmentRegistrySynchronized serializes access to the segment instance map and
  registry mutations.
- KeySegmentCache uses snapshot reads plus a mapping version; updates take a
  write lock and increment the version.
- Segment implementations are thread-safe; read/write operations proceed in
  parallel when the segment state allows it.

## API Behavior
- put/get/delete: retry on per-segment BUSY using IndexRetryPolicy
  (indexBusyBackoffMillis + indexBusyTimeoutMillis); mapping version mismatch
  triggers a retry with a fresh snapshot. Timeouts throw IndexException.
- putAsync/getAsync/deleteAsync: run the synchronous operation on a background
  thread via IndexAsyncAdapter and return a CompletionStage.
- flush/compact: start maintenance on each segment and return once accepted;
  do not wait for IO completion; BUSY retries follow IndexRetryPolicy.
- flushAndWait/compactAndWait: wait for every segment maintenance stage; do not
  call from a segment maintenance executor thread.
- getStream: captures a snapshot of segment ids and iterates them using the
  default segment iterator isolation (FAIL_FAST).

## Maintenance & Splits
- SegmentMaintenanceCoordinator evaluates thresholds after each write and
  triggers flush/compact only when segmentMaintenanceAutoEnabled is true.
- Splits are scheduled by SegmentAsyncSplitCoordinator on the shared
  maintenance executor; only one split per segment id can be in flight.
- SegmentSplitCoordinator retries BUSY using IndexRetryPolicy; timeouts throw.
- SegmentSplitCoordinator opens a FULL_ISOLATION iterator and keeps it open
  until file swap + mapping update completes to prevent partial splits from
  leaking to writers.
- After a split, KeySegmentCache updates the mapping and flushes it to disk;
  any in-flight write with a stale mapping version retries.

## Index State Machine
States:
- OPENING: index bootstrap/consistency checks (and lock acquisition) in
  progress; operations are rejected.
- READY: operations allowed.
- ERROR: unrecoverable failure; operations are rejected.
- CLOSED: operations are rejected.

Transitions:
- OPENING -> READY: after initialization and consistency checks complete.
- READY -> CLOSED: close() called; file lock released.
- any -> ERROR: unrecoverable failure (e.g., OOM, disk full, failed split/file
  swap, or consistency check failure).

Only one index instance may hold the directory lock at a time.

## Failure Handling
- SegmentResultStatus.ERROR from any segment results in IndexException.
- Maintenance failures complete per-segment CompletionStages exceptionally;
  flushAndWait/compactAndWait propagate as IndexException.
- Split failures surface through the split future and are rethrown when joined.
- When entering ERROR, the index stops accepting operations and requires manual
  intervention (recovery/repair or restore from backups).

## Components
- SegmentIndex (public API): thread-safe entry point.
- SegmentIndexImpl: retries BUSY, routes operations to segments, and manages
  maintenance.
- IndexAsyncAdapter: provides async operations by running sync calls on a
  background thread and waiting for in-flight async calls on close.
- SegmentIndexCore: single-attempt mapping + segment selection.
- IndexRetryPolicy: backoff + timeout for BUSY retries.
- IndexResult/IndexResultStatus: internal OK/BUSY/CLOSED/ERROR wrapper.
- KeySegmentCache: mapping, snapshot versioning, and persistence of segment ids.
- SegmentRegistry(Synchronized): caches Segment instances and supplies the
  maintenance executor.
- SegmentMaintenanceCoordinator: post-write flush/compact/split decisions.
- SegmentSplitCoordinator / SegmentAsyncSplitCoordinator: split execution and
  scheduling.

## Implementation Mapping
- Index implementation: IndexInternalConcurrent (caller-thread execution).
- Mapping version: KeySegmentCache.version (AtomicLong).
- Maintenance executor: SegmentRegistry.getMaintenanceExecutor() backed by
  IndexConfiguration.numberOfSegmentIndexMaintenanceThreads (default 10).
- Split isolation: SegmentIteratorIsolation.FULL_ISOLATION.
- Retry policy: IndexConfiguration.indexBusyBackoffMillis and
  IndexConfiguration.indexBusyTimeoutMillis.
