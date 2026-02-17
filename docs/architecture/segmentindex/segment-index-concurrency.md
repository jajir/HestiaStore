# Segment Index Concurrency & Lifecycle

## Glossary
- Segment index: top-level API that routes operations to segments.
- Key-segment mapping: map of max key -> SegmentId (KeyToSegmentMap).
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
- KeyToSegmentMap uses snapshot reads plus a mapping version; updates take a
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
- flushAndWait/compactAndWait: wait for each segment to return to `READY`
  (or `CLOSED`); do not call from a segment maintenance executor thread.
- getStream: captures a snapshot of segment ids and iterates them using the
  default segment iterator isolation (FAIL_FAST). An overload allows
  FULL_ISOLATION for per-segment exclusivity; the stream must be closed to
  release the segment lock.
- Segment close (async): once close starts, the segment drains in-flight work
  and rejects/blocks new operations until CLOSED. The registry should not
  reopen a closing segment; attempts should retry until the close completes.
  The per-segment `.lock` file enforces single-open at the directory level.

## Maintenance & Splits
- SegmentMaintenanceCoordinator evaluates thresholds after each write and
  triggers flush/compact only when segmentMaintenanceAutoEnabled is true.
- Splits are scheduled by SegmentAsyncSplitCoordinator on the shared
  maintenance executor; only one split per segment id can be in flight.
- SegmentSplitCoordinator retries BUSY using IndexRetryPolicy; timeouts throw.
- SegmentSplitCoordinator opens a FULL_ISOLATION iterator and keeps it open
  until file swap + mapping update completes to prevent partial splits from
  leaking to writers.
- After a split, KeyToSegmentMap updates the mapping and flushes it to disk;
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
- READY -> CLOSED: close() completes; file lock released.
- any -> ERROR: unrecoverable failure (e.g., OOM, disk full, failed split/file
  swap, or consistency check failure).

Only one index instance may hold the directory lock at a time.

## Failure Handling
- SegmentResultStatus.ERROR from any segment results in IndexException.
- Maintenance failures move the segment to ERROR; flushAndWait/compactAndWait
  propagate as IndexException.
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
- KeyToSegmentMap: mapping, snapshot versioning, and persistence of segment ids.
- SegmentRegistry(Synchronized): caches Segment instances and supplies the
  maintenance executor.
- SegmentMaintenanceCoordinator: post-write flush/compact/split decisions.
- SegmentSplitCoordinator / SegmentAsyncSplitCoordinator: split execution and
  scheduling.

## Iterator Isolation
- FAIL_FAST: iteration is optimistic; any mutation can invalidate the
  iterator and terminate the stream early.
- FULL_ISOLATION: holds exclusive access per segment while its iterator is
  open; writers, flush/compact, and split on that segment block until the
  iterator (or stream) is closed.

## Implementation Mapping
- Index implementation: IndexInternalConcurrent (caller-thread execution).
- Mapping version: KeyToSegmentMap.version (AtomicLong).
- Maintenance executor: SegmentRegistry.getMaintenanceExecutor() backed by
  IndexConfiguration.numberOfSegmentIndexMaintenanceThreads (default 10).
- Split isolation: SegmentIteratorIsolation.FULL_ISOLATION.
- Retry policy: IndexConfiguration.indexBusyBackoffMillis and
  IndexConfiguration.indexBusyTimeoutMillis.
