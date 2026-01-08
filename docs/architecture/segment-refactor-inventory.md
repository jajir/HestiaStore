# Segment Refactor Inventory

## Core Segment Types
- `src/main/java/org/hestiastore/index/segment/Segment.java`: public API (currently marked not thread-safe).
- `src/main/java/org/hestiastore/index/segment/SegmentCore.java`: single-threaded core (extracted from previous SegmentImpl).
- `src/main/java/org/hestiastore/index/segment/SegmentImpl.java`: public wrapper delegating to `SegmentCore` and gating via `SegmentStateMachine`.
- `src/main/java/org/hestiastore/index/segment/SegmentStateMachine.java`: atomic state transition controller (wired in `SegmentImpl`).
- `src/main/java/org/hestiastore/index/segment/SegmentReadPath.java`: read path (get/iterators/searcher lifecycle).
- `src/main/java/org/hestiastore/index/segment/SegmentWritePath.java`: write path (write cache, version bumps, flush snapshot).
- `src/main/java/org/hestiastore/index/segment/SegmentMaintenancePath.java`: maintenance IO helpers (flush writer, full write tx).
- `src/main/java/org/hestiastore/index/segment/SegmentImplSynchronizationAdapter.java`: thread-safe wrapper with locks + maintenance lock.

## Optional Capability Interfaces
- `src/main/java/org/hestiastore/index/segment/SegmentWriteLockSupport.java`: optional write/maintenance lock callbacks.

## Builders / Factories
- `src/main/java/org/hestiastore/index/segment/SegmentBuilder.java`: builds `SegmentImpl`.
- `src/main/java/org/hestiastore/index/segmentindex/SegmentRegistry.java`: builds `SegmentImpl` and wraps it with `SegmentImplSynchronizationAdapter` (registry stores `Segment` instances).

## Index / Split Call Sites
- `src/main/java/org/hestiastore/index/segmentindex/SegmentIndexImpl.java`: uses `Segment` interface and `SegmentResult` retries.
- `src/main/java/org/hestiastore/index/segmentindex/SegmentSplitCoordinator.java`: uses `SegmentWriteLockSupport` when available.
- `src/main/java/org/hestiastore/index/segmentindex/SegmentAsyncSplitCoordinator.java`: schedules splits on the maintenance executor.
- `src/main/java/org/hestiastore/index/segmentindex/IndexConsistencyChecker.java`: uses `Segment` interface.

## Concurrency / Maintenance
- `src/main/java/org/hestiastore/index/segmentindex/SegmentAsyncExecutor.java`: shared maintenance executor.
- `src/main/java/org/hestiastore/index/segmentindex/SegmentMaintenancePolicy*.java`: maintenance policies and decisions.
