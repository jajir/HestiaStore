# Segment Refactor Inventory

## Core Segment Types
- `src/main/java/org/hestiastore/index/segment/Segment.java`: public API (currently marked not thread-safe).
- `src/main/java/org/hestiastore/index/segment/SegmentCore.java`: single-threaded core (extracted from previous SegmentImpl).
- `src/main/java/org/hestiastore/index/segment/SegmentImpl.java`: public wrapper delegating to `SegmentCore`.
- `src/main/java/org/hestiastore/index/segment/SegmentStateMachine.java`: state transition controller (not wired yet).
- `src/main/java/org/hestiastore/index/segment/SegmentReadPath.java`: read path (get/iterators/searcher lifecycle).
- `src/main/java/org/hestiastore/index/segment/SegmentWritePath.java`: write path (write cache, version bumps, flush snapshot).
- `src/main/java/org/hestiastore/index/segment/SegmentMaintenancePath.java`: maintenance IO helpers (flush writer, full write tx).
- `src/main/java/org/hestiastore/index/segment/SegmentImplSynchronizationAdapter.java`: thread-safe wrapper with locks + maintenance lock.
- `src/main/java/org/hestiastore/index/segmentasync/SegmentAsyncAdapter.java`: async maintenance wrapper over synchronization adapter.
- `src/main/java/org/hestiastore/index/segmentasync/SegmentAsync.java`: async segment interface.

## Builders / Factories
- `src/main/java/org/hestiastore/index/segment/SegmentBuilder.java`: builds `SegmentImpl`.
- `src/main/java/org/hestiastore/index/segmentindex/SegmentRegistry.java`: builds segment via builder and wraps with `SegmentAsyncAdapter`.

## Direct Dependencies on SegmentImpl
- `src/main/java/org/hestiastore/index/segment/SegmentCompacter.java`: accepts `SegmentImpl`.
- `src/main/java/org/hestiastore/index/segment/SegmentConsistencyChecker.java`: holds `SegmentImpl`.
- `src/main/java/org/hestiastore/index/segment/SegmentImplSynchronizationAdapter.java`: special-cases `SegmentImpl` for tryPut/flush.

## Index / Split Call Sites
- `src/main/java/org/hestiastore/index/segmentindex/SegmentIndexImpl.java`: checks for `SegmentImplSynchronizationAdapter`.
- `src/main/java/org/hestiastore/index/segmentindex/SegmentSplitCoordinator.java`: checks for `SegmentImplSynchronizationAdapter` and `SegmentAsyncAdapter`.
- `src/main/java/org/hestiastore/index/segmentindex/SegmentAsyncSplitCoordinator.java`: expects `SegmentAsyncAdapter`.
- `src/main/java/org/hestiastore/index/segmentindex/IndexConsistencyChecker.java`: checks for `SegmentImplSynchronizationAdapter`.

## Concurrency / Maintenance
- `src/main/java/org/hestiastore/index/segmentasync/*`: maintenance scheduler, policies, async executor.
