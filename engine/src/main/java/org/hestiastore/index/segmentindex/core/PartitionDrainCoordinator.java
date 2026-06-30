package org.hestiastore.index.segmentindex.core;

import java.util.Map;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.partition.PartitionImmutableRun;
import org.hestiastore.index.segmentindex.partition.PartitionRuntime;

/**
 * Owns partition drain scheduling and immutable-run publishing into stable
 * segments.
 */
@SuppressWarnings("java:S107")
final class PartitionDrainCoordinator<K, V> {

    private static final String OPERATION_DRAIN = "drain";

    private final PartitionRuntime<K, V> partitionRuntime;
    private final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap;
    private final Executor drainExecutor;
    private final IndexRetryPolicy retryPolicy;
    private final StableSegmentCoordinator<K, V> stableSegmentCoordinator;
    private final Stats stats;
    private final Consumer<SegmentId> splitHintAction;
    private final Consumer<RuntimeException> failureHandler;

    PartitionDrainCoordinator(final PartitionRuntime<K, V> partitionRuntime,
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final Executor drainExecutor, final IndexRetryPolicy retryPolicy,
            final StableSegmentCoordinator<K, V> stableSegmentCoordinator,
            final Stats stats, final Consumer<SegmentId> splitHintAction,
            final Consumer<RuntimeException> failureHandler) {
        this.partitionRuntime = Vldtn.requireNonNull(partitionRuntime,
                "partitionRuntime");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.drainExecutor = Vldtn.requireNonNull(drainExecutor,
                "drainExecutor");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
        this.stableSegmentCoordinator = Vldtn.requireNonNull(
                stableSegmentCoordinator, "stableSegmentCoordinator");
        this.stats = Vldtn.requireNonNull(stats, "stats");
        this.splitHintAction = Vldtn.requireNonNull(splitHintAction,
                "splitHintAction");
        this.failureHandler = Vldtn.requireNonNull(failureHandler,
                "failureHandler");
    }

    void scheduleDrain(final SegmentId segmentId) {
        if (!partitionRuntime.markDrainScheduledIfNeeded(segmentId)) {
            return;
        }
        try {
            drainExecutor.execute(() -> drainPartitionLoop(segmentId));
        } catch (final RuntimeException e) {
            partitionRuntime.finishDrainScheduling(segmentId);
            throw e;
        }
    }

    void drainPartitions(final boolean waitForCompletion) {
        partitionRuntime.sealAllActivePartitionsForDrain();
        final List<SegmentId> segmentIds = keyToSegmentMap.getSegmentIds();
        for (final SegmentId segmentId : segmentIds) {
            if (waitForCompletion) {
                drainPartitionNow(segmentId);
            } else {
                scheduleDrain(segmentId);
            }
        }
        if (!waitForCompletion) {
            return;
        }
        final long startNanos = retryPolicy.startNanos();
        while (partitionRuntime.snapshot().getDrainInFlightCount() > 0) {
            retryPolicy.backoffOrThrow(startNanos, OPERATION_DRAIN, null);
        }
    }

    private void drainPartitionNow(final SegmentId segmentId) {
        if (!partitionRuntime.markDrainScheduledIfNeeded(segmentId)) {
            return;
        }
        drainPartitionLoop(segmentId);
    }

    private void drainPartitionLoop(final SegmentId segmentId) {
        boolean drainedAnyRun = false;
        try {
            while (true) {
                final PartitionImmutableRun<K, V> run = partitionRuntime
                        .peekOldestImmutableRun(segmentId);
                if (run == null) {
                    return;
                }
                final long drainRunStartNanos = System.nanoTime();
                drainImmutableRun(segmentId, run);
                partitionRuntime.completeDrainedRun(segmentId, run);
                stats.recordDrainLatencyNanos(
                        System.nanoTime() - drainRunStartNanos);
                drainedAnyRun = true;
            }
        } catch (final RuntimeException e) {
            failureHandler.accept(e);
            throw e;
        } finally {
            partitionRuntime.finishDrainScheduling(segmentId);
            if (drainedAnyRun) {
                splitHintAction.accept(segmentId);
            }
        }
    }

    private void drainImmutableRun(final SegmentId segmentId,
            final PartitionImmutableRun<K, V> run) {
        for (final Map.Entry<K, V> entry : run.getEntries().entrySet()) {
            stableSegmentCoordinator.putEntryForDrain(segmentId,
                    entry.getKey(), entry.getValue());
        }
        stableSegmentCoordinator.flushSegment(segmentId, true);
    }
}
