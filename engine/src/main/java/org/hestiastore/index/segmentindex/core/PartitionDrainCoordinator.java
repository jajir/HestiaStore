package org.hestiastore.index.segmentindex.core;

import java.util.Map;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

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
    private final LongSupplier nanoTimeSupplier;

    PartitionDrainCoordinator(final PartitionRuntime<K, V> partitionRuntime,
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final Executor drainExecutor, final IndexRetryPolicy retryPolicy,
            final StableSegmentCoordinator<K, V> stableSegmentCoordinator,
            final Stats stats, final Consumer<SegmentId> splitHintAction,
            final Consumer<RuntimeException> failureHandler) {
        this(partitionRuntime, keyToSegmentMap, drainExecutor, retryPolicy,
                stableSegmentCoordinator, stats, splitHintAction,
                failureHandler, System::nanoTime);
    }

    PartitionDrainCoordinator(final PartitionRuntime<K, V> partitionRuntime,
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final Executor drainExecutor, final IndexRetryPolicy retryPolicy,
            final StableSegmentCoordinator<K, V> stableSegmentCoordinator,
            final Stats stats, final Consumer<SegmentId> splitHintAction,
            final Consumer<RuntimeException> failureHandler,
            final LongSupplier nanoTimeSupplier) {
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
        this.nanoTimeSupplier = Vldtn.requireNonNull(nanoTimeSupplier,
                "nanoTimeSupplier");
    }

    void scheduleDrain(final SegmentId segmentId) {
        if (!partitionRuntime.markDrainScheduledIfNeeded(segmentId)) {
            return;
        }
        final long scheduledAtNanos = nanoTimeSupplier.getAsLong();
        try {
            drainExecutor.execute(
                    () -> drainScheduledPartition(segmentId, scheduledAtNanos));
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
        drainScheduledPartition(segmentId, nanoTimeSupplier.getAsLong());
    }

    private void drainScheduledPartition(final SegmentId segmentId,
            final long scheduledAtNanos) {
        final long startedAtNanos = nanoTimeSupplier.getAsLong();
        stats.recordDrainTaskStartDelayNanos(
                Math.max(0L, startedAtNanos - scheduledAtNanos));
        try {
            drainPartitionLoop(segmentId);
        } finally {
            stats.recordDrainTaskRunLatencyNanos(Math.max(0L,
                    nanoTimeSupplier.getAsLong() - startedAtNanos));
        }
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
