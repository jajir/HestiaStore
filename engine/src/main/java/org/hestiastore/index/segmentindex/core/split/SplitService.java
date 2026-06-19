package org.hestiastore.index.segmentindex.core.split;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;

/**
 * Aggregates split execution and policy scheduling inside one managed runtime
 * boundary.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SplitService<K, V> implements AutoCloseable {

    private final SplitExecutionCoordinator<K, V> splitCoordinator;
    private final SplitPolicyCoordinator<K, V> splitPolicyCoordinator;
    private final ManagedSplitRuntimeState managedState;
    private final SplitStatsRecorder statsRecorder;

    SplitService(
            final SplitExecutionCoordinator<K, V> splitCoordinator,
            final SplitPolicyCoordinator<K, V> splitPolicyCoordinator,
            final ManagedSplitRuntimeState managedState,
            final SplitStatsRecorder statsRecorder) {
        this.splitCoordinator = Vldtn.requireNonNull(splitCoordinator,
                "splitCoordinator");
        this.splitPolicyCoordinator = Vldtn.requireNonNull(
                splitPolicyCoordinator, "splitPolicyCoordinator");
        this.managedState = Vldtn.requireNonNull(managedState,
                "managedState");
        this.statsRecorder = Vldtn.requireNonNull(statsRecorder,
                "statsRecorder");
    }

    /**
     * Creates a builder for the default split runtime service.
     *
     * @param <M> key type
     * @param <N> value type
     * @return split service builder
     */
    public static <M, N> SplitServiceBuilder<M, N> builder() {
        return new SplitServiceBuilder<>();
    }

    boolean scheduleEligibleSplit(
            final SegmentId segmentId,
            final long splitThreshold, final long observedKeyCount) {
        return splitCoordinator.scheduleEligibleSplit(segmentId,
                splitThreshold, observedKeyCount);
    }

    public boolean isSplitBlocked(final SegmentId segmentId) {
        return splitCoordinator.isSplitBlocked(segmentId);
    }

    int splitBlockedCount() {
        return splitCoordinator.splitBlockedCount();
    }

    /**
     * Hints that the provided mapped segment may now be eligible for split.
     *
     * @param segmentId mapped segment id
     */
    public void hintSplitCandidate(final SegmentId segmentId) {
        managedState.requireRunning("accept split candidate hints");
        splitPolicyCoordinator.hintSplitCandidate(segmentId);
    }

    /**
     * Closes the managed split runtime. Shared executors are owned by the
     * surrounding runtime and are not shut down by this call.
     */
    @Override
    public void close() {
        if (!managedState.beginClose()) {
            return;
        }
        try {
            splitPolicyCoordinator.awaitQuiescence();
        } finally {
            managedState.markClosed();
        }
    }

    /**
     * Requests a full split-policy scan regardless of current in-flight split
     * state.
     */
    public void requestFullSplitScan() {
        splitPolicyCoordinator.requestFullSplitScan();
    }

    /**
     * Waits until split-policy work and in-flight splits are quiescent.
     */
    public void awaitQuiescence() {
        splitPolicyCoordinator.awaitQuiescence();
    }

    public SplitStats statsSnapshot() {
        return statsRecorder.statsSnapshot(splitCoordinator.splitInFlightCount(),
                splitCoordinator.splitBlockedCount());
    }

}
