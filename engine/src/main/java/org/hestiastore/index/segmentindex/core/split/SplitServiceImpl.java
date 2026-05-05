package org.hestiastore.index.segmentindex.core.split;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentregistry.BlockingSegment;

/**
 * Aggregates split execution and policy scheduling inside one managed runtime
 * boundary.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SplitServiceImpl<K, V>
        implements SplitService, SplitMetricsView {

    private final SplitExecutionCoordinator<K, V> splitCoordinator;
    private final SplitPolicyCoordinator<K, V> splitPolicyCoordinator;
    private final ManagedSplitRuntimeState managedState;

    SplitServiceImpl(
            final SplitExecutionCoordinator<K, V> splitCoordinator,
            final SplitPolicyCoordinator<K, V> splitPolicyCoordinator,
            final ManagedSplitRuntimeState managedState) {
        this.splitCoordinator = Vldtn.requireNonNull(splitCoordinator,
                "splitCoordinator");
        this.splitPolicyCoordinator = Vldtn.requireNonNull(
                splitPolicyCoordinator, "splitPolicyCoordinator");
        this.managedState = Vldtn.requireNonNull(managedState,
                "managedState");
    }

    boolean scheduleEligibleSplit(
            final BlockingSegment<K, V> segmentHandle,
            final long splitThreshold, final long observedKeyCount) {
        return splitCoordinator.scheduleEligibleSplit(segmentHandle,
                splitThreshold, observedKeyCount);
    }

    public boolean isSplitBlocked(final SegmentId segmentId) {
        return splitCoordinator.isSplitBlocked(segmentId);
    }

    int splitBlockedCount() {
        return splitCoordinator.splitBlockedCount();
    }

    @Override
    public void hintSplitCandidate(final SegmentId segmentId) {
        managedState.requireRunning("accept split candidate hints");
        splitPolicyCoordinator.hintSplitCandidate(segmentId);
    }

    @Override
    public SplitMetricsView splitMetricsView() {
        return this;
    }

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

    @Override
    public void requestFullSplitScan() {
        splitPolicyCoordinator.requestFullSplitScan();
    }

    @Override
    public void awaitQuiescence() {
        splitPolicyCoordinator.awaitQuiescence();
    }

    @Override
    public SplitMetricsSnapshot metricsSnapshot() {
        return new SplitMetricsSnapshot(splitCoordinator.splitInFlightCount(),
                splitCoordinator.splitBlockedCount());
    }

}
