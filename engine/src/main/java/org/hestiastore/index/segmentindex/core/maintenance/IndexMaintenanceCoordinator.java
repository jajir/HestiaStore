package org.hestiastore.index.segmentindex.core.maintenance;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.storage.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;

/**
 * Owns foreground flush and compaction orchestration across split and stable
 * segment maintenance collaborators.
 */
final class IndexMaintenanceCoordinator<K, V>
        implements SegmentIndexMaintenanceAccess<K, V> {

    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final SplitMaintenanceSynchronization<K, V> splitSynchronization;
    private final StableSegmentMaintenanceAccess<K, V> stableSegmentCoordinator;
    private final IndexWalCoordinator<K, V> walCoordinator;

    IndexMaintenanceCoordinator(
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SplitMaintenanceSynchronization<K, V> splitSynchronization,
            final StableSegmentMaintenanceAccess<K, V> stableSegmentCoordinator,
            final IndexWalCoordinator<K, V> walCoordinator) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.splitSynchronization = Vldtn.requireNonNull(splitSynchronization,
                "splitSynchronization");
        this.stableSegmentCoordinator = Vldtn.requireNonNull(
                stableSegmentCoordinator, "stableSegmentCoordinator");
        this.walCoordinator = Vldtn.requireNonNull(walCoordinator,
                "walCoordinator");
    }

    @Override
    public void compact() {
        if (hasSplitInFlight()) {
            return;
        }
        runWithSplitSchedulingPaused(this::compactMappedSegments);
        scheduleSplitScanIfIdle();
    }

    @Override
    public void compactAndWait() {
        awaitSettledSplitPolicyLoop();
        stableSegmentCoordinator.compactMappedSegmentsAndFlush();
        finalizeSettledMaintenance(
                stableSegmentCoordinator::compactMappedSegmentsAndFlush);
    }

    @Override
    public void flush() {
        runWithSplitSchedulingPaused(this::flushSegmentsWithoutWaiting);
        keyToSegmentMap.flushIfDirty();
        scheduleSplitScanIfIdle();
    }

    @Override
    public void flushAndWait() {
        awaitSettledSplitPolicyLoop();
        stableSegmentCoordinator.flushMappedSegmentsAndWait();
        finalizeSettledMaintenance(
                stableSegmentCoordinator::flushMappedSegmentsAndWait);
    }

    @Override
    public void invalidateSegmentIterators() {
        stableSegmentCoordinator.invalidateIterators();
    }

    @Override
    public void awaitSplitsIdle(final long timeoutMillis) {
        splitSynchronization.awaitIdle(timeoutMillis);
    }

    private boolean hasSplitInFlight() {
        return splitSynchronization.splitInFlightCount() > 0;
    }

    private void runWithSplitSchedulingPaused(final Runnable action) {
        splitSynchronization.runWithSplitSchedulingPaused(action);
    }

    private void compactMappedSegments() {
        keyToSegmentMap.getSegmentIds().forEach(
                segmentId -> stableSegmentCoordinator.compactSegment(segmentId,
                        false));
    }

    private void flushSegmentsWithoutWaiting() {
        stableSegmentCoordinator.flushSegments(false);
    }

    private void awaitSettledSplitPolicyLoop() {
        splitSynchronization.awaitExhausted();
    }

    private void scheduleSplitScanIfIdle() {
        splitSynchronization.scheduleScanIfIdle();
    }

    private void finalizeSettledMaintenance(final Runnable rerunAction) {
        final long topologyVersion = keyToSegmentMap.snapshot().version();
        splitSynchronization.scheduleScanIfIdle();
        splitSynchronization.awaitExhausted();
        rerunIfTopologyChanged(topologyVersion, rerunAction);
        keyToSegmentMap.flushIfDirty();
        walCoordinator.checkpoint();
    }

    private void rerunIfTopologyChanged(final long topologyVersion,
            final Runnable rerunAction) {
        if (keyToSegmentMap.isAtVersion(topologyVersion)) {
            return;
        }
        rerunAction.run();
    }
}
