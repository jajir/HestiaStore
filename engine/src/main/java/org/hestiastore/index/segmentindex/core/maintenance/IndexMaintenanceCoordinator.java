package org.hestiastore.index.segmentindex.core.maintenance;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.durability.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.core.split.BackgroundSplitCoordinator;

/**
 * Owns foreground flush and compaction orchestration across split and stable
 * segment maintenance collaborators.
 */
final class IndexMaintenanceCoordinator<K, V>
        implements SegmentIndexMaintenanceAccess<K, V> {

    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator;
    private final BackgroundSplitPolicyAccess<K, V> backgroundSplitPolicyLoop;
    private final StableSegmentMaintenanceAccess<K, V> stableSegmentCoordinator;
    private final IndexWalCoordinator<K, V> walCoordinator;

    IndexMaintenanceCoordinator(
            final KeyToSegmentMap<K> keyToSegmentMap,
            final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator,
            final BackgroundSplitPolicyAccess<K, V> backgroundSplitPolicyLoop,
            final StableSegmentMaintenanceAccess<K, V> stableSegmentCoordinator,
            final IndexWalCoordinator<K, V> walCoordinator) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.backgroundSplitCoordinator = Vldtn.requireNonNull(
                backgroundSplitCoordinator, "backgroundSplitCoordinator");
        this.backgroundSplitPolicyLoop = Vldtn.requireNonNull(
                backgroundSplitPolicyLoop, "backgroundSplitPolicyLoop");
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
        backgroundSplitCoordinator.awaitSplitsIdle(timeoutMillis);
    }

    private boolean hasSplitInFlight() {
        return backgroundSplitCoordinator.splitInFlightCount() > 0;
    }

    private void runWithSplitSchedulingPaused(final Runnable action) {
        backgroundSplitCoordinator.runWithSplitSchedulingPaused(action);
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
        backgroundSplitPolicyLoop.awaitExhausted();
    }

    private void scheduleSplitScanIfIdle() {
        backgroundSplitPolicyLoop.scheduleScanIfIdle();
    }

    private void finalizeSettledMaintenance(final Runnable rerunAction) {
        final long topologyVersion = keyToSegmentMap.snapshot().version();
        backgroundSplitPolicyLoop.scheduleScanIfIdle();
        backgroundSplitPolicyLoop.awaitExhausted();
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
