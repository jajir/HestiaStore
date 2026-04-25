package org.hestiastore.index.segmentindex.core.maintenance;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.split.SplitService;
import org.hestiastore.index.segmentindex.core.storage.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;

/**
 * Owns foreground flush and compaction orchestration across split and stable
 * segment maintenance collaborators.
 */
final class IndexMaintenanceCoordinator<K, V>
        implements SegmentIndexMaintenanceAccess<K, V> {

    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final SplitService<K, V> splitService;
    private final StableSegmentMaintenanceAccess<K, V> stableSegmentCoordinator;
    private final IndexWalCoordinator<K, V> walCoordinator;

    IndexMaintenanceCoordinator(
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SplitService<K, V> splitService,
            final StableSegmentMaintenanceAccess<K, V> stableSegmentCoordinator,
            final IndexWalCoordinator<K, V> walCoordinator) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.splitService = Vldtn.requireNonNull(splitService,
                "splitService");
        this.stableSegmentCoordinator = Vldtn.requireNonNull(
                stableSegmentCoordinator, "stableSegmentCoordinator");
        this.walCoordinator = Vldtn.requireNonNull(walCoordinator,
                "walCoordinator");
    }

    @Override
    public void compact() {
        compactMappedSegments();
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
        flushSegmentsWithoutWaiting();
        keyToSegmentMap.flushIfDirty();
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

    private void compactMappedSegments() {
        keyToSegmentMap.getSegmentIds().forEach(
                segmentId -> stableSegmentCoordinator.compactSegment(segmentId,
                        false));
    }

    private void flushSegmentsWithoutWaiting() {
        stableSegmentCoordinator.flushSegments(false);
    }

    private void awaitSettledSplitPolicyLoop() {
        splitService.awaitQuiescence();
    }

    private void finalizeSettledMaintenance(final Runnable rerunAction) {
        final long topologyVersion = keyToSegmentMap.snapshot().version();
        splitService.awaitQuiescence();
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
