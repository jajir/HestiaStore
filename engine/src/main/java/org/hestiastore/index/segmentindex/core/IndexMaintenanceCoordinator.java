package org.hestiastore.index.segmentindex.core;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.split.BackgroundSplitCoordinator;

/**
 * Owns foreground flush and compaction orchestration across split and stable
 * segment maintenance collaborators.
 */
final class IndexMaintenanceCoordinator<K, V> {

    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator;
    private final BackgroundSplitPolicyLoop<K, V> backgroundSplitPolicyLoop;
    private final StableSegmentCoordinator<K, V> stableSegmentCoordinator;
    private final IndexWalCoordinator<K, V> walCoordinator;

    IndexMaintenanceCoordinator(
            final KeyToSegmentMap<K> keyToSegmentMap,
            final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator,
            final BackgroundSplitPolicyLoop<K, V> backgroundSplitPolicyLoop,
            final StableSegmentCoordinator<K, V> stableSegmentCoordinator,
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

    void compact() {
        if (backgroundSplitCoordinator.splitInFlightCount() > 0) {
            return;
        }
        backgroundSplitCoordinator.runWithSplitSchedulingPaused(() -> keyToSegmentMap
                .getSegmentIds()
                .forEach(segmentId -> stableSegmentCoordinator.compactSegment(
                        segmentId, false)));
        backgroundSplitPolicyLoop.scheduleScanIfIdle();
    }

    void compactAndWait() {
        backgroundSplitPolicyLoop.awaitExhausted();
        stableSegmentCoordinator.compactMappedSegmentsAndFlush();
        final long finalTopologyVersion = keyToSegmentMap.snapshot().version();
        backgroundSplitPolicyLoop.scheduleScanIfIdle();
        backgroundSplitPolicyLoop.awaitExhausted();
        if (!keyToSegmentMap.isAtVersion(finalTopologyVersion)) {
            stableSegmentCoordinator.compactMappedSegmentsAndFlush();
        }
        keyToSegmentMap.flushIfDirty();
        walCoordinator.checkpoint();
    }

    void flush() {
        backgroundSplitCoordinator.runWithSplitSchedulingPaused(
                () -> stableSegmentCoordinator.flushSegments(false));
        keyToSegmentMap.flushIfDirty();
        backgroundSplitPolicyLoop.scheduleScanIfIdle();
    }

    void flushAndWait() {
        backgroundSplitPolicyLoop.awaitExhausted();
        stableSegmentCoordinator.flushMappedSegmentsAndWait();
        final long finalTopologyVersion = keyToSegmentMap.snapshot().version();
        backgroundSplitPolicyLoop.scheduleScanIfIdle();
        backgroundSplitPolicyLoop.awaitExhausted();
        if (!keyToSegmentMap.isAtVersion(finalTopologyVersion)) {
            stableSegmentCoordinator.flushMappedSegmentsAndWait();
        }
        keyToSegmentMap.flushIfDirty();
        walCoordinator.checkpoint();
    }
}
