package org.hestiastore.index.segmentindex.core;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.partition.PartitionRuntime;
import org.hestiastore.index.segmentindex.partition.PartitionRuntimeSnapshot;

/**
 * Owns foreground flush and compaction orchestration across drain, split, and
 * stable-segment maintenance collaborators.
 */
final class IndexMaintenanceCoordinator<K, V> {

    private final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap;
    private final PartitionRuntime<K, V> partitionRuntime;
    private final PartitionDrainCoordinator<K, V> partitionDrainCoordinator;
    private final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator;
    private final BackgroundSplitPolicyLoop<K, V> backgroundSplitPolicyLoop;
    private final StableSegmentCoordinator<K, V> stableSegmentCoordinator;
    private final IndexWalCoordinator<K, V> walCoordinator;

    IndexMaintenanceCoordinator(
            final KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap,
            final PartitionRuntime<K, V> partitionRuntime,
            final PartitionDrainCoordinator<K, V> partitionDrainCoordinator,
            final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator,
            final BackgroundSplitPolicyLoop<K, V> backgroundSplitPolicyLoop,
            final StableSegmentCoordinator<K, V> stableSegmentCoordinator,
            final IndexWalCoordinator<K, V> walCoordinator) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.partitionRuntime = Vldtn.requireNonNull(partitionRuntime,
                "partitionRuntime");
        this.partitionDrainCoordinator = Vldtn.requireNonNull(
                partitionDrainCoordinator, "partitionDrainCoordinator");
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
        partitionDrainCoordinator.drainPartitions(false);
        final PartitionRuntimeSnapshot partitionSnapshot = partitionRuntime
                .snapshot();
        if (partitionSnapshot.getDrainInFlightCount() > 0
                || partitionSnapshot.getActivePartitionCount() > 0
                || partitionSnapshot.getImmutableRunCount() > 0
                || partitionSnapshot.getBufferedKeyCount() > 0) {
            return;
        }
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
        partitionDrainCoordinator.drainPartitions(true);
        backgroundSplitPolicyLoop.awaitExhausted();
        partitionDrainCoordinator.drainPartitions(true);
        backgroundSplitPolicyLoop.awaitExhausted();
        stableSegmentCoordinator.compactMappedSegmentsAndFlush();
        final long finalTopologyVersion = keyToSegmentMap.snapshot().version();
        backgroundSplitPolicyLoop.scheduleScanIfIdle();
        backgroundSplitPolicyLoop.awaitExhausted();
        if (!keyToSegmentMap.isVersion(finalTopologyVersion)) {
            partitionDrainCoordinator.drainPartitions(true);
            backgroundSplitPolicyLoop.awaitExhausted();
            stableSegmentCoordinator.compactMappedSegmentsAndFlush();
        }
        keyToSegmentMap.optionalyFlush();
        walCoordinator.checkpoint();
    }

    void flush() {
        partitionDrainCoordinator.drainPartitions(false);
        backgroundSplitCoordinator.runWithSplitSchedulingPaused(
                () -> stableSegmentCoordinator.flushSegments(false));
        keyToSegmentMap.optionalyFlush();
        backgroundSplitPolicyLoop.scheduleScanIfIdle();
    }

    void flushAndWait() {
        partitionDrainCoordinator.drainPartitions(true);
        backgroundSplitPolicyLoop.awaitExhausted();
        partitionDrainCoordinator.drainPartitions(true);
        backgroundSplitPolicyLoop.awaitExhausted();
        stableSegmentCoordinator.flushMappedSegmentsAndWait();
        final long finalTopologyVersion = keyToSegmentMap.snapshot().version();
        backgroundSplitPolicyLoop.scheduleScanIfIdle();
        backgroundSplitPolicyLoop.awaitExhausted();
        if (!keyToSegmentMap.isVersion(finalTopologyVersion)) {
            partitionDrainCoordinator.drainPartitions(true);
            backgroundSplitPolicyLoop.awaitExhausted();
            stableSegmentCoordinator.flushMappedSegmentsAndWait();
        }
        keyToSegmentMap.optionalyFlush();
        walCoordinator.checkpoint();
    }
}
