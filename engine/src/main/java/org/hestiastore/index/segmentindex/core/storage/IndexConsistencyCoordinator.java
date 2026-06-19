package org.hestiastore.index.segmentindex.core.storage;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Owns explicit consistency checks and the startup-only lock validation mode.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class IndexConsistencyCoordinator<K, V> {

    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final RecoverySegmentDirectoryInspector<K> segmentDirectoryInspector;
    private final OrphanedSegmentDirectoryRemover<K, V> orphanedSegmentDirectoryRemover;
    private boolean startupSegmentLockValidationEnabled;

    IndexConsistencyCoordinator(final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final RecoverySegmentDirectoryInspector<K> segmentDirectoryInspector,
            final OrphanedSegmentDirectoryRemover<K, V> orphanedSegmentDirectoryRemover) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.segmentDirectoryInspector = Vldtn.requireNonNull(
                segmentDirectoryInspector, "segmentDirectoryInspector");
        this.orphanedSegmentDirectoryRemover = Vldtn.requireNonNull(
                orphanedSegmentDirectoryRemover,
                "orphanedSegmentDirectoryRemover");
    }

    void checkAndRepairConsistency() {
        keyToSegmentMap.validateUniqueSegmentIds();
        new IndexConsistencyChecker<>(keyToSegmentMap, segmentRegistry,
                this::shouldCheckSegment).checkAndRepairConsistency();
        segmentDirectoryInspector.discoverOrphanedSegmentDirectories()
                .forEach(orphanedSegmentDirectoryRemover::remove);
    }

    void runStartupConsistencyCheck() {
        startupSegmentLockValidationEnabled = true;
        try {
            checkAndRepairConsistency();
        } finally {
            startupSegmentLockValidationEnabled = false;
        }
    }

    private boolean shouldCheckSegment(final SegmentId segmentId) {
        return !startupSegmentLockValidationEnabled
                || segmentDirectoryInspector.hasSegmentLockFile(segmentId);
    }
}
