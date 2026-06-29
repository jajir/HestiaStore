package org.hestiastore.index.segmentindex.core.storage;

import java.util.function.Predicate;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Owns explicit consistency checks and the startup-only lock validation mode.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class StorageConsistencyCoordinator<K, V> {

    private final SegmentRouteMap<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final SegmentDirectoryRecoveryScanner<K> segmentDirectoryInspector;
    private final OrphanedSegmentCleaner<K, V> orphanedSegmentDirectoryRemover;
    private final RouteMapConsistencyChecker<K, V> checker;

    StorageConsistencyCoordinator(final SegmentRouteMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final SegmentDirectoryRecoveryScanner<K> segmentDirectoryInspector,
            final OrphanedSegmentCleaner<K, V> orphanedSegmentDirectoryRemover) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.segmentDirectoryInspector = Vldtn.requireNonNull(
                segmentDirectoryInspector, "segmentDirectoryInspector");
        this.orphanedSegmentDirectoryRemover = Vldtn.requireNonNull(
                orphanedSegmentDirectoryRemover,
                "orphanedSegmentDirectoryRemover");
        checker = new RouteMapConsistencyChecker<>(this.keyToSegmentMap,
                this.segmentRegistry);
    }

    void checkAndRepairConsistency() {
        keyToSegmentMap.validateUniqueSegmentIds();
        checker.checkAndRepairConsistency();
        cleanupOrphanedSegmentDirectories();
    }

    void runStartupConsistencyCheck() {
        checkAndRepairConsistency(segmentDirectoryInspector::hasSegmentLockFile);
    }

    /**
     * Deletes physical segment directories not referenced by the route map.
     */
    void cleanupOrphanedSegmentDirectories() {
        segmentDirectoryInspector.discoverOrphanedSegmentDirectories()
                .forEach(orphanedSegmentDirectoryRemover::remove);
    }

    private void checkAndRepairConsistency(
            final Predicate<SegmentId> segmentFilter) {
        keyToSegmentMap.validateUniqueSegmentIds();
        checker.checkAndRepairConsistency(segmentFilter);
        cleanupOrphanedSegmentDirectories();
    }
}
