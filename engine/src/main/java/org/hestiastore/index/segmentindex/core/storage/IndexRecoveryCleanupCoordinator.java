package org.hestiastore.index.segmentindex.core.storage;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.slf4j.Logger;

/**
 * Owns recovery-time cleanup of orphaned segment directories and stale lock
 * probing for consistency checks.
 */
public final class IndexRecoveryCleanupCoordinator<K, V> {

    private final RecoverySegmentDirectoryInspector<K> segmentDirectoryInspector;
    private final OrphanedSegmentDirectoryRemover<K, V> orphanedSegmentDirectoryRemover;

    public static <K, V> IndexRecoveryCleanupCoordinator<K, V> create(
            final Logger logger,
            final Directory directoryFacade,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final IndexRetryPolicy retryPolicy) {
        return new IndexRecoveryCleanupCoordinator<>(
                new RecoverySegmentDirectoryInspector<>(
                Vldtn.requireNonNull(directoryFacade, "directoryFacade"),
                Vldtn.requireNonNull(keyToSegmentMap, "keyToSegmentMap")),
                new OrphanedSegmentDirectoryRemover<>(
                        Vldtn.requireNonNull(logger, "logger"),
                        Vldtn.requireNonNull(segmentRegistry,
                                "segmentRegistry"),
                        Vldtn.requireNonNull(retryPolicy, "retryPolicy")));
    }

    IndexRecoveryCleanupCoordinator(
            final RecoverySegmentDirectoryInspector<K> segmentDirectoryInspector,
            final OrphanedSegmentDirectoryRemover<K, V> orphanedSegmentDirectoryRemover) {
        this.segmentDirectoryInspector = Vldtn.requireNonNull(
                segmentDirectoryInspector, "segmentDirectoryInspector");
        this.orphanedSegmentDirectoryRemover = Vldtn.requireNonNull(
                orphanedSegmentDirectoryRemover,
                "orphanedSegmentDirectoryRemover");
    }

    public void cleanupOrphanedSegmentDirectories() {
        segmentDirectoryInspector.discoverOrphanedSegmentDirectories()
                .forEach(orphanedSegmentDirectoryRemover::remove);
    }

    public boolean hasSegmentLockFile(final SegmentId segmentId) {
        return segmentDirectoryInspector.hasSegmentLockFile(
                Vldtn.requireNonNull(segmentId, "segmentId"));
    }
}
