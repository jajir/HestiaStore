package org.hestiastore.index.segmentindex.core.storage;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.slf4j.Logger;

/**
 * Removes orphaned segment directories through the registry with retry-aware
 * cleanup semantics.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class OrphanedSegmentDirectoryRemover<K, V> {

    private static final String OPERATION_CLEANUP_ORPHAN_SEGMENT = "cleanupOrphanSegment";

    private final Logger logger;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final IndexRetryPolicy retryPolicy;

    OrphanedSegmentDirectoryRemover(final Logger logger,
            final SegmentRegistry<K, V> segmentRegistry,
            final IndexRetryPolicy retryPolicy) {
        this.logger = Vldtn.requireNonNull(logger, "logger");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
    }

    void remove(final SegmentId segmentId) {
        final long startNanos = retryPolicy.startNanos();
        boolean deleted = false;
        while (!deleted) {
            try {
                deleted = segmentRegistry.deleteSegmentIfAvailable(segmentId);
                if (deleted) {
                    logDeletedOrphanedSegment(segmentId);
                    return;
                }
                retryPolicy.backoffOrThrow(startNanos,
                        OPERATION_CLEANUP_ORPHAN_SEGMENT, segmentId);
            } catch (final IndexException failure) {
                logDeleteFailure(segmentId, failure);
                return;
            }
        }
    }

    private void logDeletedOrphanedSegment(final SegmentId segmentId) {
        logger.info(
                "Deleted orphaned segment directory '{}' during recovery/consistency cleanup.",
                segmentId);
    }

    private void logDeleteFailure(final SegmentId segmentId,
            final IndexException failure) {
        logger.warn(
                "Orphaned segment directory '{}' could not be deleted during cleanup: {}",
                segmentId, failure.getMessage());
    }
}
