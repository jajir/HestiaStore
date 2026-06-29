package org.hestiastore.index.segmentindex.core.storage;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Removes orphaned segment directories through the registry with retry-aware
 * cleanup semantics.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class OrphanedSegmentCleaner<K, V> {

    private static final String OPERATION_CLEANUP_ORPHAN_SEGMENT = "cleanupOrphanSegment";

    private static final Logger LOGGER = LoggerFactory
            .getLogger(OrphanedSegmentCleaner.class);

    private final SegmentRegistry<K, V> segmentRegistry;
    private final BusyRetryPolicy retryPolicy;

    OrphanedSegmentCleaner(
            final SegmentRegistry<K, V> segmentRegistry,
            final BusyRetryPolicy retryPolicy) {
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
    }

    void remove(final SegmentId segmentId) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            try {
                if (segmentRegistry.deleteSegmentIfAvailable(segmentId)) {
                    LOGGER.info(
                            "Deleted orphaned segment directory '{}' during recovery/consistency cleanup.",
                            segmentId);
                    return;
                }
                retryPolicy.backoffOrThrow(startNanos,
                        OPERATION_CLEANUP_ORPHAN_SEGMENT, segmentId);
            } catch (final IndexException failure) {
                LOGGER.warn(
                        "Orphaned segment directory '{}' could not be deleted during cleanup: {}",
                        segmentId, failure.getMessage());
                return;
            }
        }
    }
}
