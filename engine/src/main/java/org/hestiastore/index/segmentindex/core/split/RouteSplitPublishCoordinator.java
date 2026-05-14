package org.hestiastore.index.segmentindex.core.split;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.SegmentRouteSplit;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns route-map publish and cleanup for an already materialized split plan.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class RouteSplitPublishCoordinator<K, V> {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(RouteSplitPublishCoordinator.class);
    private static final String ROUTE_SPLIT_ARG = "routeSplit";

    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final DefaultSegmentMaterializationService<K, V> materializationService;

    RouteSplitPublishCoordinator(final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final DefaultSegmentMaterializationService<K, V> materializationService) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.materializationService = Vldtn.requireNonNull(
                materializationService, "materializationService");
    }

    boolean applyPreparedSplit(final SegmentRouteSplit<K> routeSplit) {
        Vldtn.requireNonNull(routeSplit, ROUTE_SPLIT_ARG);
        final boolean published;
        try {
            published = keyToSegmentMap.tryReplaceRouteWithSplit(routeSplit);
        } catch (final RuntimeException e) {
            throw abortPreparedSplit(routeSplit, e);
        }
        if (!published) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                        "Route split publish returned false: replacedSegmentId='{}' lowerSegmentId='{}' upperSegmentId='{}'",
                        routeSplit.getReplacedSegmentId(),
                        routeSplit.getLowerSegmentId(),
                        routeSplit.getUpperSegmentId());
            }
            abortPreparedSplit(routeSplit);
            return false;
        }
        completePreparedSplit(routeSplit);
        return true;
    }

    private void completePreparedSplit(final SegmentRouteSplit<K> routeSplit) {
        Vldtn.requireNonNull(routeSplit, ROUTE_SPLIT_ARG);
        keyToSegmentMap.flushIfDirty();
        deleteRetiredParentSegment(routeSplit.getReplacedSegmentId());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    "Route split applied: replacedSegmentId='{}' lowerSegmentId='{}' upperSegmentId='{}' lowerMaxKey='{}'",
                    routeSplit.getReplacedSegmentId(),
                    routeSplit.getLowerSegmentId(),
                    routeSplit.getUpperSegmentId(),
                    routeSplit.getLowerMaxKey());
        }
    }

    private RuntimeException abortPreparedSplit(final SegmentRouteSplit<K> routeSplit,
            final RuntimeException failure) {
        final RuntimeException cleanupFailure = deleteChildSegments(
                Vldtn.requireNonNull(routeSplit, ROUTE_SPLIT_ARG)
                        .getLowerSegmentId(),
                routeSplit.getUpperSegmentId());
        if (cleanupFailure == null) {
            return failure;
        }
        failure.addSuppressed(cleanupFailure);
        return failure;
    }

    private void abortPreparedSplit(final SegmentRouteSplit<K> routeSplit) {
        final RuntimeException cleanupFailure = deleteChildSegments(
                Vldtn.requireNonNull(routeSplit, ROUTE_SPLIT_ARG)
                        .getLowerSegmentId(),
                routeSplit.getUpperSegmentId());
        if (cleanupFailure != null) {
            throw cleanupFailure;
        }
    }

    private void deleteRetiredParentSegment(final SegmentId segmentId) {
        try {
            if (segmentRegistry.deleteSegmentIfAvailable(segmentId)) {
                return;
            }
            LOGGER.warn(
                    "Retired parent segment '{}' remained on disk because delete was busy after split publish.",
                    segmentId);
        } catch (final IndexException e) {
            LOGGER.warn(
                    "Retired parent segment '{}' could not be deleted after split publish: {}",
                    segmentId, e.getMessage());
        }
    }

    private RuntimeException deleteChildSegments(final SegmentId lowerSegmentId,
            final SegmentId upperSegmentId) {
        RuntimeException cleanupFailure = null;
        if (lowerSegmentId != null) {
            cleanupFailure = deletePreparedSegment(lowerSegmentId,
                    cleanupFailure);
        }
        if (upperSegmentId != null) {
            cleanupFailure = deletePreparedSegment(upperSegmentId,
                    cleanupFailure);
        }
        return cleanupFailure;
    }

    private RuntimeException deletePreparedSegment(final SegmentId segmentId,
            final RuntimeException cleanupFailure) {
        try {
            materializationService.deletePreparedSegment(segmentId);
            return cleanupFailure;
        } catch (final RuntimeException e) {
            if (cleanupFailure == null) {
                return e;
            }
            cleanupFailure.addSuppressed(e);
            return cleanupFailure;
        }
    }
}
