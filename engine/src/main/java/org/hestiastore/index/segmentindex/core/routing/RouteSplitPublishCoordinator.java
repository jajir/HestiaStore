package org.hestiastore.index.segmentindex.core.routing;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
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

    private final Logger logger = LoggerFactory.getLogger(getClass());
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

    boolean applyPreparedSplit(final RouteSplitPlan<K> splitPlan) {
        Vldtn.requireNonNull(splitPlan, "splitPlan");
        try {
            if (!keyToSegmentMap.tryApplySplitPlan(splitPlan)) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "Route split publish returned false: replacedSegmentId='{}' lowerSegmentId='{}' upperSegmentId='{}'",
                            splitPlan.getReplacedSegmentId(),
                            splitPlan.getLowerSegmentId(),
                            splitPlan.getUpperSegmentId().orElse(null));
                }
                abortPreparedSplit(splitPlan);
                return false;
            }
            completePreparedSplit(splitPlan);
            return true;
        } catch (final RuntimeException e) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Route split publish failed: replacedSegmentId='{}' lowerSegmentId='{}' upperSegmentId='{}'",
                            splitPlan.getReplacedSegmentId(),
                            splitPlan.getLowerSegmentId(),
                            splitPlan.getUpperSegmentId().orElse(null), e);
            }
            abortPreparedSplit(splitPlan);
            return false;
        }
    }

    private void completePreparedSplit(final RouteSplitPlan<K> splitPlan) {
        Vldtn.requireNonNull(splitPlan, "splitPlan");
        keyToSegmentMap.flushIfDirty();
        deleteRetiredParentSegment(splitPlan.getReplacedSegmentId());
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Route split applied: replacedSegmentId='{}' lowerSegmentId='{}' upperSegmentId='{}' lowerMaxKey='{}'",
                    splitPlan.getReplacedSegmentId(),
                    splitPlan.getLowerSegmentId(),
                    splitPlan.getUpperSegmentId().orElse(null),
                    splitPlan.getLowerMaxKey());
        }
    }

    private void abortPreparedSplit(final RouteSplitPlan<K> splitPlan) {
        Vldtn.requireNonNull(splitPlan, "splitPlan");
        deleteChildSegments(splitPlan.getLowerSegmentId(),
                splitPlan.getUpperSegmentId().orElse(null));
    }

    private void deleteRetiredParentSegment(final SegmentId segmentId) {
        try {
            if (segmentRegistry.deleteSegmentIfAvailable(segmentId)) {
                return;
            }
            logger.warn(
                    "Retired parent segment '{}' remained on disk because delete was busy after split publish.",
                    segmentId);
        } catch (final IndexException e) {
            logger.warn(
                    "Retired parent segment '{}' could not be deleted after split publish: {}",
                    segmentId, e.getMessage());
        }
    }

    private void deleteChildSegments(final SegmentId lowerSegmentId,
            final SegmentId upperSegmentId) {
        if (lowerSegmentId != null) {
            materializationService.deletePreparedSegment(lowerSegmentId);
        }
        if (upperSegmentId != null) {
            materializationService.deletePreparedSegment(upperSegmentId);
        }
    }
}
