package org.hestiastore.index.segmentindex.core.split;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.mapping.SegmentRouteSplit;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinates route-first split materialization and route-map publish.
 * <p>
 * Child segments are materialized from the parent segment snapshot first and
 * only then published into the key-to-segment map.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class RouteSplitCoordinator<K, V> {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(RouteSplitCoordinator.class);
    private final SegmentIndexSplitPolicy<K, V> splitPolicy;
    private final RouteSplitPreparationService<K, V> preparationService;

    RouteSplitCoordinator(final SegmentIndexSplitPolicy<K, V> splitPolicy,
            final RouteSplitPreparationService<K, V> preparationService) {
        this.splitPolicy = Vldtn.requireNonNull(splitPolicy, "splitPolicy");
        this.preparationService = Vldtn.requireNonNull(preparationService,
                "preparationService");
    }

    SegmentRouteSplit<K> tryPrepareSplit(
            final BlockingSegment<K, V> segmentHandle,
            final long splitThreshold) {
        final BlockingSegment<K, V> nonNullBlockingSegment = requireBlockingSegment(
                segmentHandle);
        final long estimatedVisibleKeys = estimatedVisibleKeys(
                nonNullBlockingSegment);
        if (!isSplitEligible(nonNullBlockingSegment, estimatedVisibleKeys,
                splitThreshold, isSplitFeasible(estimatedVisibleKeys))) {
            logSkippedSplit(nonNullBlockingSegment, estimatedVisibleKeys,
                    splitThreshold);
            return null;
        }
        logStartedSplit(nonNullBlockingSegment, splitThreshold);
        return preparationService.prepare(nonNullBlockingSegment.getSegment(),
                splitThreshold);
    }

    private boolean shouldSplit(final BlockingSegment<K, V> segmentHandle,
            final long splitThreshold) {
        return splitPolicy.shouldSplit(segmentHandle, splitThreshold);
    }

    private BlockingSegment<K, V> requireBlockingSegment(
            final BlockingSegment<K, V> segmentHandle) {
        return Vldtn.requireNonNull(segmentHandle, "segmentHandle");
    }

    private long estimatedVisibleKeys(final BlockingSegment<K, V> segmentHandle) {
        return segmentHandle.getRuntime().getNumberOfKeysInCache();
    }

    private boolean isSplitFeasible(final long estimatedVisibleKeys) {
        return estimatedVisibleKeys >= 2L;
    }

    private void logSkippedSplit(final BlockingSegment<K, V> segmentHandle,
            final long estimatedVisibleKeys, final long splitThreshold) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    "Route split skipped: segment='{}' estimatedKeys='{}' threshold='{}' splitFeasible='{}'",
                    segmentHandle.getId(), estimatedVisibleKeys, splitThreshold,
                    isSplitFeasible(estimatedVisibleKeys));
        }
    }

    private void logStartedSplit(final BlockingSegment<K, V> segmentHandle,
            final long splitThreshold) {
        LOGGER.debug("Route split started: segment='{}' threshold='{}'",
                segmentHandle.getId(), splitThreshold);
    }

    private boolean isSplitEligible(final BlockingSegment<K, V> segmentHandle,
            final long estimatedVisibleKeys, final long splitThreshold,
            final boolean splitFeasible) {
        if (estimatedVisibleKeys < splitThreshold || !splitFeasible) {
            return false;
        }
        return shouldSplit(segmentHandle, splitThreshold);
    }
}
