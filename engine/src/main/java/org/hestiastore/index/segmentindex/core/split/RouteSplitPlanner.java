package org.hestiastore.index.segmentindex.core.split;

import org.hestiastore.index.Vldtn;
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
final class RouteSplitPlanner<K, V> {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(RouteSplitPlanner.class);
    private final RouteSplitMaterializer<K, V> preparationService;

    RouteSplitPlanner(
            final RouteSplitMaterializer<K, V> preparationService) {
        this.preparationService = Vldtn.requireNonNull(preparationService,
                "preparationService");
    }

    /**
     * Validates split eligibility from the scheduler estimate and delegates to
     * snapshot materialization without recounting parent entries.
     *
     * @param segmentHandle routed segment handle
     * @param splitThreshold active split threshold
     * @param estimatedVisibleKeys scheduler-observed key estimate
     * @return preparation outcome
     */
    RouteSplitPreparation<K> tryPrepareSplit(
            final BlockingSegment<K, V> segmentHandle,
            final long splitThreshold, final long estimatedVisibleKeys) {
        final BlockingSegment<K, V> nonNullBlockingSegment = requireBlockingSegment(
                segmentHandle);
        if (!isSplitEligible(estimatedVisibleKeys, splitThreshold,
                isSplitFeasible(estimatedVisibleKeys))) {
            logSkippedSplit(nonNullBlockingSegment, estimatedVisibleKeys,
                    splitThreshold);
            return RouteSplitPreparation.skipped();
        }
        logStartedSplit(nonNullBlockingSegment, splitThreshold);
        return preparationService.prepare(nonNullBlockingSegment.getSegment(),
                estimatedVisibleKeys);
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

    private boolean isSplitEligible(final long estimatedVisibleKeys,
            final long splitThreshold, final boolean splitFeasible) {
        return splitThreshold >= 1L
                && estimatedVisibleKeys >= splitThreshold
                && splitFeasible;
    }

    private BlockingSegment<K, V> requireBlockingSegment(
            final BlockingSegment<K, V> segmentHandle) {
        return Vldtn.requireNonNull(segmentHandle, "segmentHandle");
    }
}
