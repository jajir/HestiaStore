package org.hestiastore.index.segmentindex.core.split;

import java.util.Comparator;
import java.util.Optional;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentregistry.SegmentHandle;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
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

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final IndexConfiguration<K, V> conf;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final SegmentIndexSplitPolicy<K, V> splitPolicy;
    private final RouteSplitPreparationService<K, V> preparationService;

    RouteSplitCoordinator(final IndexConfiguration<K, V> conf,
            final Comparator<K> keyComparator,
            final SegmentRegistry<K, V> segmentRegistry,
            final DefaultSegmentMaterializationService<K, V> materializationService) {
        this(conf, keyComparator, segmentRegistry, materializationService,
                new SegmentIndexSplitPolicyThreshold<>());
    }

    RouteSplitCoordinator(final IndexConfiguration<K, V> conf,
            final Comparator<K> keyComparator,
            final SegmentRegistry<K, V> segmentRegistry,
            final DefaultSegmentMaterializationService<K, V> materializationService,
            final SegmentIndexSplitPolicy<K, V> splitPolicy) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.splitPolicy = Vldtn.requireNonNull(splitPolicy, "splitPolicy");
        Vldtn.requireNonNull(keyComparator, "keyComparator");
        this.preparationService = new RouteSplitPreparationService<>(
                Vldtn.requireNonNull(materializationService,
                        "materializationService"),
                new IndexRetryPolicy(conf.getIndexBusyBackoffMillis(),
                        conf.getIndexBusyTimeoutMillis()),
                logger);
    }

    RouteSplitPlan<K> tryPrepareSplit(
            final SegmentHandle<K, V> segmentHandle,
            final long splitThreshold) {
        final SegmentHandle<K, V> nonNullSegmentHandle = requireSegmentHandle(
                segmentHandle);
        final long estimatedVisibleKeys = estimatedVisibleKeys(
                nonNullSegmentHandle);
        if (!isSplitEligible(nonNullSegmentHandle, estimatedVisibleKeys,
                splitThreshold, isSplitFeasible(estimatedVisibleKeys))) {
            logSkippedSplit(nonNullSegmentHandle, estimatedVisibleKeys,
                    splitThreshold);
            return null;
        }
        logStartedSplit(nonNullSegmentHandle, splitThreshold);
        if (!isStillCurrentSegment(nonNullSegmentHandle)) {
            return null;
        }
        return preparationService.prepare(nonNullSegmentHandle.getSegment(),
                splitThreshold);
    }

    private boolean shouldSplit(final SegmentHandle<K, V> segmentHandle,
            final long splitThreshold) {
        return splitPolicy.shouldSplit(segmentHandle, splitThreshold);
    }

    private boolean isStillCurrentSegment(
            final SegmentHandle<K, V> segmentHandle) {
        final SegmentId segmentId = segmentHandle.getId();
        final Optional<SegmentHandle<K, V>> currentSegment;
        try {
            currentSegment = segmentRegistry.tryGetSegment(segmentId);
        } catch (final IndexException e) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Route split aborted before validation because registry lookup failed: segment='{}'",
                        segmentId, e);
            }
            return false;
        }
        if (currentSegment.isEmpty()) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Route split aborted before validation because segment is not immediately available: segment='{}'",
                        segmentId);
            }
            return false;
        }
        if (currentSegment.get() != segmentHandle) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Route split aborted because loaded segment changed: segment='{}'",
                        segmentId);
            }
            return false;
        }
        return true;
    }

    private SegmentHandle<K, V> requireSegmentHandle(
            final SegmentHandle<K, V> segmentHandle) {
        return Vldtn.requireNonNull(segmentHandle, "segmentHandle");
    }

    private long estimatedVisibleKeys(final SegmentHandle<K, V> segmentHandle) {
        return segmentHandle.getRuntime().getNumberOfKeysInCache();
    }

    private boolean isSplitFeasible(final long estimatedVisibleKeys) {
        return estimatedVisibleKeys >= 2L;
    }

    private void logSkippedSplit(final SegmentHandle<K, V> segmentHandle,
            final long estimatedVisibleKeys, final long splitThreshold) {
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Route split skipped: segment='{}' estimatedKeys='{}' threshold='{}' splitFeasible='{}'",
                    segmentHandle.getId(), estimatedVisibleKeys, splitThreshold,
                    isSplitFeasible(estimatedVisibleKeys));
        }
    }

    private void logStartedSplit(final SegmentHandle<K, V> segmentHandle,
            final long splitThreshold) {
        logger.debug("Route split started: segment='{}' threshold='{}'",
                segmentHandle.getId(), splitThreshold);
    }

    private boolean isSplitEligible(final SegmentHandle<K, V> segmentHandle,
            final long estimatedVisibleKeys, final long splitThreshold,
            final boolean splitFeasible) {
        if (estimatedVisibleKeys < splitThreshold || !splitFeasible) {
            return false;
        }
        return shouldSplit(segmentHandle, splitThreshold);
    }
}
