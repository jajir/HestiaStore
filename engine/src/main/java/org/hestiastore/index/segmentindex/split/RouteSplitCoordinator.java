package org.hestiastore.index.segmentindex.split;

import java.util.Comparator;
import java.util.NoSuchElementException;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryResult;
import org.hestiastore.index.segmentregistry.SegmentRegistryResultStatus;
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
public final class RouteSplitCoordinator<K, V> {

    private static final String SEGMENT_ARG = "segment";

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final IndexConfiguration<K, V> conf;
    private final Comparator<K> keyComparator;
    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final SegmentMaterializationService<K, V> materializationService;
    private final SegmentIndexSplitPolicy<K, V> splitPolicy;
    private final IndexRetryPolicy retryPolicy;

    public RouteSplitCoordinator(final IndexConfiguration<K, V> conf,
            final Comparator<K> keyComparator,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final SegmentMaterializationService<K, V> materializationService) {
        this(conf, keyComparator, keyToSegmentMap, segmentRegistry,
                materializationService, new SegmentIndexSplitPolicyThreshold<>());
    }

    RouteSplitCoordinator(final IndexConfiguration<K, V> conf,
            final Comparator<K> keyComparator,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final SegmentMaterializationService<K, V> materializationService,
            final SegmentIndexSplitPolicy<K, V> splitPolicy) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.keyComparator = Vldtn.requireNonNull(keyComparator,
                "keyComparator");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.materializationService = Vldtn.requireNonNull(
                materializationService, "materializationService");
        this.splitPolicy = Vldtn.requireNonNull(splitPolicy, "splitPolicy");
        this.retryPolicy = new IndexRetryPolicy(
                conf.getIndexBusyBackoffMillis(),
                conf.getIndexBusyTimeoutMillis());
    }

    public PreparedRouteSplit<K> tryPrepareSplit(final Segment<K, V> segment,
            final long splitThreshold) {
        Vldtn.requireNonNull(segment, SEGMENT_ARG);
        final long estimatedVisibleKeys = segment.getNumberOfKeysInCache();
        final boolean splitFeasible = estimatedVisibleKeys >= 2L;
        if (!isSplitEligible(segment, estimatedVisibleKeys, splitThreshold,
                splitFeasible)) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Route split skipped: segment='{}' estimatedKeys='{}' threshold='{}' splitFeasible='{}'",
                        segment.getId(), estimatedVisibleKeys, splitThreshold,
                        splitFeasible);
            }
            return null;
        }
        logger.debug("Route split started: segment='{}' threshold='{}'",
                segment.getId(), splitThreshold);
        if (!isStillCurrentSegment(segment)) {
            return null;
        }
        final SplitBoundary<K> boundary = computeSplitBoundary(segment);
        if (boundary == null || boundary.visibleCount() < splitThreshold
                || boundary.visibleCount() < 2L) {
            return null;
        }
        return materializeChildSegments(segment, boundary);
    }

    private boolean shouldSplit(final Segment<K, V> segment,
            final long splitThreshold) {
        return splitPolicy.shouldSplit(segment, splitThreshold);
    }

    public boolean publishPreparedSplit(
            final PreparedRouteSplit<K> preparedSplit) {
        final RouteSplitPlan<K> splitPlan = splitPlan(preparedSplit);
        try {
            if (!keyToSegmentMap.tryApplySplitPlan(splitPlan)) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "Route split publish returned false: replacedSegmentId='{}' lowerSegmentId='{}' upperSegmentId='{}'",
                            splitPlan.getReplacedSegmentId(),
                            splitPlan.getLowerSegmentId(),
                            splitPlan.getUpperSegmentId().orElse(null));
                }
                return false;
            }
            return true;
        } catch (final RuntimeException e) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Route split publish failed: replacedSegmentId='{}' lowerSegmentId='{}' upperSegmentId='{}'",
                        splitPlan.getReplacedSegmentId(),
                        splitPlan.getLowerSegmentId(),
                        splitPlan.getUpperSegmentId().orElse(null), e);
            }
            return false;
        }
    }

    public void completePreparedSplit(
            final PreparedRouteSplit<K> preparedSplit) {
        final RouteSplitPlan<K> splitPlan = splitPlan(preparedSplit);
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

    public void abortPreparedSplit(
            final PreparedRouteSplit<K> preparedSplit) {
        final RouteSplitPlan<K> splitPlan = splitPlan(preparedSplit);
        deleteChildSegments(splitPlan.getLowerSegmentId(),
                splitPlan.getUpperSegmentId().orElse(null));
    }

    private RouteSplitPlan<K> splitPlan(
            final PreparedRouteSplit<K> preparedSplit) {
        Vldtn.requireNonNull(preparedSplit, "preparedSplit");
        return preparedSplit.plan();
    }

    private boolean isStillCurrentSegment(final Segment<K, V> segment) {
        final SegmentId segmentId = segment.getId();
        final SegmentRegistryResult<Segment<K, V>> currentResult = segmentRegistry
                .getSegment(segmentId);
        if (currentResult.getStatus() != SegmentRegistryResultStatus.OK
                || currentResult.getValue() == null) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Route split aborted before validation: segment='{}' registryStatus='{}'",
                        segmentId, currentResult.getStatus());
            }
            return false;
        }
        final Segment<K, V> currentSegment = currentResult.getValue();
        if (currentSegment != segment) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Route split aborted because loaded segment changed: segment='{}'",
                        segmentId);
            }
            return false;
        }
        return true;
    }

    private SplitBoundary<K> computeSplitBoundary(final Segment<K, V> segment) {
        final long visibleCount = countVisibleEntries(segment);
        if (visibleCount < 2L) {
            return null;
        }
        final long targetLowerCount = Math.max(1L, visibleCount / 2L);
        try (EntryIterator<K, V> iterator = openIteratorWithRetry(segment,
                SegmentIteratorIsolation.FULL_ISOLATION)) {
            if (iterator == null) {
                return null;
            }
            K minKey = null;
            K maxLowerKey = null;
            long lowerCount = 0L;
            while (iterator.hasNext() && lowerCount < targetLowerCount) {
                final Entry<K, V> entry = iterator.next();
                if (minKey == null) {
                    minKey = entry.getKey();
                }
                maxLowerKey = entry.getKey();
                lowerCount++;
            }
            if (minKey == null || maxLowerKey == null
                    || lowerCount >= visibleCount) {
                return null;
            }
            return new SplitBoundary<>(minKey, maxLowerKey, visibleCount);
        }
    }

    private long countVisibleEntries(final Segment<K, V> segment) {
        try (EntryIterator<K, V> iterator = openIteratorWithRetry(segment,
                SegmentIteratorIsolation.FULL_ISOLATION)) {
            if (iterator == null) {
                return 0L;
            }
            long count = 0L;
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }
            return count;
        }
    }

    private PreparedRouteSplit<K> materializeChildSegments(
            final Segment<K, V> parentSegment, final SplitBoundary<K> boundary) {
        Vldtn.requireNonNull(parentSegment, SEGMENT_ARG);
        Vldtn.requireNonNull(boundary, "boundary");
        PreparedSegmentHandle<K, V> lowerSegment = null;
        PreparedSegmentHandle<K, V> upperSegment = null;
        boolean materializationCompleted = false;
        try {
            final EntryIterator<K, V> openedIterator = openIteratorWithRetry(
                    parentSegment, SegmentIteratorIsolation.FULL_ISOLATION);
            if (openedIterator == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "Route split aborted because parent segment closed before child materialization completed: segment='{}'",
                            parentSegment.getId());
                }
                return null;
            }
            lowerSegment = materializationService.openPreparedSegment();
            try (EntryIterator<K, V> iterator = openedIterator) {
                while (iterator.hasNext()) {
                    final Entry<K, V> entry = iterator.next();
                    if (isLowerKey(boundary, entry.getKey())) {
                        lowerSegment.write(entry);
                        continue;
                    }
                    if (upperSegment == null) {
                        upperSegment = materializationService
                                .openPreparedSegment();
                    }
                    upperSegment.write(entry);
                }
                if (upperSegment == null) {
                    return null;
                }
                lowerSegment.commit();
                upperSegment.commit();
            } catch (final RuntimeException e) {
                if (isIteratorInvalidated(e)) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                                "Route split aborted because parent iterator was invalidated during child materialization: segment='{}'",
                                parentSegment.getId());
                    }
                    return null;
                }
                throw e;
            }
            materializationCompleted = true;
            return new PreparedRouteSplit<>(new RouteSplitPlan<>(
                    parentSegment.getId(), lowerSegment.segmentId(),
                    upperSegment.segmentId(), boundary.minKey(),
                    boundary.maxLowerKey(), RouteSplitPlan.SplitMode.SPLIT));
        } finally {
            if (materializationCompleted) {
                closePreparedSegment(lowerSegment);
                closePreparedSegment(upperSegment);
            } else {
                discardPreparedSegment(lowerSegment);
                discardPreparedSegment(upperSegment);
            }
        }
    }

    private EntryIterator<K, V> openIteratorWithRetry(
            final Segment<K, V> segment,
            final SegmentIteratorIsolation isolation) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final SegmentResult<EntryIterator<K, V>> result = segment
                    .openIterator(isolation);
            if (result.getStatus() == SegmentResultStatus.OK
                    && result.getValue() != null) {
                return result.getValue();
            }
            if (result.getStatus() == SegmentResultStatus.BUSY) {
                retryPolicy.backoffOrThrow(startNanos, "openIterator",
                        segment.getId());
                continue;
            }
            if (result.getStatus() == SegmentResultStatus.CLOSED) {
                return null;
            }
            throw new IndexException(String.format(
                    "Segment '%s' failed to open iterator for child materialization: %s",
                    segment.getId(), result.getStatus()));
        }
    }

    private boolean isLowerKey(final SplitBoundary<K> boundary, final K key) {
        return keyComparator.compare(key, boundary.maxLowerKey()) <= 0;
    }

    private boolean isIteratorInvalidated(final RuntimeException e) {
        return e instanceof NoSuchElementException;
    }

    private void deleteRetiredParentSegment(final SegmentId segmentId) {
        final SegmentRegistryResultStatus status = segmentRegistry
                .deleteSegment(segmentId).getStatus();
        if (status == SegmentRegistryResultStatus.BUSY) {
            logger.warn(
                    "Retired parent segment '{}' remained on disk because delete was busy after split publish.",
                    segmentId);
            return;
        }
        if (status != SegmentRegistryResultStatus.OK
                && status != SegmentRegistryResultStatus.CLOSED) {
            logger.warn(
                    "Retired parent segment '{}' could not be deleted after split publish: {}",
                    segmentId, status);
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

    private void discardPreparedSegment(
            final PreparedSegmentHandle<K, V> segment) {
        if (segment != null) {
            segment.discard();
        }
    }

    private void closePreparedSegment(final PreparedSegmentHandle<K, V> segment) {
        if (segment != null) {
            segment.close();
        }
    }

    private boolean isSplitEligible(final Segment<K, V> segment,
            final long estimatedVisibleKeys, final long splitThreshold,
            final boolean splitFeasible) {
        if (estimatedVisibleKeys < splitThreshold || !splitFeasible) {
            return false;
        }
        return shouldSplit(segment, splitThreshold);
    }

    private record SplitBoundary<K>(K minKey, K maxLowerKey, long visibleCount) {
    }
}
