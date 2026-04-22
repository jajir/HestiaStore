package org.hestiastore.index.segmentindex.core.routing;

import java.util.Comparator;
import java.util.NoSuchElementException;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.OperationResult;
import org.hestiastore.index.OperationStatus;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.slf4j.Logger;

/**
 * Prepares route splits by choosing a split boundary and materializing child
 * segments from a stable parent snapshot.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class RouteSplitPreparationService<K, V> {

    private static final String SEGMENT_ARG = "segment";

    private final Comparator<K> keyComparator;
    private final DefaultSegmentMaterializationService<K, V> materializationService;
    private final IndexRetryPolicy retryPolicy;
    private final Logger logger;

    RouteSplitPreparationService(final Comparator<K> keyComparator,
            final DefaultSegmentMaterializationService<K, V> materializationService,
            final IndexRetryPolicy retryPolicy, final Logger logger) {
        this.keyComparator = Vldtn.requireNonNull(keyComparator,
                "keyComparator");
        this.materializationService = Vldtn.requireNonNull(
                materializationService, "materializationService");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
        this.logger = Vldtn.requireNonNull(logger, "logger");
    }

    RouteSplitPlan<K> prepare(final Segment<K, V> parentSegment,
            final long splitThreshold) {
        final Segment<K, V> nonNullParentSegment = Vldtn
                .requireNonNull(parentSegment, SEGMENT_ARG);
        final SplitBoundary<K> boundary = computeEligibleBoundary(
                nonNullParentSegment, splitThreshold);
        if (boundary == null) {
            return null;
        }
        return materializeChildSegments(nonNullParentSegment, boundary);
    }

    private SplitBoundary<K> computeEligibleBoundary(
            final Segment<K, V> segment, final long splitThreshold) {
        final SplitBoundary<K> boundary = computeSplitBoundary(segment);
        if (boundary == null || boundary.visibleCount() < splitThreshold
                || boundary.visibleCount() < 2L) {
            return null;
        }
        return boundary;
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

    private RouteSplitPlan<K> materializeChildSegments(
            final Segment<K, V> parentSegment, final SplitBoundary<K> boundary) {
        try (EntryIterator<K, V> iterator = openIteratorWithRetry(parentSegment,
                SegmentIteratorIsolation.FULL_ISOLATION)) {
            if (iterator == null) {
                logMaterializationAbortedBecauseParentClosed(parentSegment);
                return null;
            }
            try {
                return materializationService.materializeRouteSplit(
                        parentSegment, boundary.minKey(),
                        boundary.maxLowerKey(), keyComparator, iterator);
            } catch (final RuntimeException e) {
                if (isIteratorInvalidated(e)) {
                    logMaterializationAbortedBecauseIteratorInvalidated(
                            parentSegment);
                    return null;
                }
                throw e;
            }
        }
    }

    private EntryIterator<K, V> openIteratorWithRetry(
            final Segment<K, V> segment,
            final SegmentIteratorIsolation isolation) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final OperationResult<EntryIterator<K, V>> result = segment
                    .openIterator(isolation);
            if (result.getStatus() == OperationStatus.OK
                    && result.getValue() != null) {
                return result.getValue();
            }
            if (result.getStatus() == OperationStatus.BUSY) {
                retryPolicy.backoffOrThrow(startNanos, "openIterator",
                        segment.getId());
                continue;
            }
            if (result.getStatus() == OperationStatus.CLOSED) {
                return null;
            }
            throw new IndexException(String.format(
                    "Segment '%s' failed to open iterator for child materialization: %s",
                    segment.getId(), result.getStatus()));
        }
    }

    private void logMaterializationAbortedBecauseParentClosed(
            final Segment<K, V> parentSegment) {
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Route split aborted because parent segment closed before child materialization completed: segment='{}'",
                    parentSegment.getId());
        }
    }

    private void logMaterializationAbortedBecauseIteratorInvalidated(
            final Segment<K, V> parentSegment) {
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Route split aborted because parent iterator was invalidated during child materialization: segment='{}'",
                    parentSegment.getId());
        }
    }

    private boolean isIteratorInvalidated(final RuntimeException e) {
        return e instanceof NoSuchElementException;
    }

    private record SplitBoundary<K>(K minKey, K maxLowerKey, long visibleCount) {
    }
}
