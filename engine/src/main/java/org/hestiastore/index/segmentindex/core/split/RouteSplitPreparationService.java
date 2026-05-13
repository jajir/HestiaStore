package org.hestiastore.index.segmentindex.core.split;

import java.util.NoSuchElementException;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.OperationResult;
import org.hestiastore.index.OperationStatus;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.mapping.SegmentRouteSplitPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prepares route splits by choosing a split boundary and materializing child
 * segments from a stable parent snapshot.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class RouteSplitPreparationService<K, V> {

    private static final String SEGMENT_ARG = "segment";
    private static final Logger LOGGER = LoggerFactory
            .getLogger(RouteSplitPreparationService.class);

    private final DefaultSegmentMaterializationService<K, V> materializationService;
    private final IndexRetryPolicy retryPolicy;

    RouteSplitPreparationService(
            final DefaultSegmentMaterializationService<K, V> materializationService,
            final IndexRetryPolicy retryPolicy) {
        this.materializationService = Vldtn.requireNonNull(
                materializationService, "materializationService");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
    }

    SegmentRouteSplitPlan<K> prepare(final Segment<K, V> parentSegment,
            final long splitThreshold) {
        final Segment<K, V> nonNullParentSegment = Vldtn
                .requireNonNull(parentSegment, SEGMENT_ARG);
        final long visibleCount = countVisibleEntries(nonNullParentSegment);
        if (!isEligibleVisibleCount(visibleCount, splitThreshold)) {
            return null;
        }
        return materializeChildSegments(nonNullParentSegment,
                targetLowerCount(visibleCount));
    }

    private boolean isEligibleVisibleCount(final long visibleCount,
            final long splitThreshold) {
        return visibleCount >= splitThreshold && visibleCount >= 2L;
    }

    private long targetLowerCount(final long visibleCount) {
        return Math.max(1L, visibleCount / 2L);
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

    private SegmentRouteSplitPlan<K> materializeChildSegments(
            final Segment<K, V> parentSegment, final long targetLowerCount) {
        try (EntryIterator<K, V> iterator = openIteratorWithRetry(parentSegment,
                SegmentIteratorIsolation.FULL_ISOLATION)) {
            if (iterator == null) {
                logMaterializationAbortedBecauseParentClosed(parentSegment);
                return null;
            }
            try {
                return materializationService.materializeRouteSplit(
                        parentSegment, targetLowerCount, iterator);
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
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    "Route split aborted because parent segment closed before child materialization completed: segment='{}'",
                    parentSegment.getId());
        }
    }

    private void logMaterializationAbortedBecauseIteratorInvalidated(
            final Segment<K, V> parentSegment) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    "Route split aborted because parent iterator was invalidated during child materialization: segment='{}'",
                    parentSegment.getId());
        }
    }

    private boolean isIteratorInvalidated(final RuntimeException e) {
        return e instanceof NoSuchElementException;
    }
}
