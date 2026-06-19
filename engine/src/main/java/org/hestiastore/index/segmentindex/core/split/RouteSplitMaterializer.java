package org.hestiastore.index.segmentindex.core.split;

import java.util.NoSuchElementException;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.OperationResult;
import org.hestiastore.index.OperationStatus;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.routemap.RouteSplitPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prepares route splits by choosing a split boundary and materializing child
 * segments from a stable parent snapshot.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class RouteSplitMaterializer<K, V> {

    private static final long MIN_KEYS_PER_CHILD_SEGMENT = 3L;
    private static final long MIN_VISIBLE_KEYS_FOR_FALLBACK_SPLIT =
            MIN_KEYS_PER_CHILD_SEGMENT * 2L;
    private static final String SEGMENT_ARG = "segment";
    private static final Logger LOGGER = LoggerFactory
            .getLogger(RouteSplitMaterializer.class);

    private final PreparedSegmentMaterializer<K, V> materializationService;
    private final BusyRetryPolicy retryPolicy;

    RouteSplitMaterializer(
            final PreparedSegmentMaterializer<K, V> materializationService,
            final BusyRetryPolicy retryPolicy) {
        this.materializationService = Vldtn.requireNonNull(
                materializationService, "materializationService");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
    }

    RouteSplitPlan<K> prepare(final Segment<K, V> parentSegment,
            final long splitThreshold) {
        final Segment<K, V> nonNullParentSegment = Vldtn
                .requireNonNull(parentSegment, SEGMENT_ARG);
        final Long visibleCount = countVisibleEntries(nonNullParentSegment);
        if (visibleCount == null) {
            logEligibilityRecountAbortedBecauseParentClosed(
                    nonNullParentSegment);
            return null;
        }
        final long recountedVisibleCount = visibleCount.longValue();
        if (!shouldContinueSplitAfterVisibleRecount(nonNullParentSegment,
                recountedVisibleCount, splitThreshold)) {
            logSplitAbortedAfterRecountBelowThreshold(nonNullParentSegment,
                    recountedVisibleCount, splitThreshold);
            return null;
        }
        return materializeChildSegments(nonNullParentSegment,
                targetLowerCount(recountedVisibleCount));
    }

    private boolean shouldContinueSplitAfterVisibleRecount(
            final Segment<K, V> parentSegment, final long visibleCount,
            final long splitThreshold) {
        if (visibleCount >= splitThreshold) {
            return true;
        }
        if (visibleCount < MIN_VISIBLE_KEYS_FOR_FALLBACK_SPLIT) {
            return false;
        }
        logSplitContinuingAfterBelowThresholdRecount(parentSegment,
                visibleCount, splitThreshold);
        return true;
    }

    private long targetLowerCount(final long visibleCount) {
        return Math.max(1L, visibleCount / 2L);
    }

    private Long countVisibleEntries(final Segment<K, V> segment) {
        try (EntryIterator<K, V> iterator = openIteratorWithRetry(segment,
                SegmentIteratorIsolation.FULL_ISOLATION)) {
            if (iterator == null) {
                return null;
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

    private void logEligibilityRecountAbortedBecauseParentClosed(
            final Segment<K, V> parentSegment) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    "Route split aborted because parent segment closed before eligibility recount completed: segment='{}'",
                    parentSegment.getId());
        }
    }

    private void logSplitAbortedAfterRecountBelowThreshold(
            final Segment<K, V> parentSegment, final long visibleCount,
            final long splitThreshold) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    "Route split aborted after eligibility recount fell below threshold: segment='{}' visibleKeys='{}' threshold='{}'",
                    parentSegment.getId(), visibleCount, splitThreshold);
        }
    }

    private void logSplitContinuingAfterBelowThresholdRecount(
            final Segment<K, V> parentSegment, final long visibleCount,
            final long splitThreshold) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    "Route split continuing after eligibility recount fell below threshold because minimum child size is satisfied: segment='{}' visibleKeys='{}' threshold='{}' minKeysPerChild='{}'",
                    parentSegment.getId(), visibleCount, splitThreshold,
                    MIN_KEYS_PER_CHILD_SEGMENT);
        }
    }

    private boolean isIteratorInvalidated(final RuntimeException e) {
        return e instanceof NoSuchElementException;
    }
}
