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

    /**
     * Opens one isolated parent snapshot and materializes child segments using
     * the caller-provided visible-key estimate for the lower-child target.
     *
     * @param parentSegment parent segment
     * @param estimatedVisibleKeys scheduler-observed visible-key estimate
     * @return preparation outcome
     */
    RouteSplitPreparation<K> prepare(final Segment<K, V> parentSegment,
            final long estimatedVisibleKeys) {
        final Segment<K, V> nonNullParentSegment = Vldtn
                .requireNonNull(parentSegment, SEGMENT_ARG);
        requireEstimatedVisibleKeys(estimatedVisibleKeys);
        return materializeChildSegments(nonNullParentSegment,
                targetLowerCount(estimatedVisibleKeys));
    }

    private void requireEstimatedVisibleKeys(final long estimatedVisibleKeys) {
        if (estimatedVisibleKeys < 0L) {
            throw new IllegalArgumentException(String.format(
                    "Property 'estimatedVisibleKeys' must be >= 0 but was %d.",
                    estimatedVisibleKeys));
        }
    }

    private long targetLowerCount(final long estimatedVisibleKeys) {
        return Math.max(MIN_KEYS_PER_CHILD_SEGMENT, estimatedVisibleKeys / 2L);
    }

    private RouteSplitPreparation<K> materializeChildSegments(
            final Segment<K, V> parentSegment, final long targetLowerCount) {
        try (EntryIterator<K, V> iterator = openIteratorWithRetry(parentSegment,
                SegmentIteratorIsolation.FULL_ISOLATION)) {
            if (iterator == null) {
                logMaterializationAbortedBecauseParentClosed(parentSegment);
                return RouteSplitPreparation.skipped();
            }
            try {
                return materializationService.materializeRouteSplit(
                        parentSegment, targetLowerCount,
                        MIN_KEYS_PER_CHILD_SEGMENT, iterator);
            } catch (final RuntimeException e) {
                if (e instanceof NoSuchElementException) {
                    logMaterializationAbortedBecauseIteratorInvalidated(
                            parentSegment);
                    return RouteSplitPreparation.skipped();
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

}
