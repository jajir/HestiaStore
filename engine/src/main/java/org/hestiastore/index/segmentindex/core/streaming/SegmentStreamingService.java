package org.hestiastore.index.segmentindex.core.streaming;

import java.util.Optional;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.OperationResult;
import org.hestiastore.index.OperationStatus;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.core.stablesegment.StableSegmentOperationGateway;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides stable segment streaming operations.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentStreamingService<K, V> {

    private static final String OPEN_ITERATOR_OPERATION = "openIterator";

    private static final Logger LOGGER = LoggerFactory
            .getLogger(SegmentStreamingService.class);

    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final StableSegmentOperationGateway<K, V> stableSegmentGateway;
    private final BusyRetryPolicy retryPolicy;

    SegmentStreamingService(
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final StableSegmentOperationGateway<K, V> stableSegmentGateway,
            final BusyRetryPolicy retryPolicy) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.stableSegmentGateway = Vldtn.requireNonNull(stableSegmentGateway,
                "stableSegmentGateway");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
    }

    /**
     * Creates a builder for segment streaming services.
     *
     * @param <M> key type
     * @param <N> value type
     * @return segment streaming service builder
     */
    public static <M, N> SegmentStreamingServiceBuilder<M, N> builder() {
        return new SegmentStreamingServiceBuilder<>();
    }

    /**
     * Opens an iterator against one stable segment, retrying transient busy
     * states.
     *
     * @param segmentId segment id
     * @param isolation iterator isolation mode
     * @return entry iterator
     */
    public EntryIterator<K, V> openIterator(final SegmentId segmentId,
            final SegmentIteratorIsolation isolation) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final OperationResult<EntryIterator<K, V>> result = stableSegmentGateway
                    .openIterator(segmentId, isolation);
            if (result.getStatus() == OperationStatus.OK) {
                return result.getValue();
            }
            if (result.getStatus() == OperationStatus.BUSY) {
                retryPolicy.backoffOrThrow(startNanos, OPEN_ITERATOR_OPERATION,
                        segmentId);
                continue;
            }
            throw new IndexException(String.format(
                    "Index operation '%s' failed on segment '%s': %s",
                    OPEN_ITERATOR_OPERATION, segmentId, result.getStatus()));
        }
    }

    /**
     * Invalidates iterators for currently mapped loaded stable segments.
     */
    public void invalidateIterators() {
        forEachMappedSegment(this::invalidateIteratorsForSegment);
    }

    private void invalidateIteratorsForSegment(final SegmentId segmentId) {
        try {
            handleLoadedSegment(segmentId,
                    segmentRegistry.tryGetSegment(segmentId));
        } catch (final IndexException e) {
            logIteratorInvalidationLookupFailure(segmentId, e);
        }
    }

    private void handleLoadedSegment(final SegmentId segmentId,
            final Optional<BlockingSegment<K, V>> loadedSegment) {
        if (loadedSegment.isPresent()) {
            loadedSegment.get().invalidateIterators();
            return;
        }
        logMissingSegment(segmentId);
    }

    private void logMissingSegment(final SegmentId segmentId) {
        if (!isSegmentStillMapped(segmentId)) {
            return;
        }
        LOGGER.debug(
                "Skipping iterator invalidation for segment '{}' because it is "
                        + "not immediately available.",
                segmentId);
    }

    private void logIteratorInvalidationLookupFailure(final SegmentId segmentId,
            final IndexException exception) {
        if (!isSegmentStillMapped(segmentId)) {
            return;
        }
        LOGGER.debug(
                "Skipping iterator invalidation for segment '{}' because registry lookup failed.",
                segmentId, exception);
    }

    private boolean isSegmentStillMapped(final SegmentId segmentId) {
        return keyToSegmentMap.getSegmentIds().contains(segmentId);
    }

    private void forEachMappedSegment(
            final java.util.function.Consumer<SegmentId> segmentAction) {
        keyToSegmentMap.getSegmentIds().forEach(
                Vldtn.requireNonNull(segmentAction, "segmentAction"));
    }
}
