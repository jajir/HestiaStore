package org.hestiastore.index.segmentindex.core.streaming;

import java.util.Optional;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.core.stablesegment.StableSegmentOperationAccess;
import org.hestiastore.index.segmentindex.core.stablesegment.StableSegmentOperationResult;
import org.hestiastore.index.segmentindex.core.stablesegment.StableSegmentOperationStatus;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.slf4j.Logger;

final class SegmentStreamingServiceImpl<K, V>
        implements SegmentStreamingService<K, V> {

    private static final String OPEN_ITERATOR_OPERATION = "openIterator";

    private final Logger logger;
    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final StableSegmentOperationAccess<K, V> stableSegmentGateway;
    private final IndexRetryPolicy retryPolicy;

    SegmentStreamingServiceImpl(final Logger logger,
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final StableSegmentOperationAccess<K, V> stableSegmentGateway,
            final IndexRetryPolicy retryPolicy) {
        this.logger = Vldtn.requireNonNull(logger, "logger");
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.stableSegmentGateway = Vldtn.requireNonNull(stableSegmentGateway,
                "stableSegmentGateway");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
    }

    @Override
    public EntryIterator<K, V> openIterator(final SegmentId segmentId,
            final SegmentIteratorIsolation isolation) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final StableSegmentOperationResult<EntryIterator<K, V>> result = stableSegmentGateway
                    .openIterator(segmentId, isolation);
            if (result.getStatus() == StableSegmentOperationStatus.OK) {
                return result.getValue();
            }
            if (result.getStatus() == StableSegmentOperationStatus.BUSY) {
                retryPolicy.backoffOrThrow(startNanos, OPEN_ITERATOR_OPERATION,
                        segmentId);
                continue;
            }
            throw new IndexException(String.format(
                    "Index operation '%s' failed on segment '%s': %s",
                    OPEN_ITERATOR_OPERATION, segmentId, result.getStatus()));
        }
    }

    @Override
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
        logger.debug(
                "Skipping iterator invalidation for segment '{}' because it is "
                        + "not immediately available.",
                segmentId);
    }

    private void logIteratorInvalidationLookupFailure(final SegmentId segmentId,
            final IndexException exception) {
        if (!isSegmentStillMapped(segmentId)) {
            return;
        }
        logger.debug(
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
