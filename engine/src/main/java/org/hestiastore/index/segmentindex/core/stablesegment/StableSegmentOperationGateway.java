package org.hestiastore.index.segmentindex.core.stablesegment;

import java.util.Optional;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.OperationResult;
import org.hestiastore.index.OperationStatus;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Single-attempt gateway for stable-segment operations that returns status
 * wrappers instead of blocking on BUSY.
 */
final class StableSegmentOperationGateway<K, V>
        implements StableSegmentOperationAccess<K, V> {

    private static final String SEGMENT_ID_ARG = "segmentId";

    private final SegmentRegistry<K, V> segmentRegistry;

    StableSegmentOperationGateway(final SegmentRegistry<K, V> segmentRegistry) {
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
    }

    @Override
    public StableSegmentOperationResult<EntryIterator<K, V>> openIterator(
            final SegmentId segmentId,
            final SegmentIteratorIsolation isolation) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
        return withLoadedSegment(segmentId,
                segment -> fromSegmentResult(segment.tryOpenIterator(isolation)));
    }

    @Override
    public StableSegmentOperationResult<BlockingSegment<K, V>> compact(
            final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
        return withLoadedSegment(segmentId, segment -> {
            final StableSegmentOperationResult<Void> result = fromSegmentResult(
                    segment.tryCompact());
            if (result.getStatus() == StableSegmentOperationStatus.OK) {
                return StableSegmentOperationResult.ok(segment);
            }
            return fromIndexStatus(result.getStatus());
        });
    }

    @Override
    public StableSegmentOperationResult<BlockingSegment<K, V>> flush(
            final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
        return withLoadedSegment(segmentId, segment -> {
            final StableSegmentOperationResult<Void> result = fromSegmentResult(
                    segment.tryFlush());
            if (result.getStatus() == StableSegmentOperationStatus.OK) {
                return StableSegmentOperationResult.ok(segment);
            }
            return fromIndexStatus(result.getStatus());
        });
    }

    private <T> StableSegmentOperationResult<T> withLoadedSegment(
            final SegmentId segmentId,
            final java.util.function.Function<BlockingSegment<K, V>,
                    StableSegmentOperationResult<T>> action) {
        final Optional<BlockingSegment<K, V>> loaded = segmentRegistry
                .tryGetSegment(segmentId);
        if (loaded.isEmpty()) {
            return StableSegmentOperationResult.busy();
        }
        return action.apply(loaded.get());
    }

    private static <T> StableSegmentOperationResult<T> fromSegmentResult(
            final OperationResult<T> result) {
        if (result.isOk()) {
            return StableSegmentOperationResult.ok(result.getValue());
        }
        if (result.getStatus() == OperationStatus.BUSY) {
            return StableSegmentOperationResult.busy();
        }
        if (result.getStatus() == OperationStatus.CLOSED) {
            return StableSegmentOperationResult.closed();
        }
        return StableSegmentOperationResult.error();
    }

    private static <T> StableSegmentOperationResult<T> fromIndexStatus(
            final StableSegmentOperationStatus status) {
        if (status == StableSegmentOperationStatus.BUSY) {
            return StableSegmentOperationResult.busy();
        }
        if (status == StableSegmentOperationStatus.CLOSED) {
            return StableSegmentOperationResult.closed();
        }
        if (status == StableSegmentOperationStatus.OK) {
            return StableSegmentOperationResult.ok();
        }
        return StableSegmentOperationResult.error();
    }
}
