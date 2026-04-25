package org.hestiastore.index.segmentindex.core.routing;

import java.util.Optional;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
import org.hestiastore.index.segmentregistry.SegmentHandle;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Single-attempt gateway for stable-segment operations that returns status
 * wrappers instead of blocking on BUSY.
 */
final class StableSegmentGateway<K, V> implements StableSegmentAccess<K, V> {

    private static final String SEGMENT_ID_ARG = "segmentId";

    private final SegmentRegistry<K, V> segmentRegistry;

    StableSegmentGateway(final SegmentRegistry<K, V> segmentRegistry) {
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
    }

    @Override
    public IndexResult<V> get(final SegmentId segmentId, final K key) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
        return withLoadedSegment(segmentId,
                segment -> fromSegmentResult(segment.tryGet(key)));
    }

    @Override
    public IndexResult<Void> put(final SegmentId segmentId, final K key,
            final V value) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
        Vldtn.requireNonNull(key, "key");
        Vldtn.requireNonNull(value, "value");
        return withLoadedSegment(segmentId,
                segment -> fromSegmentResult(segment.tryPut(key, value)));
    }

    @Override
    public IndexResult<EntryIterator<K, V>> openIterator(
            final SegmentId segmentId,
            final SegmentIteratorIsolation isolation) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
        return withLoadedSegment(segmentId,
                segment -> fromSegmentResult(segment.tryOpenIterator(isolation)));
    }

    @Override
    public IndexResult<SegmentHandle<K, V>> compact(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
        return withLoadedSegment(segmentId, segment -> {
            final IndexResult<Void> result = fromSegmentResult(
                    segment.tryCompact());
            if (result.getStatus() == IndexResultStatus.OK) {
                return IndexResult.ok(segment);
            }
            return fromIndexStatus(result.getStatus());
        });
    }

    @Override
    public IndexResult<SegmentHandle<K, V>> flush(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
        return withLoadedSegment(segmentId, segment -> {
            final IndexResult<Void> result = fromSegmentResult(
                    segment.tryFlush());
            if (result.getStatus() == IndexResultStatus.OK) {
                return IndexResult.ok(segment);
            }
            return fromIndexStatus(result.getStatus());
        });
    }

    private <T> IndexResult<T> withLoadedSegment(final SegmentId segmentId,
            final java.util.function.Function<SegmentHandle<K, V>, IndexResult<T>> action) {
        final Optional<SegmentHandle<K, V>> loaded = segmentRegistry
                .tryGetSegment(segmentId);
        if (loaded.isEmpty()) {
            return IndexResult.busy();
        }
        return action.apply(loaded.get());
    }

    private static <T> IndexResult<T> fromSegmentResult(
            final SegmentResult<T> result) {
        if (result.getStatus() == SegmentResultStatus.OK) {
            return IndexResult.ok(result.getValue());
        }
        if (result.getStatus() == SegmentResultStatus.BUSY) {
            return IndexResult.busy();
        }
        if (result.getStatus() == SegmentResultStatus.CLOSED) {
            return IndexResult.closed();
        }
        return IndexResult.error();
    }

    private static <T> IndexResult<T> fromIndexStatus(
            final IndexResultStatus status) {
        if (status == IndexResultStatus.BUSY) {
            return IndexResult.busy();
        }
        if (status == IndexResultStatus.CLOSED) {
            return IndexResult.closed();
        }
        if (status == IndexResultStatus.OK) {
            return IndexResult.ok();
        }
        return IndexResult.error();
    }
}
