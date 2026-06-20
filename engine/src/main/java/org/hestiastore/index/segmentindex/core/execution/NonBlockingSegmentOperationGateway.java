package org.hestiastore.index.segmentindex.core.execution;

import java.util.Optional;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.OperationResult;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Single-attempt gateway for stable-segment operations without blocking on
 * BUSY.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class NonBlockingSegmentOperationGateway<K, V> {

    private static final String SEGMENT_ID_ARG = "segmentId";

    private final SegmentRegistry<K, V> segmentRegistry;

    /**
     * Creates a stable-segment operation gateway.
     *
     * @param <K> key type
     * @param <V> value type
     * @param segmentRegistry segment registry
     * @return stable-segment operation gateway
     */
    public static <K, V> NonBlockingSegmentOperationGateway<K, V> create(
            final SegmentRegistry<K, V> segmentRegistry) {
        return new NonBlockingSegmentOperationGateway<>(segmentRegistry);
    }

    private NonBlockingSegmentOperationGateway(
            final SegmentRegistry<K, V> segmentRegistry) {
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
    }

    /**
     * Opens an iterator for a mapped stable segment without waiting on BUSY.
     *
     * @param segmentId target segment id
     * @param isolation iterator isolation mode
     * @return operation result with iterator on OK
     */
    public OperationResult<EntryIterator<K, V>> openIterator(
            final SegmentId segmentId,
            final SegmentIteratorIsolation isolation) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
        final Optional<BlockingSegment<K, V>> loaded = segmentRegistry
                .tryGetSegment(segmentId);
        return loaded.isEmpty() ? OperationResult.busy()
                : loaded.get().tryOpenIterator(isolation);
    }

    /**
     * Starts compaction for a mapped stable segment without waiting on BUSY.
     *
     * @param segmentId target segment id
     * @return operation result with accepted segment on OK
     */
    public OperationResult<BlockingSegment<K, V>> compact(
            final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
        final Optional<BlockingSegment<K, V>> loaded = segmentRegistry
                .tryGetSegment(segmentId);
        if (loaded.isEmpty()) {
            return OperationResult.busy();
        }
        final BlockingSegment<K, V> segment = loaded.get();
        return withSegmentOnOk(segment.tryCompact(), segment);
    }

    /**
     * Starts flush for a mapped stable segment without waiting on BUSY.
     *
     * @param segmentId target segment id
     * @return operation result with accepted segment on OK
     */
    public OperationResult<BlockingSegment<K, V>> flush(
            final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, SEGMENT_ID_ARG);
        final Optional<BlockingSegment<K, V>> loaded = segmentRegistry
                .tryGetSegment(segmentId);
        if (loaded.isEmpty()) {
            return OperationResult.busy();
        }
        final BlockingSegment<K, V> segment = loaded.get();
        return withSegmentOnOk(segment.tryFlush(), segment);
    }

    private static <T> OperationResult<T> withSegmentOnOk(
            final OperationResult<Void> result,
            final T segment) {
        if (result.isOk()) {
            return OperationResult.ok(segment);
        }
        return OperationResult.fromStatus(result.getStatus());
    }
}
