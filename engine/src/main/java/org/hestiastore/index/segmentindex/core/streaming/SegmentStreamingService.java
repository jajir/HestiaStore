package org.hestiastore.index.segmentindex.core.streaming;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;

/**
 * Provides stable segment streaming operations.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SegmentStreamingService<K, V> {

    /**
     * Creates a builder for segment streaming services.
     *
     * @param <M> key type
     * @param <N> value type
     * @return segment streaming service builder
     */
    static <M, N> SegmentStreamingServiceBuilder<M, N> builder() {
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
    EntryIterator<K, V> openIterator(SegmentId segmentId,
            SegmentIteratorIsolation isolation);

    /**
     * Invalidates iterators for currently mapped loaded stable segments.
     */
    void invalidateIterators();
}
