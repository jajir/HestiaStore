package org.hestiastore.index.segmentindex.core.segmentlease;

import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentregistry.BlockingSegment;

/**
 * Scoped exclusive lease for split work against one drained segment route.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SegmentSplitLease<K, V> extends AutoCloseable {

    /**
     * Returns the drained segment id.
     *
     * @return segment id
     */
    SegmentId segmentId();

    /**
     * Returns the loaded blocking segment.
     *
     * @return blocking segment
     */
    BlockingSegment<K, V> segment();

    /**
     * Completes the drain after the split route was published.
     */
    void completeAfterPublish();

    /**
     * Aborts the drain and returns the route to active state.
     */
    void abort();

    @Override
    void close();
}
