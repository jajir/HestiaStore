package org.hestiastore.index.segmentindex.core.segmentaccess;

import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentregistry.BlockingSegment;

/**
 * Scoped access to the segment currently owning one routed key.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SegmentAccess<K, V> extends AutoCloseable {

    /**
     * Returns the acquired segment id.
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

    @Override
    void close();
}
