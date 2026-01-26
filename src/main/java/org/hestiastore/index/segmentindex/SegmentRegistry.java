package org.hestiastore.index.segmentindex;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;

/**
 * Minimal contract for retrieving and removing segments from a registry.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SegmentRegistry<K, V> {

    /**
     * Returns the segment for the provided id, loading it if needed.
     *
     * @param segmentId segment id to load
     * @return registry result containing the segment or a status
     */
    SegmentRegistryResult<Segment<K, V>> getSegment(SegmentId segmentId);

    /**
     * Removes a segment from the registry and deletes its files.
     *
     * @param segmentId segment id to remove
     */
    void removeSegment(SegmentId segmentId);

    /**
     * Closes the registry, releasing cached segments and executors.
     */
    void close();
}
