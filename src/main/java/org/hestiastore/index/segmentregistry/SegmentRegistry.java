package org.hestiastore.index.segmentregistry;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;

/**
 * Minimal contract for retrieving and managing segments from a registry.
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
     * Allocates a new, unused segment id.
     *
     * @return registry result containing the new segment id or a status
     */
    SegmentRegistryResult<SegmentId> allocateSegmentId();

    /**
     * Creates and registers a new segment using a freshly allocated id.
     *
     * @return registry result containing the new segment or a status
     */
    default SegmentRegistryResult<Segment<K, V>> createSegment() {
        final SegmentRegistryResult<SegmentId> idResult = allocateSegmentId();
        if (idResult.getStatus() == SegmentRegistryResultStatus.OK) {
            return getSegment(idResult.getValue());
        }
        if (idResult.getStatus() == SegmentRegistryResultStatus.CLOSED) {
            return SegmentRegistryResult.closed();
        }
        if (idResult.getStatus() == SegmentRegistryResultStatus.ERROR) {
            return SegmentRegistryResult.error();
        }
        return SegmentRegistryResult.busy();
    }

    /**
     * Removes a segment from the registry, closing and deleting its files.
     *
     * @param segmentId segment id to remove
     * @return registry result status
     */
    SegmentRegistryResult<Void> deleteSegment(SegmentId segmentId);

    /**
     * Closes the registry, releasing cached segments and executors.
     *
     * @return registry result status
     */
    SegmentRegistryResult<Void> close();
}
