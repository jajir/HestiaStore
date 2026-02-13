package org.hestiastore.index.segmentregistry;

import java.util.Optional;

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
     * Creates a builder for registry instances.
     *
     * @param <M> key type
     * @param <N> value type
     * @return registry builder
     */
    static <M, N> SegmentRegistryBuilder<M, N> builder() {
        return new SegmentRegistryBuilder<>();
    }

    /**
     * Returns the segment for the provided id, loading it if needed.
     *
     * @param segmentId segment id to load
     * @return registry result containing the segment or a status
     * @throws RuntimeException when segment loading/opening fails, including
     *                          missing segment files reported by segment layer
     */
    SegmentRegistryAccess<Segment<K, V>> getSegment(SegmentId segmentId);

    /**
     * Allocates a new, unused segment id.
     *
     * @return registry result containing the new segment id or a status
     */
    SegmentRegistryAccess<SegmentId> allocateSegmentId();

    /**
     * Creates and registers a new segment using a freshly allocated id.
     *
     * @return registry result containing the new segment or a status
     * @throws RuntimeException when segment loading/opening fails while
     *                          creating the segment instance
     */
    default SegmentRegistryAccess<Segment<K, V>> createSegment() {
        final SegmentRegistryAccess<SegmentId> idResult = allocateSegmentId();
        if (idResult.getSegmentStatus() != SegmentRegistryResultStatus.OK) {
            return SegmentRegistryAccessImpl
                    .forStatus(idResult.getSegmentStatus());
        }
        final Optional<SegmentId> segmentId = idResult.getSegment();
        if (segmentId.isEmpty()) {
            return SegmentRegistryAccessImpl
                    .forStatus(SegmentRegistryResultStatus.ERROR);
        }
        return getSegment(segmentId.get());
    }

    /**
     * Removes a segment from the registry, closing and deleting its files.
     *
     * @param segmentId segment id to remove
     * @return registry result status
     */
    SegmentRegistryAccess<Void> deleteSegment(SegmentId segmentId);

    /**
     * Closes the registry, releasing cached segments and executors.
     *
     * @return registry result status
     */
    SegmentRegistryAccess<Void> close();
}
