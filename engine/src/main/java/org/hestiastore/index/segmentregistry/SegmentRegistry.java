package org.hestiastore.index.segmentregistry;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;

/**
 * Minimal contract for retrieving and managing segments from a registry.
 * <p>
 * Contract source of truth is {@code docs/architecture/registry.md}.
 * Operations return status and optional value through
 * {@link SegmentRegistryResult}.
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
     * <p>
     * In {@code READY} state this operation uses per-key synchronization.
     * Unrelated keys must not block each other.
     *
     * @param segmentId segment id to load
     * @return result with status and optional segment
     */
    SegmentRegistryResult<Segment<K, V>> getSegment(SegmentId segmentId);

    /**
     * Allocates a new, unused segment id.
     *
     * @return result with status and optional segment id
     */
    SegmentRegistryResult<SegmentId> allocateSegmentId();

    /**
     * Creates and registers a new segment using a freshly allocated id.
     *
     * @return result with status and optional created segment
     */
    default SegmentRegistryResult<Segment<K, V>> createSegment() {
        final SegmentRegistryResult<SegmentId> allocated = allocateSegmentId();
        if (allocated.getStatus() != SegmentRegistryResultStatus.OK) {
            return SegmentRegistryResult.fromStatus(allocated.getStatus());
        }
        if (allocated.getValue() == null) {
            return SegmentRegistryResult.error();
        }
        return getSegment(allocated.getValue());
    }

    /**
     * Removes a segment from the registry, closing and deleting its files.
     *
     * @param segmentId segment id to remove
     * @return result status
     */
    SegmentRegistryResult<Void> deleteSegment(SegmentId segmentId);

    /**
     * Returns immutable cache counters snapshot.
     *
     * @return registry cache metrics snapshot
     */
    default SegmentRegistryCacheStats metricsSnapshot() {
        return SegmentRegistryCacheStats.empty();
    }

    /**
     * Closes the registry, releasing cached segments and executors.
     * <p>
     * Close is idempotent and maps the gate to {@code CLOSED} unless the gate is
     * already terminal {@code ERROR}.
     *
     * @return result status
     */
    SegmentRegistryResult<Void> close();
}
