package org.hestiastore.index.segmentregistry;

import java.util.Optional;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;

/**
 * Minimal contract for retrieving and managing segments from a registry.
 * <p>
 * Contract source of truth is
 * {@code docs/architecture/registry.md}. Core behavior:
 * <ul>
 * <li>Registry state gate mapping:
 * {@code READY -> normal flow}, {@code FREEZE -> BUSY},
 * {@code CLOSED -> CLOSED}, {@code ERROR -> ERROR}.</li>
 * <li>Per-key cache behavior:
 * {@code LOADING} waits on the same key only, {@code UNLOADING} is exposed as
 * {@code BUSY} to callers.</li>
 * <li>Load/open failures are exception-driven and may propagate as runtime
 * exceptions.</li>
 * </ul>
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
     * <p>
     * Close is idempotent and maps the gate to {@code CLOSED} unless the gate is
     * already terminal {@code ERROR}.
     *
     * @return registry result status
     */
    SegmentRegistryAccess<Void> close();
}
