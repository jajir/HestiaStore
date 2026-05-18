package org.hestiastore.index.segmentindex.core.segmentlease;

import java.util.List;
import java.util.Optional;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;

/**
 * Provides scoped blocking leases for the segment that owns a key.
 * <p>
 * Implementations hide route-map lookup, topology lease acquisition, split
 * draining, and registry loading from callers. Callers must close returned
 * leases after using the segment.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SegmentLeaseService<K, V> {

    /**
     * Creates a builder for segment lease services.
     *
     * @param <M> key type
     * @param <N> value type
     * @return segment lease service builder
     */
    static <M, N> SegmentLeaseServiceBuilder<M, N> builder() {
        return new SegmentLeaseServiceBuilder<>();
    }

    /**
     * Acquires a read lease for the segment mapped to the provided key.
     *
     * @param key key used to find the segment
     * @return segment lease, or {@code null} when no segment maps the key
     */
    SegmentLease<K, V> acquireForRead(K key);

    /**
     * Acquires a write lease for the segment mapped to the provided key,
     * extending the tail route when needed.
     *
     * @param key key used to find the segment
     * @return segment lease
     */
    SegmentLease<K, V> acquireForWrite(K key);

    /**
     * Attempts to acquire a foreground lease for the exact mapped segment id.
     *
     * @param segmentId segment id to load
     * @return loaded segment lease when the route and segment are immediately
     *         available
     */
    Optional<SegmentLease<K, V>> tryAcquireMappedSegment(SegmentId segmentId);

    /**
     * Returns a best-effort snapshot of loaded segment ids that are also
     * present in the current route map.
     *
     * @return loaded mapped segment ids
     */
    default List<SegmentId> getLoadedMappedSegmentIds() {
        return List.of();
    }

    /**
     * Attempts to acquire a foreground lease for an already-loaded mapped
     * segment id.
     * <p>
     * This is a no-load acquisition path. It returns empty when the segment is
     * mapped but not currently loaded.
     *
     * @param segmentId segment id to acquire
     * @return loaded segment lease when the route and loaded segment are
     *         immediately available
     */
    default Optional<SegmentLease<K, V>> tryAcquireLoadedMappedSegment(
            final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        return Optional.empty();
    }

    /**
     * Attempts to acquire an exclusive split lease for the exact mapped segment
     * id.
     *
     * @param segmentId segment id to drain and split
     * @return split lease when the route drain and segment are immediately
     *         available
     */
    Optional<SegmentSplitLease<K, V>> tryAcquireForSplit(SegmentId segmentId);
}
