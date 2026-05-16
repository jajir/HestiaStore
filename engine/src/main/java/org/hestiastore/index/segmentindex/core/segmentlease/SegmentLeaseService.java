package org.hestiastore.index.segmentindex.core.segmentlease;

import java.util.Optional;

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
     * Attempts to acquire an exclusive split lease for the exact mapped segment
     * id.
     *
     * @param segmentId segment id to drain and split
     * @return split lease when the route drain and segment are immediately
     *         available
     */
    Optional<SegmentSplitLease<K, V>> tryAcquireForSplit(SegmentId segmentId);
}
