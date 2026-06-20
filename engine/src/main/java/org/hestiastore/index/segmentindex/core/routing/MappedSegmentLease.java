package org.hestiastore.index.segmentindex.core.routing;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.core.routing.RouteTopology.RouteLease;
import org.hestiastore.index.segmentregistry.BlockingSegment;

/**
 * Scoped lease for the segment currently owning one routed key.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class MappedSegmentLease<K, V> implements AutoCloseable {

    private final RouteLease routeLease;
    private final BlockingSegment<K, V> segment;

    MappedSegmentLease(final RouteLease routeLease,
            final BlockingSegment<K, V> segment) {
        this.routeLease = Vldtn.requireNonNull(routeLease, "routeLease");
        this.segment = Vldtn.requireNonNull(segment, "segment");
    }

    /**
     * Returns the acquired segment id.
     *
     * @return segment id
     */
    public SegmentId segmentId() {
        return routeLease.segmentId();
    }

    /**
     * Returns the loaded blocking segment.
     *
     * @return blocking segment
     */
    public BlockingSegment<K, V> segment() {
        return segment;
    }

    /**
     * Releases the route lease.
     */
    @Override
    public void close() {
        routeLease.close();
    }
}
