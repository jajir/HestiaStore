package org.hestiastore.index.segmentindex.core.segmentaccess;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology.RouteLease;
import org.hestiastore.index.segmentregistry.BlockingSegment;

final class DefaultSegmentAccess<K, V> implements SegmentAccess<K, V> {

    private final RouteLease routeLease;
    private final BlockingSegment<K, V> segment;

    DefaultSegmentAccess(final RouteLease routeLease,
            final BlockingSegment<K, V> segment) {
        this.routeLease = Vldtn.requireNonNull(routeLease, "routeLease");
        this.segment = Vldtn.requireNonNull(segment, "segment");
    }

    @Override
    public SegmentId segmentId() {
        return routeLease.segmentId();
    }

    @Override
    public BlockingSegment<K, V> segment() {
        return segment;
    }

    @Override
    public void close() {
        routeLease.close();
    }
}
