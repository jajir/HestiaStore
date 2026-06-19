package org.hestiastore.index.segmentindex.core.routing;

import java.util.concurrent.atomic.AtomicBoolean;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;

final class RouteLeaseHandle implements RouteTopology.RouteLease {

    private final RouteTopology<?> topology;
    private final SegmentId segmentId;
    private final AtomicBoolean closed = new AtomicBoolean();

    RouteLeaseHandle(final RouteTopology<?> topology,
            final SegmentId segmentId) {
        this.topology = Vldtn.requireNonNull(topology, "topology");
        this.segmentId = Vldtn.requireNonNull(segmentId, "segmentId");
    }

    @Override
    public SegmentId segmentId() {
        return segmentId;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            topology.releaseLease(segmentId);
        }
    }
}
