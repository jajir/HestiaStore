package org.hestiastore.index.segmentindex.core.routing;

import java.util.concurrent.atomic.AtomicBoolean;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;

final class RouteDrainHandle implements RouteTopology.RouteDrain {

    private final RouteTopology<?> topology;
    private final SegmentId segmentId;
    private final AtomicBoolean completed = new AtomicBoolean();

    RouteDrainHandle(final RouteTopology<?> topology,
            final SegmentId segmentId) {
        this.topology = Vldtn.requireNonNull(topology, "topology");
        this.segmentId = Vldtn.requireNonNull(segmentId, "segmentId");
    }

    @Override
    public void awaitDrained() {
        topology.awaitDrained(segmentId);
    }

    @Override
    public void abort() {
        if (completed.compareAndSet(false, true)) {
            topology.abortDrain(segmentId);
        }
    }

    @Override
    public void complete() {
        completed.set(true);
    }
}
