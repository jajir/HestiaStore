package org.hestiastore.index.segmentindex.core.topology;

import java.util.concurrent.atomic.AtomicBoolean;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;

final class DefaultRouteDrain implements SegmentTopology.RouteDrain {

    private final SegmentTopologyImpl<?> topology;
    private final SegmentId segmentId;
    private final AtomicBoolean completed = new AtomicBoolean();

    DefaultRouteDrain(final SegmentTopologyImpl<?> topology,
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
