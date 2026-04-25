package org.hestiastore.index.segmentindex.core.topology;

import java.util.concurrent.atomic.AtomicBoolean;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;

/**
 * Handle for a route that is refusing new leases while existing leases drain.
 */
public final class RouteDrain {

    private final SegmentTopology<?> topology;
    private final SegmentId segmentId;
    private final AtomicBoolean completed = new AtomicBoolean();

    RouteDrain(final SegmentTopology<?> topology, final SegmentId segmentId) {
        this.topology = Vldtn.requireNonNull(topology, "topology");
        this.segmentId = Vldtn.requireNonNull(segmentId, "segmentId");
    }

    public void awaitDrained() {
        topology.awaitDrained(segmentId);
    }

    public void abort() {
        if (completed.compareAndSet(false, true)) {
            topology.abortDrain(segmentId);
        }
    }

    public void complete() {
        completed.set(true);
    }
}
