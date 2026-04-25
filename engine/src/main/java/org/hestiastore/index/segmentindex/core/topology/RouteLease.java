package org.hestiastore.index.segmentindex.core.topology;

import java.util.concurrent.atomic.AtomicBoolean;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;

/**
 * Lease held by a foreground operation against one active route.
 */
public final class RouteLease implements AutoCloseable {

    private final SegmentTopology<?> topology;
    private final SegmentId segmentId;
    private final AtomicBoolean closed = new AtomicBoolean();

    RouteLease(final SegmentTopology<?> topology, final SegmentId segmentId) {
        this.topology = Vldtn.requireNonNull(topology, "topology");
        this.segmentId = Vldtn.requireNonNull(segmentId, "segmentId");
    }

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
