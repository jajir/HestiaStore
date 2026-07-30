package org.hestiastore.index.segmentindex.core.routing;

import java.util.concurrent.atomic.AtomicBoolean;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;

/**
 * Idempotent lease handle that releases the exact acquired route entry.
 */
final class RouteLeaseHandle implements RouteTopology.RouteLease {

    private final RouteTopology<?> topology;
    private final RouteEntry entry;
    private final SegmentId segmentId;
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Creates a lease bound to its acquired entry.
     *
     * @param topology owning route topology
     * @param entry acquired route entry
     * @param segmentId acquired segment id
     */
    RouteLeaseHandle(final RouteTopology<?> topology,
            final RouteEntry entry, final SegmentId segmentId) {
        this.topology = Vldtn.requireNonNull(topology, "topology");
        this.entry = Vldtn.requireNonNull(entry, "entry");
        this.segmentId = Vldtn.requireNonNull(segmentId, "segmentId");
    }

    @Override
    public SegmentId segmentId() {
        return segmentId;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            topology.releaseLease(entry, segmentId);
        }
    }
}
