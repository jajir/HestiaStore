package org.hestiastore.index.segmentindex.core.routing;

import java.util.concurrent.atomic.AtomicBoolean;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;

/**
 * Drain handle bound to the exact route entry moved out of active state.
 */
final class RouteDrainHandle implements RouteTopology.RouteDrain {

    private final RouteEntry entry;
    private final SegmentId segmentId;
    private final AtomicBoolean completed = new AtomicBoolean();

    /**
     * Creates a drain bound to its exact route entry.
     *
     * @param entry draining route entry
     * @param segmentId draining segment id
     */
    RouteDrainHandle(final RouteEntry entry,
            final SegmentId segmentId) {
        this.entry = Vldtn.requireNonNull(entry, "entry");
        this.segmentId = Vldtn.requireNonNull(segmentId, "segmentId");
    }

    @Override
    public void awaitDrained() {
        entry.awaitDrained(segmentId);
    }

    @Override
    public void abort() {
        if (completed.compareAndSet(false, true)) {
            entry.markActive();
        }
    }

    @Override
    public void complete() {
        completed.set(true);
    }
}
