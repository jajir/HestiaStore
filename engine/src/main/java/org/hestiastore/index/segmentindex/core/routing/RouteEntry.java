package org.hestiastore.index.segmentindex.core.routing;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;

/**
 * Mutable runtime state for one routed segment entry.
 */
final class RouteEntry {

    private final Object monitor = new Object();
    private RouteState state;
    private long inFlight;

    RouteEntry(final RouteState state) {
        this.state = Vldtn.requireNonNull(state, "state");
    }

    /**
     * Returns the current lifecycle state guarded with the lease count.
     *
     * @return route lifecycle state
     */
    RouteState state() {
        synchronized (monitor) {
            return state;
        }
    }

    /**
     * Returns whether this route currently accepts leases.
     *
     * @return true when the route is active
     */
    boolean isActive() {
        synchronized (monitor) {
            return state == RouteState.ACTIVE;
        }
    }

    /**
     * Returns whether this route has left the published topology.
     *
     * @return true when the route is retired
     */
    boolean isRetired() {
        synchronized (monitor) {
            return state == RouteState.RETIRED;
        }
    }

    /**
     * Returns whether at least one acquired lease remains open.
     *
     * @return true when a lease remains open
     */
    boolean hasInFlight() {
        synchronized (monitor) {
            return inFlight > 0L;
        }
    }

    /**
     * Returns the exact number of acquired leases.
     *
     * @return acquired lease count
     */
    long inFlight() {
        synchronized (monitor) {
            return inFlight;
        }
    }

    /**
     * Atomically acquires a lease only while the route remains active.
     *
     * @return true when the lease count was incremented
     */
    boolean tryAcquireLease() {
        synchronized (monitor) {
            if (state != RouteState.ACTIVE) {
                return false;
            }
            if (inFlight == Long.MAX_VALUE) {
                throw new IllegalStateException(
                        "Route lease count reached its maximum value.");
            }
            inFlight++;
            return true;
        }
    }

    /**
     * Atomically returns one lease and reports completed retirement.
     *
     * @param segmentId segment id used in validation failures
     * @return true when the route is retired and no leases remain
     */
    boolean releaseLease(final SegmentId segmentId) {
        synchronized (monitor) {
            if (inFlight == 0L) {
                throw new IllegalStateException(String.format(
                        "Route '%s' lease count is already zero.", segmentId));
            }
            inFlight--;
            if (inFlight == 0L && state != RouteState.ACTIVE) {
                monitor.notifyAll();
            }
            return inFlight == 0L && state == RouteState.RETIRED;
        }
    }

    /**
     * Atomically refuses new leases while preserving the current lease count.
     *
     * @return true when this call started the drain
     */
    boolean tryMarkDraining() {
        synchronized (monitor) {
            if (state != RouteState.ACTIVE) {
                return false;
            }
            state = RouteState.DRAINING;
            return true;
        }
    }

    /**
     * Restores a draining route after an aborted split operation.
     */
    void markActive() {
        synchronized (monitor) {
            if (state == RouteState.DRAINING) {
                state = RouteState.ACTIVE;
            }
        }
    }

    /**
     * Prevents future leases after the route leaves the published topology.
     *
     * @return true when no leases remain at retirement time
     */
    boolean markRetired() {
        synchronized (monitor) {
            state = RouteState.RETIRED;
            if (inFlight == 0L) {
                monitor.notifyAll();
            }
            return inFlight == 0L;
        }
    }

    /**
     * Waits on this route only until its acquired leases are returned.
     *
     * @param segmentId segment id used in interruption failures
     */
    void awaitDrained(final SegmentId segmentId) {
        synchronized (monitor) {
            while (inFlight > 0L) {
                try {
                    monitor.wait();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IndexException(String.format(
                            "Interrupted while waiting for route '%s' to drain.",
                            segmentId),
                            e);
                }
            }
        }
    }
}
