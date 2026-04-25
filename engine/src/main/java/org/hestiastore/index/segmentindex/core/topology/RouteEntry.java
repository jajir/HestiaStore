package org.hestiastore.index.segmentindex.core.topology;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;

/**
 * Mutable runtime state for one routed segment entry.
 */
final class RouteEntry {

    private RouteState state;
    private long inFlight;

    RouteEntry(final RouteState state) {
        this.state = Vldtn.requireNonNull(state, "state");
    }

    RouteState state() {
        return state;
    }

    boolean isActive() {
        return state == RouteState.ACTIVE;
    }

    boolean isRetired() {
        return state == RouteState.RETIRED;
    }

    boolean hasInFlight() {
        return inFlight > 0L;
    }

    long inFlight() {
        return inFlight;
    }

    void acquireLease() {
        inFlight++;
    }

    void releaseLease(final SegmentId segmentId) {
        if (inFlight <= 0L) {
            throw new IllegalStateException(String.format(
                    "Route '%s' lease count is already zero.", segmentId));
        }
        inFlight--;
    }

    void markDraining() {
        state = RouteState.DRAINING;
    }

    void markActive() {
        state = RouteState.ACTIVE;
    }

    void markRetired() {
        state = RouteState.RETIRED;
    }
}
