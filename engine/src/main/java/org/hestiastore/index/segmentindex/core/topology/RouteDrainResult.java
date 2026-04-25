package org.hestiastore.index.segmentindex.core.topology;

import org.hestiastore.index.Vldtn;

/**
 * Route drain acquisition result.
 */
public final class RouteDrainResult {

    private final RouteDrain drain;
    private final RouteDrainStatus status;

    private RouteDrainResult(final RouteDrain drain,
            final RouteDrainStatus status) {
        this.drain = drain;
        this.status = Vldtn.requireNonNull(status, "status");
    }

    static RouteDrainResult acquired(final RouteDrain drain) {
        return new RouteDrainResult(Vldtn.requireNonNull(drain, "drain"),
                RouteDrainStatus.ACQUIRED);
    }

    static RouteDrainResult routeUnavailable() {
        return new RouteDrainResult(null, RouteDrainStatus.ROUTE_UNAVAILABLE);
    }

    public boolean isAcquired() {
        return status == RouteDrainStatus.ACQUIRED;
    }

    public RouteDrain drain() {
        if (!isAcquired()) {
            throw new IllegalStateException(String.format(
                    "Route drain is not available for status '%s'.", status));
        }
        return drain;
    }

    public RouteDrainStatus status() {
        return status;
    }
}
