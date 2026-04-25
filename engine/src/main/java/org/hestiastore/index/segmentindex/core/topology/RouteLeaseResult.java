package org.hestiastore.index.segmentindex.core.topology;

import org.hestiastore.index.Vldtn;

/**
 * Lease acquisition result.
 */
public final class RouteLeaseResult {

    private final RouteLease lease;
    private final RouteLeaseStatus status;

    private RouteLeaseResult(final RouteLease lease,
            final RouteLeaseStatus status) {
        this.lease = lease;
        this.status = Vldtn.requireNonNull(status, "status");
    }

    static RouteLeaseResult acquired(final RouteLease lease) {
        return new RouteLeaseResult(Vldtn.requireNonNull(lease, "lease"),
                RouteLeaseStatus.ACQUIRED);
    }

    static RouteLeaseResult staleTopology() {
        return new RouteLeaseResult(null, RouteLeaseStatus.STALE_TOPOLOGY);
    }

    static RouteLeaseResult routeUnavailable() {
        return new RouteLeaseResult(null, RouteLeaseStatus.ROUTE_UNAVAILABLE);
    }

    public boolean isAcquired() {
        return status == RouteLeaseStatus.ACQUIRED;
    }

    public RouteLease lease() {
        if (!isAcquired()) {
            throw new IllegalStateException(String.format(
                    "Route lease is not available for status '%s'.", status));
        }
        return lease;
    }

    public RouteLeaseStatus status() {
        return status;
    }
}
