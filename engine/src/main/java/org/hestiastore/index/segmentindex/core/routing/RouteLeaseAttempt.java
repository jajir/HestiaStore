package org.hestiastore.index.segmentindex.core.routing;

import org.hestiastore.index.Vldtn;

final class RouteLeaseAttempt
        implements RouteTopology.RouteLeaseResult {

    private final RouteTopology.RouteLease lease;
    private final RouteLeaseStatus status;

    private RouteLeaseAttempt(final RouteTopology.RouteLease lease,
            final RouteLeaseStatus status) {
        this.lease = lease;
        this.status = Vldtn.requireNonNull(status, "status");
    }

    static RouteTopology.RouteLeaseResult acquired(
            final RouteTopology.RouteLease lease) {
        return new RouteLeaseAttempt(
                Vldtn.requireNonNull(lease, "lease"),
                RouteLeaseStatus.ACQUIRED);
    }

    static RouteTopology.RouteLeaseResult staleTopology() {
        return new RouteLeaseAttempt(null,
                RouteLeaseStatus.STALE_TOPOLOGY);
    }

    static RouteTopology.RouteLeaseResult routeUnavailable() {
        return new RouteLeaseAttempt(null,
                RouteLeaseStatus.ROUTE_UNAVAILABLE);
    }

    @Override
    public boolean isAcquired() {
        return status == RouteLeaseStatus.ACQUIRED;
    }

    @Override
    public boolean isStaleTopology() {
        return status == RouteLeaseStatus.STALE_TOPOLOGY;
    }

    @Override
    public boolean isRouteUnavailable() {
        return status == RouteLeaseStatus.ROUTE_UNAVAILABLE;
    }

    @Override
    public RouteTopology.RouteLease lease() {
        if (!isAcquired()) {
            throw new IllegalStateException(String.format(
                    "Route lease is not available for status '%s'.", status));
        }
        return lease;
    }
}
