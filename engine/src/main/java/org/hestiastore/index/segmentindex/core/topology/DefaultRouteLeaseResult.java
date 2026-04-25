package org.hestiastore.index.segmentindex.core.topology;

import org.hestiastore.index.Vldtn;

final class DefaultRouteLeaseResult
        implements SegmentTopology.RouteLeaseResult {

    private final SegmentTopology.RouteLease lease;
    private final RouteLeaseStatus status;

    private DefaultRouteLeaseResult(final SegmentTopology.RouteLease lease,
            final RouteLeaseStatus status) {
        this.lease = lease;
        this.status = Vldtn.requireNonNull(status, "status");
    }

    static SegmentTopology.RouteLeaseResult acquired(
            final SegmentTopology.RouteLease lease) {
        return new DefaultRouteLeaseResult(
                Vldtn.requireNonNull(lease, "lease"),
                RouteLeaseStatus.ACQUIRED);
    }

    static SegmentTopology.RouteLeaseResult staleTopology() {
        return new DefaultRouteLeaseResult(null,
                RouteLeaseStatus.STALE_TOPOLOGY);
    }

    static SegmentTopology.RouteLeaseResult routeUnavailable() {
        return new DefaultRouteLeaseResult(null,
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
    public SegmentTopology.RouteLease lease() {
        if (!isAcquired()) {
            throw new IllegalStateException(String.format(
                    "Route lease is not available for status '%s'.", status));
        }
        return lease;
    }
}
