package org.hestiastore.index.segmentindex.core.topology;

import org.hestiastore.index.BusyRetryPolicy;

/**
 * Retry policy for route-drain waits.
 *
 * Package-private by design: callers pass retry values to
 * {@link SegmentTopologyBuilder}, and the topology package owns the concrete
 * retry semantics behind its builder entry point.
 */
final class RouteDrainRetryPolicy extends BusyRetryPolicy {

    RouteDrainRetryPolicy(final int backoffMillis, final int timeoutMillis) {
        super(backoffMillis, timeoutMillis);
    }

    @Override
    protected String formatOperationLabel() {
        return "Route drain operation";
    }
}
