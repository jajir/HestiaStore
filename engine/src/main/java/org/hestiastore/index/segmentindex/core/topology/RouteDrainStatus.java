package org.hestiastore.index.segmentindex.core.topology;

/**
 * Result status for route drain acquisition.
 */
public enum RouteDrainStatus {

    /** Drain was acquired and must be completed or aborted by the caller. */
    ACQUIRED,

    /** Route cannot be drained because it is absent or not active. */
    ROUTE_UNAVAILABLE
}
