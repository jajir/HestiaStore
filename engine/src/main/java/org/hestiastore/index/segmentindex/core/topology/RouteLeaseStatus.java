package org.hestiastore.index.segmentindex.core.topology;

/**
 * Result status for route lease acquisition.
 */
public enum RouteLeaseStatus {

    /** Lease was acquired and must be closed by the caller. */
    ACQUIRED,

    /** Caller resolved a map version that is newer than this topology view. */
    STALE_TOPOLOGY,

    /** Route is not currently available for new operations. */
    ROUTE_UNAVAILABLE
}
