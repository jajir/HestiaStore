package org.hestiastore.index.segmentindex.core.routing;

/**
 * Runtime state of a route entry in {@link RouteTopology}.
 */
enum RouteState {

    /** Route accepts new foreground operation leases. */
    ACTIVE,

    /** Route refuses new leases and waits for in-flight operations to finish. */
    DRAINING,

    /** Route has been replaced by another topology entry. */
    RETIRED
}
