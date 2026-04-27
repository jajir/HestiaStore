package org.hestiastore.index.segmentindex.core.topology;

import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.mapping.Snapshot;

/**
 * Runtime route topology used to lease routed operations and drain routes
 * before split materialization.
 *
 * @param <K> key type
 */
public interface SegmentTopology<K> {

    /**
     * Creates a builder for a segment topology.
     *
     * @param <M> key type
     * @return segment topology builder
     */
    static <M> SegmentTopologyBuilder<M> builder() {
        return new SegmentTopologyBuilder<>();
    }

    /**
     * Attempts to acquire a foreground operation lease for an active route.
     *
     * @param segmentId routed segment id
     * @param expectedVersion route-map version the caller resolved
     * @return route lease acquisition result
     */
    RouteLeaseResult tryAcquire(SegmentId segmentId, long expectedVersion);

    /**
     * Attempts to move a route into drain state before split materialization.
     *
     * @param segmentId routed segment id
     * @return route drain acquisition result
     */
    RouteDrainResult tryBeginDrain(SegmentId segmentId);

    /**
     * Reconciles runtime routes with the persisted route-map snapshot.
     *
     * @param snapshot route-map snapshot
     */
    void reconcile(Snapshot<K> snapshot);

    /**
     * Returns the runtime topology version.
     *
     * @return topology version
     */
    long version();

    /**
     * Lease held by a foreground operation against one active route.
     */
    interface RouteLease extends AutoCloseable {

        /**
         * Returns the leased segment id.
         *
         * @return leased segment id
         */
        SegmentId segmentId();

        @Override
        void close();
    }

    /**
     * Lease acquisition result.
     */
    interface RouteLeaseResult {

        /**
         * Returns whether the route lease was acquired.
         *
         * @return true when a lease is available
         */
        boolean isAcquired();

        /**
         * Returns whether the caller resolved a newer map version than the
         * topology currently holds.
         *
         * @return true when the topology is stale
         */
        boolean isStaleTopology();

        /**
         * Returns whether the requested route is unavailable.
         *
         * @return true when no lease is available for the route
         */
        boolean isRouteUnavailable();

        /**
         * Returns the acquired lease.
         *
         * @return route lease
         */
        RouteLease lease();
    }

    /**
     * Handle for a route refusing new leases while existing leases drain.
     */
    interface RouteDrain {

        /**
         * Blocks until in-flight leases for the route complete.
         */
        void awaitDrained();

        /**
         * Returns the route to active state when split materialization fails.
         */
        void abort();

        /**
         * Marks the drain as completed after publish or retirement.
         */
        void complete();
    }

    /**
     * Route drain acquisition result.
     */
    interface RouteDrainResult {

        /**
         * Returns whether the route drain was acquired.
         *
         * @return true when a drain handle is available
         */
        boolean isAcquired();

        /**
         * Returns the acquired drain handle.
         *
         * @return route drain handle
         */
        RouteDrain drain();
    }
}
