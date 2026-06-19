package org.hestiastore.index.segmentindex.core.topology;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.mapping.Snapshot;

/**
 * Runtime route topology that coordinates route leases, drains, and
 * reconciliation with route-map snapshots.
 *
 * @param <K> key type
 */
public final class SegmentTopology<K> {

    private static final String OPERATION_DRAIN = "drainRouteLeases";

    private final Object monitor = new Object();
    private final Map<SegmentId, RouteEntry> routes = new HashMap<>();
    private final BusyRetryPolicy retryPolicy;
    private long version;

    SegmentTopology(final Snapshot<K> snapshot,
            final BusyRetryPolicy retryPolicy) {
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
        reconcile(snapshot);
    }

    /**
     * Creates a builder for a segment topology.
     *
     * @param <M> key type
     * @return segment topology builder
     */
    public static <M> SegmentTopologyBuilder<M> builder() {
        return new SegmentTopologyBuilder<>();
    }

    /**
     * Attempts to acquire a foreground operation lease for an active route.
     *
     * @param segmentId routed segment id
     * @param expectedVersion route-map version the caller resolved
     * @return route lease acquisition result
     */
    public RouteLeaseResult tryAcquire(final SegmentId segmentId,
            final long expectedVersion) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        synchronized (monitor) {
            if (version != expectedVersion) {
                return DefaultRouteLeaseResult.staleTopology();
            }
            final RouteEntry entry = routes.get(segmentId);
            if (entry == null || !entry.isActive()) {
                return DefaultRouteLeaseResult.routeUnavailable();
            }
            entry.acquireLease();
            return DefaultRouteLeaseResult
                    .acquired(new DefaultRouteLease(this, segmentId));
        }
    }

    /**
     * Attempts to move a route into drain state before split materialization.
     *
     * @param segmentId routed segment id
     * @return route drain acquisition result
     */
    public RouteDrainResult tryBeginDrain(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        synchronized (monitor) {
            final RouteEntry entry = routes.get(segmentId);
            if (entry == null || !entry.isActive()) {
                return DefaultRouteDrainResult.routeUnavailable();
            }
            entry.markDraining();
            monitor.notifyAll();
            return DefaultRouteDrainResult
                    .acquired(new DefaultRouteDrain(this, segmentId));
        }
    }

    /**
     * Waits until all currently acquired route leases are returned.
     */
    public void drain() {
        final long startNanos = retryPolicy.startNanos();
        while (hasInFlightLeasesSnapshot()) {
            retryPolicy.backoffOrThrow(startNanos, OPERATION_DRAIN, null);
        }
    }

    /**
     * Reconciles runtime routes with the persisted route-map snapshot.
     *
     * @param snapshot route-map snapshot
     */
    public void reconcile(final Snapshot<K> snapshot) {
        final Snapshot<K> nonNullSnapshot = Vldtn.requireNonNull(snapshot,
                "snapshot");
        final List<SegmentId> activeSegmentIds = nonNullSnapshot
                .getSegmentIds(SegmentWindow.unbounded());
        synchronized (monitor) {
            if (nonNullSnapshot.version() < version) {
                return;
            }
            final Set<SegmentId> activeIds = new HashSet<>(activeSegmentIds);
            activeIds.forEach(this::ensureActiveRoute);
            retireRoutesMissingFrom(activeIds);
            version = nonNullSnapshot.version();
            monitor.notifyAll();
        }
    }

    /**
     * Returns the runtime topology version.
     *
     * @return topology version
     */
    public long version() {
        synchronized (monitor) {
            return version;
        }
    }

    void releaseLease(final SegmentId segmentId) {
        synchronized (monitor) {
            final RouteEntry entry = routes.get(segmentId);
            if (entry == null) {
                return;
            }
            entry.releaseLease(segmentId);
            if (!entry.hasInFlight() && entry.isRetired()) {
                routes.remove(segmentId);
            }
            monitor.notifyAll();
        }
    }

    void awaitDrained(final SegmentId segmentId) {
        synchronized (monitor) {
            while (inFlightCount(segmentId) > 0) {
                try {
                    monitor.wait();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IndexException(String.format(
                            "Interrupted while waiting for route '%s' to drain.",
                            segmentId),
                            e);
                }
            }
        }
    }

    void abortDrain(final SegmentId segmentId) {
        synchronized (monitor) {
            final RouteEntry entry = routes.get(segmentId);
            if (entry != null && entry.state() == RouteState.DRAINING) {
                entry.markActive();
            }
            monitor.notifyAll();
        }
    }

    private void ensureActiveRoute(final SegmentId segmentId) {
        final RouteEntry entry = routes.get(segmentId);
        if (entry == null || entry.isRetired()) {
            routes.put(segmentId, new RouteEntry(RouteState.ACTIVE));
        }
    }

    private void retireRoutesMissingFrom(final Set<SegmentId> activeIds) {
        final Iterator<Map.Entry<SegmentId, RouteEntry>> iterator = routes
                .entrySet().iterator();
        while (iterator.hasNext()) {
            final Map.Entry<SegmentId, RouteEntry> route = iterator.next();
            if (activeIds.contains(route.getKey())) {
                continue;
            }
            final RouteEntry entry = route.getValue();
            if (!entry.hasInFlight()) {
                iterator.remove();
            } else {
                entry.markRetired();
            }
        }
    }

    private long inFlightCount(final SegmentId segmentId) {
        final RouteEntry entry = routes.get(segmentId);
        return entry == null ? 0L : entry.inFlight();
    }

    private boolean hasInFlightLeases() {
        return routes.values().stream().anyMatch(RouteEntry::hasInFlight);
    }

    private boolean hasInFlightLeasesSnapshot() {
        synchronized (monitor) {
            return hasInFlightLeases();
        }
    }

    /**
     * Lease held by a foreground operation against one active route.
     */
    public interface RouteLease extends AutoCloseable {

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
    public interface RouteLeaseResult {

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
    public interface RouteDrain {

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
    public interface RouteDrainResult {

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
