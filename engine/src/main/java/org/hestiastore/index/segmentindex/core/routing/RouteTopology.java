package org.hestiastore.index.segmentindex.core.routing;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.routemap.RouteMapSnapshot;

/**
 * Runtime route topology that coordinates route leases, drains, and
 * reconciliation with route-map snapshots.
 *
 * @param <K> key type
 */
public final class RouteTopology<K> {

    private static final String OPERATION_DRAIN = "drainRouteLeases";

    private final Object reconciliationMonitor = new Object();
    private final Set<RouteEntry> pendingRetiredEntries = ConcurrentHashMap
            .newKeySet();
    private final BusyRetryPolicy retryPolicy;
    private volatile RouteView routeView = new RouteView(Map.of(), 0L);

    RouteTopology(final RouteMapSnapshot<K> snapshot,
            final BusyRetryPolicy retryPolicy) {
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
        reconcile(snapshot);
    }

    /**
     * Creates a segment topology with its route-drain retry policy.
     *
     * @param snapshot initial route-map snapshot
     * @param busyBackoffMillis retry backoff in milliseconds
     * @param busyTimeoutMillis retry timeout in milliseconds
     * @param <M> key type
     * @return segment topology
     */
    public static <M> RouteTopology<M> create(
            final RouteMapSnapshot<M> snapshot,
            final int busyBackoffMillis,
            final int busyTimeoutMillis) {
        return new RouteTopology<>(
                Vldtn.requireNonNull(snapshot, "snapshot"),
                new BusyRetryPolicy(busyBackoffMillis, busyTimeoutMillis,
                        "Route drain operation"));
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
        final RouteView currentView = routeView;
        if (currentView.version() != expectedVersion) {
            return RouteLeaseAttempt.staleTopology();
        }
        final RouteEntry entry = currentView.routes().get(segmentId);
        if (entry == null || !entry.tryAcquireLease()) {
            return RouteLeaseAttempt.routeUnavailable();
        }
        if (routeView != currentView) {
            releaseLease(entry, segmentId);
            return RouteLeaseAttempt.staleTopology();
        }
        return RouteLeaseAttempt
                .acquired(new RouteLeaseHandle(this, entry, segmentId));
    }

    /**
     * Attempts to move a route into drain state before split materialization.
     *
     * @param segmentId routed segment id
     * @return acquired route drain when available
     */
    public Optional<RouteDrain> tryBeginDrain(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        final RouteEntry entry = routeView.routes().get(segmentId);
        if (entry == null || !entry.tryMarkDraining()) {
            return Optional.empty();
        }
        return Optional.of(new RouteDrainHandle(entry, segmentId));
    }

    /**
     * Waits until all currently acquired route leases are returned.
     */
    public void drain() {
        final long startNanos = retryPolicy.startNanos();
        while (hasInFlightLeases()) {
            retryPolicy.backoffOrThrow(startNanos, OPERATION_DRAIN, null);
        }
    }

    /**
     * Reconciles runtime routes with the persisted route-map snapshot.
     *
     * @param snapshot route-map snapshot
     */
    public void reconcile(final RouteMapSnapshot<K> snapshot) {
        final RouteMapSnapshot<K> nonNullSnapshot = Vldtn.requireNonNull(snapshot,
                "snapshot");
        final List<SegmentId> activeSegmentIds = nonNullSnapshot
                .getSegmentIds(SegmentWindow.unbounded());
        synchronized (reconciliationMonitor) {
            final RouteView currentView = routeView;
            if (nonNullSnapshot.version() < currentView.version()) {
                return;
            }
            final Set<SegmentId> activeIds = new HashSet<>(activeSegmentIds);
            if (nonNullSnapshot.version() == currentView.version()
                    && currentView.routes().keySet().equals(activeIds)) {
                return;
            }
            final Map<SegmentId, RouteEntry> updatedRoutes = new HashMap<>(
                    currentView.routes());
            activeIds.forEach(
                    segmentId -> ensureActiveRoute(updatedRoutes, segmentId));
            retireRoutesMissingFrom(updatedRoutes, activeIds);
            routeView = new RouteView(Map.copyOf(updatedRoutes),
                    nonNullSnapshot.version());
        }
    }

    /**
     * Returns the runtime topology version.
     *
     * @return topology version
     */
    public long version() {
        return routeView.version();
    }

    /**
     * Releases the directly referenced route entry without another route-map
     * lookup.
     *
     * @param entry leased route entry
     * @param segmentId leased segment id
     */
    void releaseLease(final RouteEntry entry, final SegmentId segmentId) {
        if (entry.releaseLease(segmentId)) {
            pendingRetiredEntries.remove(entry);
        }
    }

    private void ensureActiveRoute(
            final Map<SegmentId, RouteEntry> updatedRoutes,
            final SegmentId segmentId) {
        final RouteEntry entry = updatedRoutes.get(segmentId);
        if (entry == null || entry.isRetired()) {
            updatedRoutes.put(segmentId, new RouteEntry(RouteState.ACTIVE));
        }
    }

    private void retireRoutesMissingFrom(
            final Map<SegmentId, RouteEntry> updatedRoutes,
            final Set<SegmentId> activeIds) {
        final Iterator<Map.Entry<SegmentId, RouteEntry>> iterator = updatedRoutes
                .entrySet().iterator();
        while (iterator.hasNext()) {
            final Map.Entry<SegmentId, RouteEntry> route = iterator.next();
            if (activeIds.contains(route.getKey())) {
                continue;
            }
            final RouteEntry entry = route.getValue();
            pendingRetiredEntries.add(entry);
            final boolean retiredWithoutLeases = entry.markRetired();
            iterator.remove();
            if (retiredWithoutLeases) {
                pendingRetiredEntries.remove(entry);
            }
        }
    }

    private boolean hasInFlightLeases() {
        return routeView.routes().values().stream()
                .anyMatch(RouteEntry::hasInFlight)
                || pendingRetiredEntries.stream()
                        .anyMatch(RouteEntry::hasInFlight);
    }

    private static final class RouteView {

        private final Map<SegmentId, RouteEntry> routes;
        private final long version;

        private RouteView(final Map<SegmentId, RouteEntry> routes,
                final long version) {
            this.routes = routes;
            this.version = version;
        }

        private Map<SegmentId, RouteEntry> routes() {
            return routes;
        }

        private long version() {
            return version;
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

}
