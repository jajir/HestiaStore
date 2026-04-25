package org.hestiastore.index.segmentindex.core.topology;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.mapping.Snapshot;

/**
 * Runtime route topology used to lease routed operations and drain routes
 * before split materialization.
 *
 * @param <K> key type
 */
public final class SegmentTopology<K> {

    private final Object monitor = new Object();
    private final Map<SegmentId, RouteEntry> routes = new HashMap<>();
    private long version;

    public SegmentTopology(final Snapshot<K> snapshot) {
        reconcile(snapshot);
    }

    public static <K> SegmentTopology<K> from(final Snapshot<K> snapshot) {
        return new SegmentTopology<>(snapshot);
    }

    public RouteLeaseResult tryAcquire(final SegmentId segmentId,
            final long expectedVersion) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        synchronized (monitor) {
            if (version != expectedVersion) {
                return RouteLeaseResult.staleTopology();
            }
            final RouteEntry entry = routes.get(segmentId);
            if (entry == null || !entry.isActive()) {
                return RouteLeaseResult.routeUnavailable();
            }
            entry.acquireLease();
            return RouteLeaseResult.acquired(new RouteLease(this, segmentId));
        }
    }

    public RouteDrainResult tryBeginDrain(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        synchronized (monitor) {
            final RouteEntry entry = routes.get(segmentId);
            if (entry == null || !entry.isActive()) {
                return RouteDrainResult.routeUnavailable();
            }
            entry.markDraining();
            monitor.notifyAll();
            return RouteDrainResult.acquired(new RouteDrain(this, segmentId));
        }
    }

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

    public long version() {
        synchronized (monitor) {
            return version;
        }
    }

    public RouteState routeState(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        synchronized (monitor) {
            final RouteEntry entry = routes.get(segmentId);
            return entry == null ? RouteState.RETIRED : entry.state();
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
}
