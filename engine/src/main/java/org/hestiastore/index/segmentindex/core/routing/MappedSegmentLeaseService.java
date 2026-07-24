package org.hestiastore.index.segmentindex.core.routing;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.routing.RouteTopology.RouteDrain;
import org.hestiastore.index.segmentindex.core.routing.RouteTopology.RouteLease;
import org.hestiastore.index.segmentindex.core.routing.RouteTopology.RouteLeaseResult;
import org.hestiastore.index.segmentindex.routemap.RouteMapSnapshot;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Provides scoped blocking leases for mapped segments and coordinates route
 * leases with segment registry handles.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class MappedSegmentLeaseService<K, V> {

    private static final String OPERATION_READ = "acquireForRead";
    private static final String OPERATION_WRITE = "acquireForWrite";
    private static final String OPERATION_MAPPED_SEGMENT = "acquireMappedSegment";
    private static final String SEGMENT_ID_ARG = "segmentId";

    private final SegmentRouteMap<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final RouteTopology<K> segmentTopology;
    private final BusyRetryPolicy retryPolicy;

    MappedSegmentLeaseService(
            final SegmentRouteMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final RouteTopology<K> segmentTopology,
            final BusyRetryPolicy retryPolicy) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.segmentTopology = Vldtn.requireNonNull(segmentTopology,
                "segmentTopology");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
    }

    /**
     * Creates a segment lease service with its retry policy.
     *
     * @param keyToSegmentMap   key-to-segment map
     * @param segmentRegistry   segment registry
     * @param segmentTopology   segment topology
     * @param busyBackoffMillis retry backoff in milliseconds
     * @param busyTimeoutMillis retry timeout in milliseconds
     * @param <M>               key type
     * @param <N>               value type
     * @return segment lease service
     */
    public static <M, N> MappedSegmentLeaseService<M, N> create(
            final SegmentRouteMap<M> keyToSegmentMap,
            final SegmentRegistry<M, N> segmentRegistry,
            final RouteTopology<M> segmentTopology,
            final int busyBackoffMillis,
            final int busyTimeoutMillis) {
        return new MappedSegmentLeaseService<>(
                Vldtn.requireNonNull(keyToSegmentMap, "keyToSegmentMap"),
                Vldtn.requireNonNull(segmentRegistry, "segmentRegistry"),
                Vldtn.requireNonNull(segmentTopology, "segmentTopology"),
                new BusyRetryPolicy(busyBackoffMillis, busyTimeoutMillis,
                        "Segment access operation"));
    }

    /**
     * Acquires a read lease for the segment mapped to the provided key.
     *
     * @param key key used to find the segment
     * @return segment lease, or {@code null} when no segment maps the key
     */
    public MappedSegmentLease<K, V> acquireForRead(final K key) {
        final K nonNullKey = Vldtn.requireNonNull(key, "key");
        final RouteLease lease = acquireReadRouteLease(nonNullKey);
        if (lease == null) {
            return null;
        }
        return loadSegmentLease(lease);
    }

    /**
     * Acquires a write lease for the segment mapped to the provided key,
     * extending the tail route when needed.
     *
     * @param key key used to find the segment
     * @return segment lease
     */
    public MappedSegmentLease<K, V> acquireForWrite(final K key) {
        final K nonNullKey = Vldtn.requireNonNull(key, "key");
        return loadSegmentLease(acquireWriteRouteLease(nonNullKey));
    }

    /**
     * Waits until currently acquired segment leases are returned.
     */
    public void drain() {
        segmentTopology.drain();
    }

    /**
     * Returns routed segment ids for the selected window.
     *
     * @param segmentWindow segment window
     * @return routed segment ids
     */
    public List<SegmentId> getSegmentIds(final SegmentWindow segmentWindow) {
        return keyToSegmentMap.getSegmentIds(
                Vldtn.requireNonNull(segmentWindow, "segmentWindow"));
    }

    /**
     * Returns a versioned snapshot of routed segment ids for the selected
     * window.
     *
     * @param segmentWindow segment window
     * @return versioned segment-window snapshot
     */
    public RouteWindowSnapshot snapshotSegmentIds(
            final SegmentWindow segmentWindow) {
        final RouteMapSnapshot<K> snapshot = keyToSegmentMap.snapshot();
        return new RouteWindowSnapshot(
                snapshot.getSegmentIds(
                        Vldtn.requireNonNull(segmentWindow, "segmentWindow")),
                snapshot.version());
    }

    /**
     * Returns whether the versioned segment-window snapshot is still current.
     *
     * @param snapshot segment-window snapshot
     * @return true when the route map is still at the snapshot version
     */
    public boolean isCurrent(final RouteWindowSnapshot snapshot) {
        return keyToSegmentMap.isAtVersion(
                Vldtn.requireNonNull(snapshot, "snapshot").version());
    }

    /**
     * Attempts to acquire a foreground lease for the exact mapped segment id.
     *
     * @param segmentId segment id to load
     * @return loaded segment lease when the route and segment are immediately
     *         available
     */
    public Optional<MappedSegmentLease<K, V>> tryAcquireMappedSegment(
            final SegmentId segmentId) {
        final SegmentId nonNullSegmentId = Vldtn.requireNonNull(segmentId,
                SEGMENT_ID_ARG);
        final RouteMapSnapshot<K> snapshot = keyToSegmentMap.snapshot();
        if (!isRoutedSegment(snapshot, nonNullSegmentId)) {
            return Optional.empty();
        }
        final RouteLease lease = tryAcquireRouteLease(nonNullSegmentId,
                snapshot);
        if (lease == null) {
            return Optional.empty();
        }
        return loadOptionalSegmentLease(nonNullSegmentId, lease, false);
    }

    /**
     * Acquires a foreground lease for the exact mapped segment id, waiting for
     * transient route drain or stale-topology states to clear.
     *
     * @param segmentId segment id to load
     * @return loaded segment lease, or null when the segment id is no longer
     *         mapped
     */
    public MappedSegmentLease<K, V> acquireMappedSegment(
            final SegmentId segmentId) {
        final SegmentId nonNullSegmentId = Vldtn.requireNonNull(segmentId,
                SEGMENT_ID_ARG);
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final RouteMapSnapshot<K> snapshot = keyToSegmentMap.snapshot();
            if (!isRoutedSegment(snapshot, nonNullSegmentId)) {
                return null;
            }
            final RouteLease lease = tryAcquireRouteLease(nonNullSegmentId,
                    snapshot);
            if (lease != null) {
                return loadSegmentLease(lease);
            }
            retryPolicy.backoffOrThrow(startNanos, OPERATION_MAPPED_SEGMENT,
                    nonNullSegmentId);
        }
    }

    /**
     * Returns a best-effort snapshot of loaded segment ids that are also
     * present in the current route map.
     *
     * @return loaded mapped segment ids
     */
    public List<SegmentId> getLoadedMappedSegmentIds() {
        final RouteMapSnapshot<K> snapshot = keyToSegmentMap.snapshot();
        final List<SegmentId> routedSegmentIds = snapshot
                .getSegmentIds(SegmentWindow.unbounded());
        if (routedSegmentIds.isEmpty()) {
            return List.of();
        }
        final Set<SegmentId> routedSegmentIdSet = Set.copyOf(routedSegmentIds);
        return segmentRegistry.runtime().loadedSegmentsSnapshot().stream()
                .map(BlockingSegment::getId)
                .filter(routedSegmentIdSet::contains).distinct().toList();
    }

    /**
     * Attempts to acquire a foreground lease for an already-loaded mapped
     * segment id.
     *
     * @param segmentId segment id to acquire
     * @return loaded segment lease when the route and loaded segment are
     *         immediately available
     */
    public Optional<MappedSegmentLease<K, V>> tryAcquireLoadedMappedSegment(
            final SegmentId segmentId) {
        final SegmentId nonNullSegmentId = Vldtn.requireNonNull(segmentId,
                SEGMENT_ID_ARG);
        final RouteMapSnapshot<K> snapshot = keyToSegmentMap.snapshot();
        if (!isRoutedSegment(snapshot, nonNullSegmentId)) {
            return Optional.empty();
        }
        final RouteLease lease = tryAcquireRouteLease(nonNullSegmentId,
                snapshot);
        if (lease == null) {
            return Optional.empty();
        }
        return loadOptionalSegmentLease(nonNullSegmentId, lease, true);
    }

    /**
     * Attempts to acquire an exclusive split lease for the exact mapped segment
     * id.
     *
     * @param segmentId segment id to drain and split
     * @return split lease when the route drain and segment are immediately
     *         available
     */
    public Optional<RouteSplitLease<K, V>> tryAcquireForSplit(
            final SegmentId segmentId) {
        final SegmentId nonNullSegmentId = Vldtn.requireNonNull(segmentId,
                SEGMENT_ID_ARG);
        final Optional<RouteDrain> drainResult = segmentTopology.tryBeginDrain(
                nonNullSegmentId);
        if (drainResult.isEmpty()) {
            return Optional.empty();
        }
        final RouteDrain drain = drainResult.get();
        try {
            drain.awaitDrained();
        } catch (final RuntimeException e) {
            drain.abort();
            throw e;
        }
        return loadSplitLease(nonNullSegmentId, drain);
    }

    private MappedSegmentLease<K, V> loadSegmentLease(final RouteLease lease) {
        try {
            return new MappedSegmentLease<>(lease,
                    segmentRegistry.loadSegment(lease.segmentId()));
        } catch (final RuntimeException e) {
            lease.close();
            throw e;
        }
    }

    private Optional<MappedSegmentLease<K, V>> loadOptionalSegmentLease(
            final SegmentId segmentId,
            final RouteLease lease, final boolean loadedOnly) {
        try {
            final Optional<BlockingSegment<K, V>> segment = loadedOnly
                    ? segmentRegistry.tryGetLoadedSegment(segmentId)
                    : segmentRegistry.tryGetSegment(segmentId);
            if (segment.isEmpty()) {
                lease.close();
                return Optional.empty();
            }
            return Optional.of(new MappedSegmentLease<>(lease, segment.get()));
        } catch (final IndexException e) {
            lease.close();
            if (!isCurrentlyMapped(segmentId)) {
                return Optional.empty();
            }
            throw e;
        } catch (final RuntimeException e) {
            lease.close();
            throw e;
        }
    }

    private Optional<RouteSplitLease<K, V>> loadSplitLease(
            final SegmentId segmentId,
            final RouteDrain drain) {
        if (!isCurrentlyMapped(segmentId)) {
            completeDrain(drain);
            return Optional.empty();
        }
        try {
            final Optional<BlockingSegment<K, V>> segment = segmentRegistry.tryGetSegment(segmentId);
            if (segment.isEmpty()) {
                finishUnavailableSplitDrain(segmentId, drain);
                return Optional.empty();
            }
            return Optional.of(new RouteSplitLease<>(drain,
                    keyToSegmentMap, segmentTopology, segment.get()));
        } catch (final IndexException e) {
            finishUnavailableSplitDrain(segmentId, drain);
            if (!isCurrentlyMapped(segmentId)) {
                return Optional.empty();
            }
            throw e;
        } catch (final RuntimeException e) {
            drain.abort();
            throw e;
        }
    }

    private void finishUnavailableSplitDrain(final SegmentId segmentId,
            final RouteDrain drain) {
        if (isCurrentlyMapped(segmentId)) {
            drain.abort();
        } else {
            completeDrain(drain);
        }
    }

    private void completeDrain(final RouteDrain drain) {
        RuntimeException reconcileFailure = null;
        try {
            segmentTopology.reconcile(keyToSegmentMap.snapshot());
        } catch (final RuntimeException e) {
            reconcileFailure = e;
        } finally {
            drain.complete();
        }
        if (reconcileFailure != null) {
            throw reconcileFailure;
        }
    }

    private RouteLease acquireReadRouteLease(final K key) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final RouteMapSnapshot<K> snapshot = keyToSegmentMap.snapshot();
            final SegmentId routedSegmentId = snapshot.findSegmentIdForKey(key);
            if (routedSegmentId == null) {
                return null;
            }
            final RouteLease lease = tryAcquireRouteLease(routedSegmentId,
                    snapshot);
            if (lease != null) {
                return lease;
            }
            retryPolicy.backoffOrThrow(startNanos, OPERATION_READ,
                    routedSegmentId);
        }
    }

    private RouteLease acquireWriteRouteLease(final K key) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final RouteMapSnapshot<K> snapshot = keyToSegmentMap.snapshot();
            final SegmentId routedSegmentId = snapshot.findSegmentIdForKey(key);
            final RouteLease lease = routedSegmentId == null
                    ? tryAcquireBootstrapRoute(key)
                    : tryAcquireRouteLease(routedSegmentId, snapshot);
            if (lease != null) {
                return lease;
            }
            retryPolicy.backoffOrThrow(startNanos, OPERATION_WRITE,
                    routedSegmentId);
        }
    }

    private RouteLease tryAcquireBootstrapRoute(final K key) {
        keyToSegmentMap.extendMaxKeyIfNeeded(key);
        final RouteMapSnapshot<K> bootstrappedSnapshot = keyToSegmentMap.snapshot();
        segmentTopology.reconcile(bootstrappedSnapshot);
        final SegmentId bootstrappedSegmentId = bootstrappedSnapshot
                .findSegmentIdForKey(key);
        if (bootstrappedSegmentId == null) {
            return null;
        }
        return tryAcquireRouteLease(bootstrappedSegmentId,
                bootstrappedSnapshot);
    }

    private RouteLease tryAcquireRouteLease(final SegmentId segmentId,
            final RouteMapSnapshot<K> snapshot) {
        final RouteLeaseResult result = segmentTopology.tryAcquire(segmentId,
                snapshot.version());
        if (result.isAcquired()) {
            return result.lease();
        }
        if (result.isStaleTopology()) {
            segmentTopology.reconcile(snapshot);
        }
        return null;
    }

    private boolean isCurrentlyMapped(final SegmentId segmentId) {
        return isRoutedSegment(keyToSegmentMap.snapshot(), segmentId);
    }

    private boolean isRoutedSegment(final RouteMapSnapshot<K> snapshot,
            final SegmentId segmentId) {
        return snapshot.containsSegmentId(segmentId);
    }
}
