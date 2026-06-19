package org.hestiastore.index.segmentindex.core.segmentlease;

import java.util.List;
import java.util.Optional;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology.RouteDrain;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology.RouteDrainResult;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology.RouteLease;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology.RouteLeaseResult;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.Snapshot;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Provides scoped blocking leases for mapped segments and coordinates route
 * leases with segment registry handles.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentLeaseService<K, V> {

    private static final String OPERATION_READ = "acquireForRead";
    private static final String OPERATION_WRITE = "acquireForWrite";

    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final SegmentTopology<K> segmentTopology;
    private final BusyRetryPolicy retryPolicy;

    SegmentLeaseService(
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final SegmentTopology<K> segmentTopology,
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
     * Creates a builder for segment lease services.
     *
     * @param <M> key type
     * @param <N> value type
     * @return segment lease service builder
     */
    public static <M, N> SegmentLeaseServiceBuilder<M, N> builder() {
        return new SegmentLeaseServiceBuilder<>();
    }

    /**
     * Acquires a read lease for the segment mapped to the provided key.
     *
     * @param key key used to find the segment
     * @return segment lease, or {@code null} when no segment maps the key
     */
    public SegmentLease<K, V> acquireForRead(final K key) {
        final K nonNullKey = requireKey(key);
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
    public SegmentLease<K, V> acquireForWrite(final K key) {
        final K nonNullKey = requireKey(key);
        return loadSegmentLease(acquireWriteRouteLease(nonNullKey));
    }

    /**
     * Waits until currently acquired segment leases are returned.
     */
    public void drain() {
        segmentTopology.drain();
    }

    /**
     * Attempts to acquire a foreground lease for the exact mapped segment id.
     *
     * @param segmentId segment id to load
     * @return loaded segment lease when the route and segment are immediately
     *         available
     */
    public Optional<SegmentLease<K, V>> tryAcquireMappedSegment(
            final SegmentId segmentId) {
        final SegmentId nonNullSegmentId = requireSegmentId(segmentId);
        final Snapshot<K> snapshot = currentRouteSnapshot();
        if (!isRoutedSegment(snapshot, nonNullSegmentId)) {
            return Optional.empty();
        }
        final RouteLease lease = tryAcquireRouteLease(nonNullSegmentId,
                snapshot);
        if (lease == null) {
            return Optional.empty();
        }
        return loadOptionalSegmentLease(nonNullSegmentId, lease);
    }

    /**
     * Returns a best-effort snapshot of loaded segment ids that are also
     * present in the current route map.
     *
     * @return loaded mapped segment ids
     */
    public List<SegmentId> getLoadedMappedSegmentIds() {
        final Snapshot<K> snapshot = currentRouteSnapshot();
        final List<SegmentId> routedSegmentIds = snapshot
                .getSegmentIds(SegmentWindow.unbounded());
        if (routedSegmentIds.isEmpty()) {
            return List.of();
        }
        return segmentRegistry.runtime().loadedSegmentsSnapshot().stream()
                .map(BlockingSegment::getId)
                .filter(routedSegmentIds::contains).distinct().toList();
    }

    /**
     * Attempts to acquire a foreground lease for an already-loaded mapped
     * segment id.
     *
     * @param segmentId segment id to acquire
     * @return loaded segment lease when the route and loaded segment are
     *         immediately available
     */
    public Optional<SegmentLease<K, V>> tryAcquireLoadedMappedSegment(
            final SegmentId segmentId) {
        final SegmentId nonNullSegmentId = requireSegmentId(segmentId);
        final Snapshot<K> snapshot = currentRouteSnapshot();
        if (!isRoutedSegment(snapshot, nonNullSegmentId)) {
            return Optional.empty();
        }
        final RouteLease lease = tryAcquireRouteLease(nonNullSegmentId,
                snapshot);
        if (lease == null) {
            return Optional.empty();
        }
        return loadOptionalLoadedSegmentLease(nonNullSegmentId, lease);
    }

    /**
     * Attempts to acquire an exclusive split lease for the exact mapped segment
     * id.
     *
     * @param segmentId segment id to drain and split
     * @return split lease when the route drain and segment are immediately
     *         available
     */
    public Optional<SegmentSplitLease<K, V>> tryAcquireForSplit(
            final SegmentId segmentId) {
        final SegmentId nonNullSegmentId = requireSegmentId(segmentId);
        final RouteDrainResult drainResult = segmentTopology.tryBeginDrain(
                nonNullSegmentId);
        if (!drainResult.isAcquired()) {
            return Optional.empty();
        }
        final RouteDrain drain = drainResult.drain();
        try {
            drain.awaitDrained();
        } catch (final RuntimeException e) {
            abortDrain(drain);
            throw e;
        }
        return loadSplitLease(nonNullSegmentId, drain);
    }

    private SegmentLease<K, V> loadSegmentLease(final RouteLease lease) {
        try {
            return new SegmentLease<>(lease,
                    segmentRegistry.loadSegment(lease.segmentId()));
        } catch (final RuntimeException e) {
            lease.close();
            throw e;
        }
    }

    private Optional<SegmentLease<K, V>> loadOptionalSegmentLease(
            final SegmentId segmentId,
            final RouteLease lease) {
        try {
            final Optional<BlockingSegment<K, V>> segment =
                    segmentRegistry.tryGetSegment(segmentId);
            if (segment.isEmpty()) {
                lease.close();
                return Optional.empty();
            }
            return Optional.of(new SegmentLease<>(lease, segment.get()));
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

    private Optional<SegmentLease<K, V>> loadOptionalLoadedSegmentLease(
            final SegmentId segmentId,
            final RouteLease lease) {
        try {
            final Optional<BlockingSegment<K, V>> segment =
                    segmentRegistry.tryGetLoadedSegment(segmentId);
            if (segment.isEmpty()) {
                lease.close();
                return Optional.empty();
            }
            return Optional.of(new SegmentLease<>(lease, segment.get()));
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

    private Optional<SegmentSplitLease<K, V>> loadSplitLease(
            final SegmentId segmentId,
            final RouteDrain drain) {
        if (!isCurrentlyMapped(segmentId)) {
            completeDrain(drain);
            return Optional.empty();
        }
        try {
            final Optional<BlockingSegment<K, V>> segment =
                    segmentRegistry.tryGetSegment(segmentId);
            if (segment.isEmpty()) {
                finishUnavailableSplitDrain(segmentId, drain);
                return Optional.empty();
            }
            return Optional.of(new SegmentSplitLease<>(drain,
                    keyToSegmentMap, segmentTopology, segment.get()));
        } catch (final IndexException e) {
            finishUnavailableSplitDrain(segmentId, drain);
            if (!isCurrentlyMapped(segmentId)) {
                return Optional.empty();
            }
            throw e;
        } catch (final RuntimeException e) {
            abortDrain(drain);
            throw e;
        }
    }

    private void finishUnavailableSplitDrain(final SegmentId segmentId,
            final RouteDrain drain) {
        if (isCurrentlyMapped(segmentId)) {
            abortDrain(drain);
        } else {
            completeDrain(drain);
        }
    }

    private void abortDrain(final RouteDrain drain) {
        drain.abort();
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
            final Snapshot<K> snapshot = currentRouteSnapshot();
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
            final Snapshot<K> snapshot = currentRouteSnapshot();
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
        if (!keyToSegmentMap.extendMaxKeyIfNeeded(key)) {
            return null;
        }
        final Snapshot<K> bootstrappedSnapshot = currentRouteSnapshot();
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
            final Snapshot<K> snapshot) {
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

    private K requireKey(final K key) {
        return Vldtn.requireNonNull(key, "key");
    }

    private SegmentId requireSegmentId(final SegmentId segmentId) {
        return Vldtn.requireNonNull(segmentId, "segmentId");
    }

    private Snapshot<K> currentRouteSnapshot() {
        return keyToSegmentMap.snapshot();
    }

    private boolean isCurrentlyMapped(final SegmentId segmentId) {
        return isRoutedSegment(currentRouteSnapshot(), segmentId);
    }

    private boolean isRoutedSegment(final Snapshot<K> snapshot,
            final SegmentId segmentId) {
        return snapshot.getSegmentIds(SegmentWindow.unbounded())
                .contains(segmentId);
    }
}
