package org.hestiastore.index.segmentindex.core.routing;

import java.util.List;
import java.util.function.Supplier;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.Snapshot;
import org.hestiastore.index.segmentindex.core.topology.RouteLease;
import org.hestiastore.index.segmentindex.core.topology.RouteLeaseResult;
import org.hestiastore.index.segmentindex.core.topology.RouteLeaseStatus;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Owns routed direct reads and writes against stable segments.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class DirectSegmentCoordinator<K, V> implements DirectSegmentAccess<K, V> {

    private static final String OPERATION_OPEN_FULL_ISOLATION_ITERATOR = "openFullIsolationIterator";

    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final StableSegmentAccess<K, V> stableSegmentAccess;
    private final SegmentTopology<K> segmentTopology;
    private final IndexRetryPolicy retryPolicy;

    DirectSegmentCoordinator(
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final StableSegmentAccess<K, V> stableSegmentAccess,
            final SegmentTopology<K> segmentTopology,
            final IndexRetryPolicy retryPolicy) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.stableSegmentAccess = Vldtn.requireNonNull(stableSegmentAccess,
                "stableSegmentAccess");
        this.segmentTopology = Vldtn.requireNonNull(segmentTopology,
                "segmentTopology");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
    }

    @Override
    public IndexResult<V> get(final K key) {
        final K nonNullKey = requireKey(key);
        final IndexResult<RouteLease> leaseResult = acquireReadRouteLease(
                nonNullKey);
        if (!wasRouteResolved(leaseResult)) {
            return toReadResult(leaseResult.getStatus());
        }
        try (RouteLease lease = leaseResult.getValue()) {
            return stableReadWithRetry(
                    () -> stableSegmentAccess.get(lease.segmentId(),
                            nonNullKey),
                    "get", lease.segmentId());
        }
    }

    @Override
    public EntryIterator<K, V> openWindowIterator(
            final SegmentWindow resolvedWindows,
            final SegmentIteratorIsolation isolation) {
        final SegmentWindow nonNullWindow = requireResolvedWindow(
                resolvedWindows);
        final SegmentIteratorIsolation nonNullIsolation = requireIsolation(
                isolation);
        if (isFullIsolation(nonNullIsolation)) {
            return openStableIteratorWithRouteSnapshot(nonNullWindow,
                    nonNullIsolation);
        }
        return openStableIterator(segmentIdsForWindow(nonNullWindow),
                nonNullIsolation);
    }

    @Override
    public IndexResult<Void> put(final K key, final V value) {
        final K nonNullKey = requireKey(key);
        final V nonNullValue = requireValue(value);
        return putWithResolvedRoute(nonNullKey, nonNullValue);
    }

    private IndexResult<Void> putWithResolvedRoute(final K key, final V value) {
        final IndexResult<RouteLease> routeResult = acquireWriteRouteLease(key);
        if (!wasRouteResolved(routeResult)) {
            return toVoidResult(routeResult.getStatus());
        }
        try (RouteLease lease = routeResult.getValue()) {
            return stableWriteWithRetry(
                    () -> stableSegmentAccess.put(lease.segmentId(), key,
                            value),
                    "put", lease.segmentId());
        }
    }

    private IndexResult<RouteLease> acquireReadRouteLease(final K key) {
        final Snapshot<K> snapshot = currentRouteSnapshot();
        final SegmentId routedSegmentId = snapshot.findSegmentIdForKey(key);
        if (routedSegmentId == null) {
            return IndexResult.ok(null);
        }
        return acquireRouteLease(routedSegmentId, snapshot);
    }

    private IndexResult<RouteLease> acquireWriteRouteLease(final K key) {
        Snapshot<K> snapshot = currentRouteSnapshot();
        SegmentId routedSegmentId = snapshot.findSegmentIdForKey(key);
        if (routedSegmentId == null) {
            final IndexResult<RouteLease> extendedRoute = acquireTailRoute(
                    snapshot, key);
            if (wasRouteResolved(extendedRoute)) {
                return extendedRoute;
            }
            return extendedRoute;
        }
        return acquireRouteLease(routedSegmentId, snapshot);
    }

    private IndexResult<RouteLease> acquireTailRoute(
            final Snapshot<K> snapshot, final K key) {
        final var segmentIds = snapshot.getSegmentIds(SegmentWindow.unbounded());
        if (segmentIds.isEmpty()) {
            if (!keyToSegmentMap.extendMaxKeyIfNeeded(key)) {
                return IndexResult.busy();
            }
            final Snapshot<K> bootstrappedSnapshot = currentRouteSnapshot();
            segmentTopology.reconcile(bootstrappedSnapshot);
            final SegmentId bootstrappedSegmentId = bootstrappedSnapshot
                    .findSegmentIdForKey(key);
            if (bootstrappedSegmentId == null) {
                return IndexResult.busy();
            }
            return acquireRouteLease(bootstrappedSegmentId,
                    bootstrappedSnapshot);
        }
        final SegmentId tailSegmentId = segmentIds.get(segmentIds.size() - 1);
        final IndexResult<RouteLease> leaseResult = acquireRouteLease(
                tailSegmentId, snapshot);
        if (!wasRouteResolved(leaseResult)) {
            return leaseResult;
        }
        if (!keyToSegmentMap.extendMaxKeyIfNeeded(key)) {
            leaseResult.getValue().close();
            return IndexResult.busy();
        }
        return leaseResult;
    }

    private IndexResult<RouteLease> acquireRouteLease(
            final SegmentId segmentId,
            final Snapshot<K> snapshot) {
        final RouteLeaseResult result = segmentTopology.tryAcquire(segmentId,
                snapshot.version());
        if (result.isAcquired()) {
            return IndexResult.ok(result.lease());
        }
        if (result.status() == RouteLeaseStatus.STALE_TOPOLOGY) {
            segmentTopology.reconcile(snapshot);
        }
        return IndexResult.busy();
    }

    private EntryIterator<K, V> openStableIteratorWithRouteSnapshot(
            final SegmentWindow resolvedWindows,
            final SegmentIteratorIsolation isolation) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final Snapshot<K> snapshot = currentRouteSnapshot();
            final EntryIterator<K, V> iterator = openStableIterator(
                    segmentIdsForWindow(snapshot, resolvedWindows), isolation);
            if (isSnapshotCurrent(snapshot)) {
                return iterator;
            }
            iterator.close();
            retryPolicy.backoffOrThrow(startNanos,
                    OPERATION_OPEN_FULL_ISOLATION_ITERATOR, null);
        }
    }

    private EntryIterator<K, V> openStableIterator(
            final List<SegmentId> segmentIds,
            final SegmentIteratorIsolation isolation) {
        return new SegmentsIterator<>(segmentIds, segmentRegistry, isolation);
    }

    private boolean isSnapshotCurrent(final Snapshot<K> snapshot) {
        return keyToSegmentMap.isAtVersion(snapshot.version());
    }

    private boolean wasRouteResolved(final IndexResult<RouteLease> routeResult) {
        return routeResult.getStatus() == IndexResultStatus.OK
                && routeResult.getValue() != null;
    }

    private K requireKey(final K key) {
        return Vldtn.requireNonNull(key, "key");
    }

    private V requireValue(final V value) {
        return Vldtn.requireNonNull(value, "value");
    }

    private SegmentWindow requireResolvedWindow(
            final SegmentWindow resolvedWindows) {
        return Vldtn.requireNonNull(resolvedWindows, "resolvedWindows");
    }

    private SegmentIteratorIsolation requireIsolation(
            final SegmentIteratorIsolation isolation) {
        return Vldtn.requireNonNull(isolation, "isolation");
    }

    private boolean isFullIsolation(
            final SegmentIteratorIsolation isolation) {
        return isolation == SegmentIteratorIsolation.FULL_ISOLATION;
    }

    private Snapshot<K> currentRouteSnapshot() {
        return keyToSegmentMap.snapshot();
    }

    private List<SegmentId> segmentIdsForWindow(
            final SegmentWindow resolvedWindows) {
        return keyToSegmentMap.getSegmentIds(resolvedWindows);
    }

    private List<SegmentId> segmentIdsForWindow(final Snapshot<K> snapshot,
            final SegmentWindow resolvedWindows) {
        return snapshot.getSegmentIds(resolvedWindows);
    }

    private IndexResult<V> stableReadWithRetry(
            final Supplier<IndexResult<V>> operation,
            final String operationName, final SegmentId segmentId) {
        return retryStableOperation(operation, operationName, segmentId);
    }

    private IndexResult<Void> stableWriteWithRetry(
            final Supplier<IndexResult<Void>> operation,
            final String operationName, final SegmentId segmentId) {
        return retryStableOperation(operation, operationName, segmentId);
    }

    private <T> IndexResult<T> retryStableOperation(
            final Supplier<IndexResult<T>> operation,
            final String operationName, final SegmentId segmentId) {
        final long startNanos = retryPolicy.startNanos();
        IndexResult<T> result = operation.get();
        while (result.getStatus() == IndexResultStatus.BUSY) {
            retryPolicy.backoffOrThrow(startNanos, operationName, segmentId);
            result = operation.get();
        }
        return result;
    }

    private static <T> IndexResult<T> toReadResult(
            final IndexResultStatus status) {
        if (status == IndexResultStatus.BUSY) {
            return IndexResult.busy();
        }
        if (status == IndexResultStatus.CLOSED) {
            return IndexResult.closed();
        }
        if (status == IndexResultStatus.OK) {
            return IndexResult.ok();
        }
        return IndexResult.error();
    }

    private static IndexResult<Void> toVoidResult(
            final IndexResultStatus status) {
        if (status == IndexResultStatus.BUSY) {
            return IndexResult.busy();
        }
        if (status == IndexResultStatus.CLOSED) {
            return IndexResult.closed();
        }
        if (status == IndexResultStatus.OK) {
            return IndexResult.ok();
        }
        return IndexResult.error();
    }
}
