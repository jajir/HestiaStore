package org.hestiastore.index.segmentindex.core.routing;

import org.hestiastore.index.OperationStatus;
import org.hestiastore.index.OperationResult;
import java.util.List;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.Snapshot;
import org.hestiastore.index.segmentindex.core.routing.BackgroundSplitCoordinator;
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
    private final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator;
    private final IndexRetryPolicy retryPolicy;

    DirectSegmentCoordinator(
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final StableSegmentAccess<K, V> stableSegmentAccess,
            final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator,
            final IndexRetryPolicy retryPolicy) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.stableSegmentAccess = Vldtn.requireNonNull(stableSegmentAccess,
                "stableSegmentAccess");
        this.backgroundSplitCoordinator = Vldtn.requireNonNull(
                backgroundSplitCoordinator, "backgroundSplitCoordinator");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
    }

    @Override
    public OperationResult<V> get(final K key) {
        final K nonNullKey = requireKey(key);
        return backgroundSplitCoordinator
                .runWithSharedSplitAdmission(
                        () -> stableSegmentAccess.get(nonNullKey));
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
            return backgroundSplitCoordinator.runWithSharedSplitAdmission(
                    () -> openStableIteratorWithRouteSnapshot(nonNullWindow,
                            nonNullIsolation));
        }
        return openStableIterator(segmentIdsForWindow(nonNullWindow),
                nonNullIsolation);
    }

    @Override
    public OperationResult<SegmentId> put(final K key, final V value) {
        final K nonNullKey = requireKey(key);
        final V nonNullValue = requireValue(value);
        return backgroundSplitCoordinator.runWithSharedSplitAdmission(
                () -> putWithResolvedRoute(nonNullKey, nonNullValue));
    }

    private OperationResult<SegmentId> putWithResolvedRoute(final K key,
            final V value) {
        final OperationResult<SegmentId> routeResult = resolveWriteSegmentId(key);
        if (!wasRouteResolved(routeResult)) {
            return toSegmentResult(routeResult.getStatus(), null);
        }
        final SegmentId segmentId = routeResult.getValue();
        final OperationResult<Void> writeResult = stableSegmentAccess.put(segmentId,
                key, value);
        return toSegmentResult(writeResult.getStatus(), segmentId);
    }

    private OperationResult<SegmentId> resolveWriteSegmentId(final K key) {
        final Snapshot<K> snapshot = currentRouteSnapshot();
        final SegmentId routedSegmentId = snapshot.findSegmentIdForKey(key);
        if (!canResolveInitialRoute(snapshot, routedSegmentId, key)) {
            return OperationResult.busy();
        }
        final SegmentId segmentId = resolveStableSegmentId(key);
        if (segmentId == null) {
            return OperationResult.busy();
        }
        return OperationResult.ok(segmentId);
    }

    private boolean canResolveInitialRoute(final Snapshot<K> snapshot,
            final SegmentId routedSegmentId, final K key) {
        if (routedSegmentId == null) {
            return canExtendTailRoute(snapshot, key);
        }
        return !isSplitBlocked(routedSegmentId);
    }

    private boolean canExtendTailRoute(final Snapshot<K> snapshot, final K key) {
        return !isTailRouteSplitBlocked(snapshot)
                && keyToSegmentMap.extendMaxKeyIfNeeded(key);
    }

    private SegmentId resolveStableSegmentId(final K key) {
        final Snapshot<K> stableSnapshot = currentRouteSnapshot();
        final SegmentId segmentId = stableSnapshot.findSegmentIdForKey(key);
        if (segmentId == null || isSplitBlocked(segmentId)) {
            return null;
        }
        return segmentId;
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

    private boolean isSplitBlocked(final SegmentId segmentId) {
        return backgroundSplitCoordinator.isSplitBlocked(segmentId);
    }

    private boolean wasRouteResolved(final OperationResult<SegmentId> routeResult) {
        return routeResult.getStatus() == OperationStatus.OK
                && routeResult.getValue() != null;
    }

    private boolean isTailRouteSplitBlocked(final Snapshot<K> snapshot) {
        final var segmentIds = snapshot.getSegmentIds(SegmentWindow.unbounded());
        if (segmentIds.isEmpty()) {
            return false;
        }
        return isSplitBlocked(segmentIds.get(segmentIds.size() - 1));
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

    private static OperationResult<SegmentId> toSegmentResult(
            final OperationStatus status, final SegmentId segmentId) {
        if (status == OperationStatus.BUSY) {
            return OperationResult.busy();
        }
        if (status == OperationStatus.CLOSED) {
            return OperationResult.closed();
        }
        if (status == OperationStatus.OK) {
            return OperationResult.ok(segmentId);
        }
        return OperationResult.error();
    }
}
