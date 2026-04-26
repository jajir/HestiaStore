package org.hestiastore.index.segmentindex.core.streaming;

import java.util.List;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.Snapshot;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Coordinates stable segment window iterators.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class DirectSegmentCoordinator<K, V> implements DirectSegmentAccess<K, V> {

    private static final String OPERATION_OPEN_FULL_ISOLATION_ITERATOR = "openFullIsolationIterator";

    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final IndexRetryPolicy retryPolicy;

    DirectSegmentCoordinator(
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final IndexRetryPolicy retryPolicy) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
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
}
