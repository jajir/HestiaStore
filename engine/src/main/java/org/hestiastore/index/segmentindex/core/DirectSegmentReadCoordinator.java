package org.hestiastore.index.segmentindex.core;

import java.util.List;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.Snapshot;
import org.hestiastore.index.segmentindex.split.BackgroundSplitCoordinator;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Owns direct reads from routed stable segments.
 */
final class DirectSegmentReadCoordinator<K, V> {

    private static final String OPERATION_OPEN_FULL_ISOLATION_ITERATOR = "openFullIsolationIterator";

    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final SegmentRegistry<K, V> segmentRegistry;
    private final StableSegmentGateway<K, V> stableSegmentGateway;
    private final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator;
    private final IndexRetryPolicy retryPolicy;

    DirectSegmentReadCoordinator(
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final StableSegmentGateway<K, V> stableSegmentGateway,
            final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator,
            final IndexRetryPolicy retryPolicy) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        this.stableSegmentGateway = Vldtn.requireNonNull(stableSegmentGateway,
                "stableSegmentGateway");
        this.backgroundSplitCoordinator = Vldtn.requireNonNull(
                backgroundSplitCoordinator, "backgroundSplitCoordinator");
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
    }

    IndexResult<V> get(final K key) {
        Vldtn.requireNonNull(key, "key");
        return backgroundSplitCoordinator
                .runWithSharedSplitAdmission(
                        () -> stableSegmentGateway.get(key));
    }

    EntryIterator<K, V> openWindowIterator(final SegmentWindow resolvedWindows,
            final SegmentIteratorIsolation isolation) {
        Vldtn.requireNonNull(resolvedWindows, "resolvedWindows");
        Vldtn.requireNonNull(isolation, "isolation");
        if (isolation == SegmentIteratorIsolation.FULL_ISOLATION) {
            return backgroundSplitCoordinator
                    .runWithSharedSplitAdmission(
                    () -> openStableIteratorWithRouteSnapshot(resolvedWindows,
                            isolation));
        }
        return openStableIterator(keyToSegmentMap.getSegmentIds(resolvedWindows),
                isolation);
    }

    private EntryIterator<K, V> openStableIteratorWithRouteSnapshot(
            final SegmentWindow resolvedWindows,
            final SegmentIteratorIsolation isolation) {
        final long startNanos = retryPolicy.startNanos();
        while (true) {
            final Snapshot<K> snapshot = keyToSegmentMap.snapshot();
            final List<SegmentId> segmentIds = snapshot
                    .getSegmentIds(resolvedWindows);
            final EntryIterator<K, V> iterator = openStableIterator(segmentIds,
                    isolation);
            if (keyToSegmentMap.isAtVersion(snapshot.version())) {
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
        return new SegmentsIterator<>(segmentIds, segmentRegistry, isolation,
                retryPolicy);
    }
}
