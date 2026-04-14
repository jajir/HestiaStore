package org.hestiastore.index.segmentindex.core;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.Snapshot;

/**
 * Owns routed writes that go directly into stable segments.
 */
final class DirectSegmentWriteCoordinator<K, V> {

    private final KeyToSegmentMap<K> keyToSegmentMap;
    private final StableSegmentGateway<K, V> stableSegmentGateway;
    private final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator;

    DirectSegmentWriteCoordinator(
            final KeyToSegmentMap<K> keyToSegmentMap,
            final StableSegmentGateway<K, V> stableSegmentGateway,
            final BackgroundSplitCoordinator<K, V> backgroundSplitCoordinator) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        this.stableSegmentGateway = Vldtn.requireNonNull(stableSegmentGateway,
                "stableSegmentGateway");
        this.backgroundSplitCoordinator = Vldtn.requireNonNull(
                backgroundSplitCoordinator, "backgroundSplitCoordinator");
    }

    IndexResult<Void> put(final K key, final V value) {
        Vldtn.requireNonNull(key, "key");
        Vldtn.requireNonNull(value, "value");
        return backgroundSplitCoordinator
                .runWithSharedSplitAdmission(() -> {
            final IndexResult<SegmentId> routeResult = resolveWriteSegmentId(
                    key);
            if (routeResult.getStatus() != IndexResultStatus.OK
                    || routeResult.getValue() == null) {
                return toVoidResult(routeResult.getStatus());
            }
            return stableSegmentGateway.put(routeResult.getValue(), key, value);
        });
    }

    private IndexResult<SegmentId> resolveWriteSegmentId(final K key) {
        final Snapshot<K> snapshot = keyToSegmentMap.snapshot();
        final SegmentId routedSegmentId = snapshot.findSegmentIdForKey(key);
        if (routedSegmentId == null) {
            if (isTailRouteSplitBlocked(snapshot)
                    || !keyToSegmentMap.extendMaxKeyIfNeeded(key)) {
                return IndexResult.busy();
            }
        } else if (backgroundSplitCoordinator.isSplitBlocked(routedSegmentId)) {
            return IndexResult.busy();
        }
        final Snapshot<K> stableSnapshot = keyToSegmentMap.snapshot();
        final SegmentId segmentId = stableSnapshot.findSegmentIdForKey(key);
        if (segmentId == null
                || backgroundSplitCoordinator.isSplitBlocked(segmentId)) {
            return IndexResult.busy();
        }
        return IndexResult.ok(segmentId);
    }

    private boolean isTailRouteSplitBlocked(final Snapshot<K> snapshot) {
        final var segmentIds = snapshot.getSegmentIds(SegmentWindow.unbounded());
        if (segmentIds.isEmpty()) {
            return false;
        }
        return backgroundSplitCoordinator.isSplitBlocked(
                segmentIds.get(segmentIds.size() - 1));
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
