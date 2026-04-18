package org.hestiastore.index.segmentindex.core;

import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.wal.WalRuntime;

/**
 * Test-only bridge exposing package-private runtime collaborators.
 */
public final class SegmentIndexTestAccess {

    private SegmentIndexTestAccess() {
    }

    public static <K> KeyToSegmentMap<K> keyToSegmentMap(
            final SegmentIndexImpl<K, ?> index) {
        return index.keyToSegmentMap();
    }

    public static Object segmentRegistry(final SegmentIndexImpl<?, ?> index) {
        return index.segmentRegistry();
    }

    public static WalRuntime<?, ?> walRuntime(final SegmentIndexImpl<?, ?> index) {
        return index.walRuntime();
    }

    public static SegmentIndexStateCoordinator stateCoordinator(
            final SegmentIndexImpl<?, ?> index) {
        return new SegmentIndexStateCoordinator(index.stateCoordinator());
    }
}
