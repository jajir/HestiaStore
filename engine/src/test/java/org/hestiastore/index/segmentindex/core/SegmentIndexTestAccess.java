package org.hestiastore.index.segmentindex.core;

import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.SegmentRegistryImpl;

/**
 * Test-only bridge exposing package-private runtime collaborators.
 */
public final class SegmentIndexTestAccess {

    private SegmentIndexTestAccess() {
    }

    public static <K> KeyToSegmentMapSynchronizedAdapter<K> keyToSegmentMap(
            final SegmentIndexImpl<K, ?> index) {
        return index.keyToSegmentMap();
    }

    @SuppressWarnings("unchecked")
    public static <K, V> SegmentRegistryImpl<K, V> segmentRegistry(
            final SegmentIndexImpl<K, V> index) {
        return (SegmentRegistryImpl<K, V>) index.segmentRegistry();
    }

    public static WalRuntime<?, ?> walRuntime(final SegmentIndexImpl<?, ?> index) {
        return index.walRuntime();
    }

    public static SegmentIndexStateCoordinator stateCoordinator(
            final SegmentIndexImpl<?, ?> index) {
        return new SegmentIndexStateCoordinator(index.stateCoordinator());
    }
}
