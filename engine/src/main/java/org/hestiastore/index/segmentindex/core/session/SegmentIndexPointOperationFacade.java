package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;

/**
 * Owns mutation operations on the index data path.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexPointOperationFacade<K, V> {

    private final SegmentIndexTrackedOperationRunner<K, V> trackedRunner;
    private final SegmentIndexDataAccess<K, V> dataAccess;

    public SegmentIndexPointOperationFacade(
            final SegmentIndexTrackedOperationRunner<K, V> trackedRunner,
            final SegmentIndexDataAccess<K, V> dataAccess) {
        this.trackedRunner = Vldtn.requireNonNull(trackedRunner,
                "trackedRunner");
        this.dataAccess = Vldtn.requireNonNull(dataAccess, "dataAccess");
    }

    public void put(final K key, final V value) {
        trackedRunner.runTrackedVoid(() -> dataAccess.put(key, value));
    }

    public V get(final K key) {
        return trackedRunner.runTracked(() -> dataAccess.get(key));
    }

    public void delete(final K key) {
        trackedRunner.runTrackedVoid(() -> dataAccess.delete(key));
    }
}
