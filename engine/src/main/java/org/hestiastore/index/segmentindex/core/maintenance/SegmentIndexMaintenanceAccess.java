package org.hestiastore.index.segmentindex.core.maintenance;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.storage.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;

/**
 * Capability view exposing only the runtime services needed by maintenance
 * facade and lifecycle code.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SegmentIndexMaintenanceAccess<K, V> {

    static <K, V> SegmentIndexMaintenanceAccess<K, V> create(
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SplitMaintenanceSynchronization<K, V> splitSynchronization,
            final StableSegmentMaintenanceAccess<K, V> stableSegmentCoordinator,
            final IndexWalCoordinator<K, V> walCoordinator) {
        return new IndexMaintenanceCoordinator<>(
                Vldtn.requireNonNull(keyToSegmentMap, "keyToSegmentMap"),
                Vldtn.requireNonNull(splitSynchronization,
                        "splitSynchronization"),
                Vldtn.requireNonNull(stableSegmentCoordinator,
                        "stableSegmentCoordinator"),
                Vldtn.requireNonNull(walCoordinator, "walCoordinator"));
    }

    void compact();

    void compactAndWait();

    void flush();

    void flushAndWait();

    void invalidateSegmentIterators();

    void awaitSplitsIdle(long timeoutMillis);
}
