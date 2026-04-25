package org.hestiastore.index.segmentindex.core.maintenance;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.split.SplitService;
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
            final SplitService<K, V> splitService,
            final StableSegmentMaintenanceAccess<K, V> stableSegmentCoordinator,
            final IndexWalCoordinator<K, V> walCoordinator) {
        return new IndexMaintenanceCoordinator<>(
                Vldtn.requireNonNull(keyToSegmentMap, "keyToSegmentMap"),
                Vldtn.requireNonNull(splitService, "splitService"),
                Vldtn.requireNonNull(stableSegmentCoordinator,
                        "stableSegmentCoordinator"),
                Vldtn.requireNonNull(walCoordinator, "walCoordinator"));
    }

    void compact();

    void compactAndWait();

    void flush();

    void flushAndWait();

    void invalidateSegmentIterators();
}
