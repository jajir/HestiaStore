package org.hestiastore.index.segmentindex.core.storage;

import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Observer notified as core storage resources are opened for an index runtime.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SegmentIndexCoreStorageOpenObserver<K, V> {

    /**
     * Called after the route map was opened.
     *
     * @param keyToSegmentMap created route map
     */
    default void onKeyToSegmentMapCreated(
            final KeyToSegmentMap<K> keyToSegmentMap) {
    }

    /**
     * Called after the segment registry was opened.
     *
     * @param segmentRegistry created segment registry
     */
    default void onSegmentRegistryCreated(
            final SegmentRegistry<K, V> segmentRegistry) {
    }
}
