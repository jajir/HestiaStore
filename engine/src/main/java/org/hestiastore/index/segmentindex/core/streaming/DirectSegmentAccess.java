package org.hestiastore.index.segmentindex.core.streaming;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Opens routed window iterators for runtime streaming operations.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface DirectSegmentAccess<K, V> {

    /**
     * Creates direct segment access with package-local streaming retry
     * semantics.
     *
     * @param <K> key type
     * @param <V> value type
     * @param keyToSegmentMap route map used to resolve segment windows
     * @param segmentRegistry registry used to open stable segment iterators
     * @param busyBackoffMillis backoff in milliseconds for transient busy
     *            states
     * @param busyTimeoutMillis timeout in milliseconds for transient busy
     *            states
     * @return direct segment access
     */
    static <K, V> DirectSegmentAccess<K, V> create(
            final KeyToSegmentMap<K> keyToSegmentMap,
            final SegmentRegistry<K, V> segmentRegistry,
            final int busyBackoffMillis,
            final int busyTimeoutMillis) {
        return new DirectSegmentCoordinator<>(keyToSegmentMap, segmentRegistry,
                new StreamingRetryPolicy(busyBackoffMillis,
                        busyTimeoutMillis));
    }

    /**
     * Opens an iterator over a resolved segment window.
     *
     * @param resolvedWindows segment window already resolved for the caller
     * @param isolation iterator isolation mode
     * @return entry iterator
     */
    EntryIterator<K, V> openWindowIterator(SegmentWindow resolvedWindows,
            SegmentIteratorIsolation isolation);
}
