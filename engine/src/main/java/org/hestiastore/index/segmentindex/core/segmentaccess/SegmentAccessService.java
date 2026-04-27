package org.hestiastore.index.segmentindex.core.segmentaccess;

/**
 * Provides scoped blocking access to the segment that owns a key.
 * <p>
 * Implementations hide route-map lookup, topology lease acquisition, split
 * draining, and registry loading from callers. Callers must close returned
 * access objects after using the segment.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SegmentAccessService<K, V> {

    /**
     * Creates a builder for segment access services.
     *
     * @param <M> key type
     * @param <N> value type
     * @return segment access service builder
     */
    static <M, N> SegmentAccessServiceBuilder<M, N> builder() {
        return new SegmentAccessServiceBuilder<>();
    }

    /**
     * Acquires read access to the segment mapped to the provided key.
     *
     * @param key key used to find the segment
     * @return segment access, or {@code null} when no segment maps the key
     */
    SegmentAccess<K, V> acquireForRead(K key);

    /**
     * Acquires write access to the segment mapped to the provided key,
     * extending the tail route when needed.
     *
     * @param key key used to find the segment
     * @return segment access
     */
    SegmentAccess<K, V> acquireForWrite(K key);
}
