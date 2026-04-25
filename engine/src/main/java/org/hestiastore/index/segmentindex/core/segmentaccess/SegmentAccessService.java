package org.hestiastore.index.segmentindex.core.segmentaccess;

import java.util.function.Function;

import org.hestiastore.index.segmentregistry.BlockingSegment;

/**
 * Provides scoped blocking access to the segment that owns a key.
 * <p>
 * Implementations hide route-map lookup, topology lease acquisition, split
 * draining, and registry loading from callers. The supplied operation runs
 * while the internal topology lease is held.
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
     * Runs a read operation with the segment mapped to the provided key.
     *
     * @param key key used to find the segment
     * @param operation operation to run while segment access is held
     * @param <R> result type
     * @return operation result, or {@code null} when no segment maps the key
     */
    <R> R withSegmentForRead(K key,
            Function<BlockingSegment<K, V>, R> operation);

    /**
     * Runs a write operation with the segment mapped to the provided key,
     * extending the tail route when needed.
     *
     * @param key key used to find the segment
     * @param operation operation to run while segment access is held
     * @param <R> result type
     * @return operation result
     */
    <R> R withSegmentForWrite(K key,
            Function<BlockingSegment<K, V>, R> operation);
}
