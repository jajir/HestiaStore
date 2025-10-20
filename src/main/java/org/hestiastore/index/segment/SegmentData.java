package org.hestiastore.index.segment;

import org.hestiastore.index.bloomfilter.BloomFilter;
import org.hestiastore.index.scarceindex.ScarceIndex;

/**
 * Provides access to heavyweight, segment-scoped data structures such as the
 * delta cache, Bloom filter and sparse (scarce) index.
 * <p>
 * Implementations typically lazy-load these components from disk on first
 * access and may cache instances for reuse. Callers should assume that
 * returned instances can be long-lived and should invoke {@link #close()} when
 * the data container is no longer needed to release underlying resources.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SegmentData<K, V> {

    /**
     * Returns the in-memory delta cache for this segment. May trigger lazy
     * initialization on first access.
     *
     * @return delta cache instance associated with the segment
     */
    SegmentDeltaCache<K, V> getSegmentDeltaCache();

    /**
     * Returns the Bloom filter used to accelerate negative lookups. May be
     * constructed lazily based on segment configuration and on-disk files.
     *
     * @return Bloom filter instance
     */
    BloomFilter<K> getBloomFilter();

    /**
     * Returns the sparse (scarce) index that points into the main on-disk
     * index to speed up searches. May be initialized on first use.
     *
     * @return scarce index instance
     */
    ScarceIndex<K> getScarceIndex();

    /**
     * Releases resources held by this data container (e.g., closes Bloom
     * filter, clears caches) and makes subsequent getters return fresh
     * instances on demand.
     */
    void close();

}
