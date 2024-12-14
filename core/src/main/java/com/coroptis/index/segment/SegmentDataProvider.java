package com.coroptis.index.segment;

import com.coroptis.index.bloomfilter.BloomFilter;
import com.coroptis.index.scarceindex.ScarceIndex;

/**
 * Provide access to main data object that are considerably large in memory.
 * Implementations use some sort of cache to minimize memory impact.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public interface SegmentDataProvider<K, V> {

    /**
     * Provide information if data are loaded into memory.
     * 
     * @return
     */
    boolean isLoaded();

    SegmentDeltaCache<K, V> getSegmentDeltaCache();

    BloomFilter<K> getBloomFilter();

    ScarceIndex<K> getScarceIndex();

    /**
     * Invalidate object in memory. Could be used in both scenarios: to free
     * some memory of to evict obsolete data.
     *
     * internally it will close all resources.
     */
    void invalidate();
}
