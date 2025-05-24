package com.hestiastore.index.segment;

import com.hestiastore.index.bloomfilter.BloomFilter;
import com.hestiastore.index.scarceindex.ScarceIndex;

/**
 * Object that hold references to largest object in segment. It allows to put
 * this object into cache.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public interface SegmentData<K, V> {

    SegmentDeltaCache<K, V> getSegmentDeltaCache();

    BloomFilter<K> getBloomFilter();

    ScarceIndex<K> getScarceIndex();

    /**
     * When object will not be used. It should be called. Methods allows to
     * close all resources.
     */
    void close();

}
