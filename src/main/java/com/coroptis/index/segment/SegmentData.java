package com.coroptis.index.segment;

import com.coroptis.index.bloomfilter.BloomFilter;
import com.coroptis.index.scarceindex.ScarceIndex;

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

}
