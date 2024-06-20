package com.coroptis.index.segment;

import com.coroptis.index.bloomfilter.BloomFilter;
import com.coroptis.index.scarceindex.ScarceIndex;

//FIXME add some comment
public interface SegmentData<K, V> {

    SegmentDeltaCache<K, V> getSegmentDeltaCache();

    BloomFilter<K> getBloomFilter();

    ScarceIndex<K> getScarceIndex();

}
