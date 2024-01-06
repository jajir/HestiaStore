package com.coroptis.index.segmentcache;

import com.coroptis.index.PairWriter;

public interface SegmentCacheWriter<K, V> {

    PairWriter<K, V> openWriter();

}
