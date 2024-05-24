package com.coroptis.index.segment;

import com.coroptis.index.CloseableResource;

public interface SegmentIndexSearcher<K, V> extends CloseableResource {

    V search(K key, long startPosition);

}
