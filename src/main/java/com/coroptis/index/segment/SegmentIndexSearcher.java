package com.coroptis.index.segment;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.Pair;

public interface SegmentIndexSearcher<K, V> extends CloseableResource {

    Pair<K, V> search(K key, long startPosition);

}
