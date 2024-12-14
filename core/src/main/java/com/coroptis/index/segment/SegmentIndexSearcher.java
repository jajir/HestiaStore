package com.coroptis.index.segment;

import com.coroptis.index.CloseableResource;

/**
 * Allows to search in main index file for given key from given position.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public interface SegmentIndexSearcher<K, V> extends CloseableResource {

    V search(K key, long startPosition);

}
