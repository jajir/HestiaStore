package org.hestiastore.index.segment;

import org.hestiastore.index.CloseableResource;

/**
 * Allows to search in main index file for given key from given position.
 * 
 * @author honza
 *
 * 
 * 
 * @param <K>
 * @param <V>
 */
// TODO remove it.
@Deprecated
public interface SegmentIndexSearcher<K, V> extends CloseableResource {

    V search(K key, long startPosition);

}
