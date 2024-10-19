package com.coroptis.index;

/**
 * Allows to sequentially read key value pairs from data file. After data
 * reading resource should be closed.
 * 
 * @author Honza
 *
 * @param<K> key type
 * @param <V> value type
 */
public interface CloseablePairReader<K, V> extends CloseableResource, PairReader<K, V> {

}
