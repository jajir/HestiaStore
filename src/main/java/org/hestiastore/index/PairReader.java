package org.hestiastore.index;

/**
 * Allows to sequentially read key value. When <code>null</code> is returned it
 * means that last pair was returned.
 * 
 * @author Honza
 *
 * @param<K> key type
 * @param <V> value type
 */
@Deprecated
// TODO remove it
public interface PairReader<K, V> extends Reader<Pair<K, V>> {

    /**
     * Try to read data.
     * 
     * @return Return read data when it's possible. Return <code>null</code>
     *         when there are no data.
     */
    Pair<K, V> read();

}
