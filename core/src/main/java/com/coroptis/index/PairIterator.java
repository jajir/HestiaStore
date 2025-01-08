package com.coroptis.index;

import java.util.Iterator;

/**
 * Define key value pair iterator. It allows to go through all records and
 * further works with them. When object is initialized method
 * {@link #getCurrent()} return null.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public interface PairIterator<K, V>
        extends Iterator<Pair<K, V>>, CloseableResource {

}
