package com.hestiastore.index;

import java.util.Spliterator;

/**
 * Interaface defining sliterator that could be closed.
 *
 */
public interface CloseableSpliterator<K, V>
        extends Spliterator<Pair<K, V>>, CloseableResource {

}
