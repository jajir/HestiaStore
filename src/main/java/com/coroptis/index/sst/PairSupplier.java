package com.coroptis.index.sst;

import java.util.function.Supplier;

import com.coroptis.index.Pair;

/**
 * Define supplier for key value pairs.
 * 
 * Method {@link Supplier#get()} returns <code>null</code> when there is no other element.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public interface PairSupplier<K, V> extends Supplier<Pair<K, V>> {

}
