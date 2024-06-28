package com.coroptis.index.sst;

import java.util.function.Supplier;

import com.coroptis.index.Pair;

/**
 * Define supplier for key value pairs.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public interface PairSupplier<K, V> extends Supplier<Pair<K, V>> {

}
