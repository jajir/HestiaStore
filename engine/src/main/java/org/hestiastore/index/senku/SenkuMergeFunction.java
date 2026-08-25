package org.hestiastore.index.senku;

/**
 * Reduces two values written for the same key to one value.
 * <p>
 * Senku does not preserve write order. Implementations must therefore be fast,
 * associative, commutative, and free of interaction with external state.
 *
 * @param <K> key type
 * @param <V> value type
 */
@FunctionalInterface
public interface SenkuMergeFunction<K, V> {

    /**
     * Merges two values belonging to the same key.
     *
     * @param key         duplicate key
     * @param firstValue one value associated with the key
     * @param secondValue another value associated with the key
     * @return merged non-null value
     */
    V apply(K key, V firstValue, V secondValue);
}
