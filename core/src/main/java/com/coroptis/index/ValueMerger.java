package com.coroptis.index;

import java.util.Objects;

/**
 * Allows to merge two key value pairs into one.
 * 
 * @author jan
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface ValueMerger<K, V> {

    default Pair<K, V> merge(final Pair<K, V> pair1, final Pair<K, V> pair2) {
        Objects.requireNonNull(pair1, "First pair for merging can't be null.");
        Objects.requireNonNull(pair2, "Second pair for merging can't be null.");
        final K key = pair1.getKey();
        if (!key.equals(pair2.getKey())) {
            throw new IllegalArgumentException(String.format(
                    "Comparing pair with different keys '%s' and '%s'", pair1,
                    pair2));
        }
        final V val = merge(key, pair1.getValue(), pair2.getValue());
        Objects.requireNonNull(val, () -> String.format(
                "Results of merging values '%s' and '%s' cant't by null.",
                pair1, pair2));
        return new Pair<K, V>(key, val);
    }

    /**
     * Merge two values associated with one key.
     * 
     * @param key    required key
     * @param value1 required value of first pair
     * @param value2 required value of second pair
     * @return merged value
     */
    V merge(K key, V value1, V value2);

}
