package org.hestiastore.index.sorteddatafile;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;

/**
 * Allows to merge two key value entries into one.
 * 
 * It's useful for sorting data.
 * 
 * @author jan
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface Merger<K, V> {

    default Entry<K, V> merge(final Entry<K, V> entry1,
            final Entry<K, V> entry2) {
        Vldtn.requireNonNull(entry1, "entry1");
        Vldtn.requireNonNull(entry2, "entry2");
        final K key = entry1.getKey();
        if (!key.equals(entry2.getKey())) {
            throw new IllegalArgumentException(
                    "Comparing entry with different keys");
        }
        final V val = merge(key, entry1.getValue(), entry2.getValue());
        if (val == null) {
            throw new IndexException(String.format(
                    "Results of merging values '%s' and '%s' cant't by null.",
                    entry1, entry2));
        }
        return new Entry<K, V>(key, val);
    }

    /**
     * Merge two values associated with one key.
     * 
     * @param key    required key
     * @param value1 required value of first entry
     * @param value2 required value of second entry
     * @return merged value
     */
    V merge(K key, V value1, V value2);

}
