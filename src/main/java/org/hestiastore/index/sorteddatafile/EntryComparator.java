package org.hestiastore.index.sorteddatafile;

import java.util.Comparator;

import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;

/**
 * Allows to compare entries by key.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class EntryComparator<K, V> implements Comparator<Entry<K, V>> {

    private final Comparator<? super K> keyComparator;

    /**
     * Creates a comparator that orders entries by their keys.
     *
     * @param keyComparator comparator for keys
     */
    public EntryComparator(final Comparator<? super K> keyComparator) {
        this.keyComparator = Vldtn.requireNonNull(keyComparator,
                "keyComparator");
    }

    /**
     * Compares two entries by their keys.
     *
     * @param entry1 first entry
     * @param entry2 second entry
     * @return comparator result for the keys
     */
    @Override
    public int compare(final Entry<K, V> entry1,
            final Entry<K, V> entry2) {
        return keyComparator.compare(entry1.getKey(), entry2.getKey());
    }

}
