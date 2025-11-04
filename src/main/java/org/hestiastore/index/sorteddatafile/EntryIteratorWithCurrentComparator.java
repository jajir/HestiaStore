package org.hestiastore.index.sorteddatafile;

import java.util.Comparator;
import java.util.Optional;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorWithCurrent;
import org.hestiastore.index.Vldtn;

public class EntryIteratorWithCurrentComparator<K, V>
        implements Comparator<EntryIteratorWithCurrent<K, V>> {

    private final Comparator<K> keyComparator;

    public EntryIteratorWithCurrentComparator(
            final Comparator<K> keyComparator) {
        this.keyComparator = Vldtn.requireNonNull(keyComparator,
                "keyComparator");
    }

    @Override
    public int compare(final EntryIteratorWithCurrent<K, V> iter1,
            final EntryIteratorWithCurrent<K, V> iter2) {

        if (iter1 == null && iter2 == null) {
            return 0;
        } else if (iter1 == null) {
            return -1;
        } else if (iter2 == null) {
            return 1;
        }

        final Optional<Entry<K, V>> oPair1 = iter1.getCurrent();
        final Optional<Entry<K, V>> oPair2 = iter2.getCurrent();

        if (oPair1.isPresent()) {
            if (oPair2.isPresent()) {
                final K k1 = oPair1.get().getKey();
                final K k2 = oPair2.get().getKey();
                return keyComparator.compare(k1, k2);
            } else {
                return 1;
            }
        } else {
            if (oPair2.isPresent()) {
                return -1;
            } else {
                return 0;
            }
        }

    }

}
