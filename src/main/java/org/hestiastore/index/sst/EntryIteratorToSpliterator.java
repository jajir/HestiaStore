package org.hestiastore.index.sst;

import java.util.Comparator;
import java.util.Spliterator;
import java.util.function.Consumer;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.sorteddatafile.EntryComparator;

public class EntryIteratorToSpliterator<K, V>
        implements Spliterator<Entry<K, V>> {

    private final EntryIterator<K, V> entryIterator;

    private final EntryComparator<K, V> entryComparator;

    public EntryIteratorToSpliterator(final EntryIterator<K, V> entryIterator,
            final TypeDescriptor<K> keyTypeDescriptor) {
        this.entryIterator = Vldtn.requireNonNull(entryIterator, "entryIterator");
        Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
        this.entryComparator = new EntryComparator<>(
                keyTypeDescriptor.getComparator());
    }

    @Override
    public boolean tryAdvance(final Consumer<? super Entry<K, V>> action) {
        if (entryIterator.hasNext()) {
            action.accept(entryIterator.next());
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Comparator<? super Entry<K, V>> getComparator() {
        return entryComparator;
    }

    @Override
    public Spliterator<Entry<K, V>> trySplit() {
        /*
         * It's not supported. So return null.
         */
        return null;
    }

    @Override
    public long estimateSize() {
        /*
         * Stream is not sized.
         */
        return Integer.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return Spliterator.DISTINCT | Spliterator.IMMUTABLE
                | Spliterator.NONNULL | Spliterator.SORTED;
    }

}
