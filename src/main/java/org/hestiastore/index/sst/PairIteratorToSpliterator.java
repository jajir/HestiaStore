package org.hestiastore.index.sst;

import java.util.Comparator;
import java.util.Spliterator;
import java.util.function.Consumer;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.sorteddatafile.PairComparator;

public class PairIteratorToSpliterator<K, V>
        implements Spliterator<Pair<K, V>> {

    private final PairIterator<K, V> pairIterator;

    private final PairComparator<K, V> pairComparator;

    public PairIteratorToSpliterator(final PairIterator<K, V> pairIterator,
            final TypeDescriptor<K> keyTypeDescriptor) {
        this.pairIterator = Vldtn.requireNonNull(pairIterator, "pairIterator");
        Vldtn.requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
        this.pairComparator = new PairComparator<>(
                keyTypeDescriptor.getComparator());
    }

    @Override
    public boolean tryAdvance(final Consumer<? super Pair<K, V>> action) {
        if (pairIterator.hasNext()) {
            action.accept(pairIterator.next());
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Comparator<? super Pair<K, V>> getComparator() {
        return pairComparator;
    }

    @Override
    public Spliterator<Pair<K, V>> trySplit() {
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
