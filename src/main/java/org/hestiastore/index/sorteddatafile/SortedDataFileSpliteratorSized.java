package org.hestiastore.index.sorteddatafile;

import java.util.Comparator;
import java.util.Spliterator;
import java.util.function.Consumer;

import org.hestiastore.index.CloseablePairReader;
import org.hestiastore.index.Pair;
import org.hestiastore.index.Vldtn;

public class SortedDataFileSpliteratorSized<K, V>
        implements Spliterator<Pair<K, V>> {

    private final CloseablePairReader<K, V> pairReader;

    private final PairComparator<K, V> pairComparator;

    private final long estimateSize;

    public SortedDataFileSpliteratorSized(
            final CloseablePairReader<K, V> pairReader,
            final PairComparator<K, V> pairComparator,
            final long estimateSize) {
        this.pairReader = Vldtn.requireNonNull(pairReader, "pairReader");
        this.pairComparator = Vldtn.requireNonNull(pairComparator,
                "pairComparator");
        this.estimateSize = estimateSize;
    }

    @Override
    public boolean tryAdvance(final Consumer<? super Pair<K, V>> action) {
        final Pair<K, V> out = pairReader.read();
        if (out == null) {
            return false;
        } else {
            action.accept(out);
            return true;
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
         * Stream is sized.
         */
        return estimateSize;
    }

    @Override
    public int characteristics() {
        return Spliterator.DISTINCT | Spliterator.IMMUTABLE
                | Spliterator.NONNULL | Spliterator.SORTED | Spliterator.SIZED;
    }

}
