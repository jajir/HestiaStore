package org.hestiastore.index.sorteddatafile;

import java.util.Comparator;
import java.util.Spliterator;
import java.util.function.Consumer;

import org.hestiastore.index.CloseablePairReader;
import org.hestiastore.index.Pair;
import org.hestiastore.index.Vldtn;

/**
 * A spliterator that reads pairs from a {@link CloseablePairReader} and
 * provides them to a consumer. The spliterator is sorted according to a
 * specified {@link PairComparator}.
 * 
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class PairReaderSpliterator<K, V> implements Spliterator<Pair<K, V>> {

    private final CloseablePairReader<K, V> pairReader;

    private final PairComparator<K, V> pairComparator;

    public PairReaderSpliterator(final CloseablePairReader<K, V> pairReader,
            final PairComparator<K, V> pairComparator) {
        this.pairReader = Vldtn.requireNonNull(pairReader, "pairReader");
        this.pairComparator = Vldtn.requireNonNull(pairComparator,
                "pairComparator");
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
