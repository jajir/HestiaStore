package com.coroptis.index.sst;

import java.util.Comparator;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;

import com.coroptis.index.Pair;
import com.coroptis.index.PairReader;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.sorteddatafile.PairComparator;

@Deprecated
public class StreamSpliteratorFromPairReader<K, V>
        implements Spliterator<Pair<K, V>> {

    private final PairReader<K, V> pairReader;

    private final PairComparator<K, V> pairComparator;

    public StreamSpliteratorFromPairReader(final PairReader<K, V> pairReader,
            final TypeDescriptor<K> keyTypeDescriptor) {
        this.pairReader = Objects.requireNonNull(pairReader);
        Objects.requireNonNull(keyTypeDescriptor,
                "Key type descriptor must not be null");
        this.pairComparator = new PairComparator<>(
                keyTypeDescriptor.getComparator());
    }

    @Override
    public boolean tryAdvance(final Consumer<? super Pair<K, V>> action) {
        final Pair<K, V> pair = pairReader.read();
        if (pair == null) {
            return false;
        } else {
            action.accept(pair);
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
