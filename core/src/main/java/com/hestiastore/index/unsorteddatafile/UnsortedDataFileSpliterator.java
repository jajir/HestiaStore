package com.hestiastore.index.unsorteddatafile;

import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;

import com.hestiastore.index.CloseablePairReader;
import com.hestiastore.index.CloseableSpliterator;
import com.hestiastore.index.Pair;

public class UnsortedDataFileSpliterator<K, V>
        implements CloseableSpliterator<K, V> {

    private final CloseablePairReader<K, V> pairReader;

    public UnsortedDataFileSpliterator(
            final CloseablePairReader<K, V> pairReader) {
        this.pairReader = Objects.requireNonNull(pairReader);
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
    public Spliterator<Pair<K, V>> trySplit() {
        /*
         * It's not supported. So return null.
         */
        return null;
    }

    @Override
    public long estimateSize() {
        /*
         * Stream is not sized. It's not possible to determine stream size.
         */
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return Spliterator.DISTINCT | Spliterator.IMMUTABLE
                | Spliterator.NONNULL;
    }

    @Override
    public void close() {
        pairReader.close();
    }

}
