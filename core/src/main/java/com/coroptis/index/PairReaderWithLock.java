package com.coroptis.index;

import java.util.Objects;

/**
 * To pair reader add lock, that allows to skip rest of data.
 * 
 * @author Honza
 *
 * @param<K> key type
 * @param <V> value type
 */
public class PairReaderWithLock<K, V> implements CloseablePairReader<K, V> {

    private final CloseablePairReader<K, V> reader;
    private final OptimisticLock optimisticLock;

    public PairReaderWithLock(final CloseablePairReader<K, V> reader,
            final OptimisticLock optimisticLock) {
        this.reader = Objects.requireNonNull(reader,
                "Pair reader can't be null.");
        this.optimisticLock = Objects.requireNonNull(optimisticLock,
                "Optimistic lock can't be null.");
    }

    @Override
    public void close() {
        reader.close();
    }

    @Override
    public Pair<K, V> read() {
        if (optimisticLock.isLocked()) {
            return null;
        } else {
            return reader.read();
        }
    }

}
