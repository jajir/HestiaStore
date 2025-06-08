package org.hestiastore.index;

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

    public PairReaderWithLock(final CloseablePairReader<K, V> pairReader,
            final OptimisticLock optimisticLock) {
        this.reader = Vldtn.requireNonNull(pairReader, "pairReader");
        this.optimisticLock = Vldtn.requireNonNull(optimisticLock,
                "optimisticLock");
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
