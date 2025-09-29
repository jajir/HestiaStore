package org.hestiastore.index;

/**
 * Class represents empty file.
 * 
 * It could be useful to represent empty input.
 * 
 * 
 * TODO remove it, Reader is just for low level operations.
 * 
 * @author Honza
 *
 * @param<K> key type
 * @param <V> value type
 */
@Deprecated
public class PairReaderEmpty<K, V> implements PairSeekableReader<K, V> {

    /**
     * It return just null.
     * 
     * @return Return always <code>null</code>.
     */
    public Pair<K, V> read() {
        return null;
    }

    @Override
    public void close() {
        // Intentionally do nothing
    }

    @Override
    public void seek(long position) {
        // Intentionally do nothing
    }

}
