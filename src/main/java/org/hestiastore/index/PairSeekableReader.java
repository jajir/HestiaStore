package org.hestiastore.index;

@Deprecated
// TODO remove
public interface PairSeekableReader<K, V> extends CloseablePairReader<K, V> {

    void seek(long position);

}
