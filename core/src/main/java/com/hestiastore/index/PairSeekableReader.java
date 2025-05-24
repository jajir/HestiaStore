package com.hestiastore.index;

public interface PairSeekableReader<K, V> extends CloseablePairReader<K, V> {

    void seek(long position);

}
