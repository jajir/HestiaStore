package com.coroptis.index;

public interface PairSeekableReader<K, V>
        extends PairReader<K, V>, CloseableResource {

    void seek(long position);

}
