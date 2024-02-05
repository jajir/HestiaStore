package com.coroptis.index.segment;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.PairReader;

public interface PairSearcher<K, V>
        extends PairReader<K, V>, CloseableResource {

    void seek(long position);

}
