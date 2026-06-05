package org.hestiastore.index.segmentindex.core.storage;

import org.hestiastore.index.segmentindex.wal.WalRuntime;

interface IndexWalCoordinatorDelegate<K, V> extends AutoCloseable {

    void recover(WalRuntime.ReplayConsumer<K, V> replayConsumer);

    void checkpoint();

    long appendPut(K key, V value);

    long appendDelete(K key);

    void recordAppliedLsn(long walLsn);

    @Override
    void close();
}
