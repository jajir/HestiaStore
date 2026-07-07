package org.hestiastore.index.segmentindex.core.storage;

import java.util.function.Consumer;

import org.hestiastore.index.segmentindex.wal.WalRuntime;

/**
 * Disabled WAL coordination for indexes that run without WAL.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class NoopWalCoordinator<K, V>
        implements WalCoordinator<K, V> {

    @Override
    public void recover(
            final Consumer<WalRuntime.ReplayRecord<K, V>> replayConsumer) {
    }

    @Override
    public void checkpoint() {
    }

    @Override
    public long appendPut(final K key, final V value) {
        return 0L;
    }

    @Override
    public long appendDelete(final K key) {
        return 0L;
    }

    @Override
    public void recordAppliedLsn(final long walLsn) {
    }

    @Override
    public void close() {
    }
}
