package org.hestiastore.index.segmentindex.wal;

import java.util.concurrent.CompletableFuture;

/**
 * Queued WAL append request processed by the append worker.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class WalAppendTask<K, V> {

    private final WalRuntime.Operation operation;
    private final K key;
    private final V value;
    private final CompletableFuture<Long> writtenLsn =
            new CompletableFuture<>();
    private final boolean stop;

    WalAppendTask(final WalRuntime.Operation operation, final K key,
            final V value) {
        this.operation = operation;
        this.key = key;
        this.value = value;
        this.stop = false;
    }

    private WalAppendTask() {
        this.operation = null;
        this.key = null;
        this.value = null;
        this.stop = true;
    }

    static <K, V> WalAppendTask<K, V> stopTask() {
        return new WalAppendTask<>();
    }

    WalRuntime.Operation operation() {
        return operation;
    }

    K key() {
        return key;
    }

    V value() {
        return value;
    }

    boolean stop() {
        return stop;
    }

    CompletableFuture<Long> writtenLsn() {
        return writtenLsn;
    }

    void complete(final long lsn) {
        writtenLsn.complete(Long.valueOf(lsn));
    }

    void fail(final RuntimeException failure) {
        writtenLsn.completeExceptionally(failure);
    }
}
