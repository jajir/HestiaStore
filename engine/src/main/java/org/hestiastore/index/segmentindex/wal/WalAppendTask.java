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
    private long assignedLsn;
    private boolean written;

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

    /**
     * Records the LSN after the append worker has written the record. Completion
     * is deliberately deferred until the worker has applied the batch durability
     * boundary.
     *
     * @param lsn assigned WAL sequence number
     */
    void markWritten(final long lsn) {
        assignedLsn = lsn;
        written = true;
    }

    /**
     * Completes a successfully written append after its batch durability work.
     */
    void completeWritten() {
        if (written) {
            writtenLsn.complete(Long.valueOf(assignedLsn));
        }
    }

    void fail(final RuntimeException failure) {
        writtenLsn.completeExceptionally(failure);
    }
}
