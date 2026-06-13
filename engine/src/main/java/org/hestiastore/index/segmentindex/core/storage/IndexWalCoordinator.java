package org.hestiastore.index.segmentindex.core.storage;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.wal.WalRuntime;

/**
 * Owns WAL replay, checkpointing, retention pressure handling, and failure
 * transition coordination for the segment index runtime.
 */
final class IndexWalCoordinator<K, V> implements AutoCloseable {

    private final IndexWalCoordinatorDelegate<K, V> delegate;

    private IndexWalCoordinator(
            final IndexWalCoordinatorDelegate<K, V> delegate) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
    }

    /**
     * Creates active WAL coordination for enabled WAL configuration.
     *
     * @param <K> key type
     * @param <V> value type
     * @param initialization named WAL runtime initialization collaborators
     * @param retryPolicy package-local WAL backpressure retry policy
     * @return active WAL coordinator
     */
    static <K, V> IndexWalCoordinator<K, V> create(
            final WalRuntimeInitialization<K, V> initialization,
            final WalBackpressureRetryPolicy retryPolicy) {
        final WalRuntimeInitialization<K, V> walInitialization =
                Vldtn.requireNonNull(initialization, "initialization");
        Vldtn.requireTrue(walInitialization.configuration().wal().isEnabled(),
                "WAL configuration must be enabled to create active WAL coordinator.");
        return new IndexWalCoordinator<>(ActiveIndexWalCoordinator.create(
                walInitialization,
                Vldtn.requireNonNull(retryPolicy, "retryPolicy")));
    }

    /**
     * Creates disabled WAL coordination.
     *
     * @param <K> key type
     * @param <V> value type
     * @return disabled WAL coordinator
     */
    static <K, V> IndexWalCoordinator<K, V> disabled() {
        return new IndexWalCoordinator<>(new DisabledIndexWalCoordinator<>());
    }

    /**
     * Replays unapplied WAL entries into the runtime.
     *
     * @param replayConsumer replay consumer invoked for each recovered entry
     */
    void recover(final WalRuntime.ReplayConsumer<K, V> replayConsumer) {
        delegate.recover(replayConsumer);
    }

    /**
     * Runs a WAL checkpoint.
     */
    void checkpoint() {
        delegate.checkpoint();
    }

    /**
     * Appends one put entry to the WAL.
     *
     * @param key entry key
     * @param value entry value
     * @return appended WAL LSN or {@code 0} when WAL is disabled
     */
    long appendPut(final K key, final V value) {
        return delegate.appendPut(key, value);
    }

    /**
     * Appends one delete entry to the WAL.
     *
     * @param key deleted key
     * @return appended WAL LSN or {@code 0} when WAL is disabled
     */
    long appendDelete(final K key) {
        return delegate.appendDelete(key);
    }

    /**
     * Records the highest WAL LSN already applied to durable runtime state.
     *
     * @param walLsn applied WAL LSN
     */
    void recordAppliedLsn(final long walLsn) {
        delegate.recordAppliedLsn(walLsn);
    }

    @Override
    public void close() {
        delegate.close();
    }
}
