package org.hestiastore.index.segmentindex.core.storage;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.wal.WalRuntime;

/**
 * Owns WAL replay, checkpointing, retention pressure handling, and failure
 * transition coordination for the segment index runtime.
 */
@SuppressWarnings("java:S107")
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
     * @param conf effective index configuration
     * @param walRuntime WAL runtime
     * @param retryPolicy package-local WAL backpressure retry policy
     * @param prepareDurableStateAction action run before checkpoint durability
     * @param flushDurableStateAction action that flushes durable state
     * @param stateSupplier runtime state supplier
     * @param failureHandler runtime failure handler
     * @param lastAppliedWalLsn last durable WAL LSN tracker
     * @return active WAL coordinator
     */
    @SuppressWarnings("java:S107")
    static <K, V> IndexWalCoordinator<K, V> create(
            final EffectiveIndexConfiguration<K, V> conf,
            final WalRuntime<K, V> walRuntime,
            final WalBackpressureRetryPolicy retryPolicy,
            final Runnable prepareDurableStateAction,
            final Runnable flushDurableStateAction,
            final Supplier<SegmentIndexState> stateSupplier,
            final Consumer<RuntimeException> failureHandler,
            final AtomicLong lastAppliedWalLsn) {
        Vldtn.requireTrue(Vldtn.requireNonNull(conf, "conf").wal().isEnabled(),
                "WAL configuration must be enabled to create active WAL coordinator.");
        return new IndexWalCoordinator<>(ActiveIndexWalCoordinator.create(conf,
                Vldtn.requireNonNull(walRuntime, "walRuntime"),
                Vldtn.requireNonNull(retryPolicy, "retryPolicy"),
                Vldtn.requireNonNull(prepareDurableStateAction,
                        "prepareDurableStateAction"),
                Vldtn.requireNonNull(flushDurableStateAction,
                        "flushDurableStateAction"),
                Vldtn.requireNonNull(stateSupplier, "stateSupplier"),
                Vldtn.requireNonNull(failureHandler, "failureHandler"),
                Vldtn.requireNonNull(lastAppliedWalLsn,
                        "lastAppliedWalLsn")));
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
