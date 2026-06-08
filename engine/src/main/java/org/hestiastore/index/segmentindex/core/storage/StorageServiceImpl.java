package org.hestiastore.index.segmentindex.core.storage;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.wal.WalRuntime;

/**
 * Default storage service implementation.
 */
final class StorageServiceImpl<K, V> implements StorageService<K, V> {

    private final RecoverySegmentDirectoryInspector<K> segmentDirectoryInspector;
    private final OrphanedSegmentDirectoryRemover<K, V> orphanedSegmentDirectoryRemover;
    private final IndexConsistencyCoordinator<K, V> consistencyCoordinator;
    private final WalBackpressureRetryPolicy walBackpressureRetryPolicy;
    private IndexWalCoordinator<K, V> walCoordinator;

    StorageServiceImpl(
            final RecoverySegmentDirectoryInspector<K> segmentDirectoryInspector,
            final OrphanedSegmentDirectoryRemover<K, V> orphanedSegmentDirectoryRemover,
            final IndexConsistencyCoordinator<K, V> consistencyCoordinator,
            final WalBackpressureRetryPolicy walBackpressureRetryPolicy) {
        this.segmentDirectoryInspector = Vldtn.requireNonNull(
                segmentDirectoryInspector, "segmentDirectoryInspector");
        this.orphanedSegmentDirectoryRemover = Vldtn.requireNonNull(
                orphanedSegmentDirectoryRemover,
                "orphanedSegmentDirectoryRemover");
        this.consistencyCoordinator = Vldtn.requireNonNull(
                consistencyCoordinator, "consistencyCoordinator");
        this.walBackpressureRetryPolicy = Vldtn.requireNonNull(
                walBackpressureRetryPolicy, "walBackpressureRetryPolicy");
        walCoordinator = IndexWalCoordinator.disabled();
    }

    @Override
    public void cleanupOrphanedSegmentDirectories() {
        segmentDirectoryInspector.discoverOrphanedSegmentDirectories()
                .forEach(orphanedSegmentDirectoryRemover::remove);
    }

    @Override
    public boolean hasSegmentLockFile(final SegmentId segmentId) {
        return segmentDirectoryInspector.hasSegmentLockFile(
                Vldtn.requireNonNull(segmentId, "segmentId"));
    }

    @Override
    public void checkAndRepairConsistency() {
        consistencyCoordinator.checkAndRepairConsistency();
    }

    @Override
    public void runStartupConsistencyCheck() {
        consistencyCoordinator.runStartupConsistencyCheck();
    }

    @Override
    public void initializeWal(
            final EffectiveIndexConfiguration<K, V> conf,
            final WalRuntime<K, V> walRuntime,
            final Runnable prepareDurableStateAction,
            final Runnable flushDurableStateAction,
            final Supplier<SegmentIndexState> stateSupplier,
            final Consumer<RuntimeException> failureHandler,
            final AtomicLong lastAppliedWalLsn) {
        final EffectiveIndexConfiguration<K, V> validatedConf =
                Vldtn.requireNonNull(conf, "conf");
        if (!validatedConf.wal().isEnabled()) {
            walCoordinator = IndexWalCoordinator.disabled();
            return;
        }
        walCoordinator = IndexWalCoordinator.create(validatedConf,
                Vldtn.requireNonNull(walRuntime, "walRuntime"),
                walBackpressureRetryPolicy,
                Vldtn.requireNonNull(prepareDurableStateAction,
                        "prepareDurableStateAction"),
                Vldtn.requireNonNull(flushDurableStateAction,
                        "flushDurableStateAction"),
                Vldtn.requireNonNull(stateSupplier, "stateSupplier"),
                Vldtn.requireNonNull(failureHandler, "failureHandler"),
                Vldtn.requireNonNull(lastAppliedWalLsn,
                        "lastAppliedWalLsn"));
    }

    @Override
    public void recoverFromWal(
            final WalRuntime.ReplayConsumer<K, V> replayConsumer) {
        walCoordinator.recover(replayConsumer);
    }

    @Override
    public void checkpointWal() {
        walCoordinator.checkpoint();
    }

    @Override
    public long appendWalPut(final K key, final V value) {
        return walCoordinator.appendPut(key, value);
    }

    @Override
    public long appendWalDelete(final K key) {
        return walCoordinator.appendDelete(key);
    }

    @Override
    public void recordAppliedWalLsn(final long walLsn) {
        walCoordinator.recordAppliedLsn(walLsn);
    }

    @Override
    public void closeWal() {
        walCoordinator.close();
    }
}
