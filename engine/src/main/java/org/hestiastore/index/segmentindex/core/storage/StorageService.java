package org.hestiastore.index.segmentindex.core.storage;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.wal.WalRuntime;

/**
 * Storage-level operations that coordinate persisted routing metadata with
 * physical segment storage.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class StorageService<K, V> {

    private final RecoverySegmentDirectoryInspector<K> segmentDirectoryInspector;
    private final OrphanedSegmentDirectoryRemover<K, V> orphanedSegmentDirectoryRemover;
    private final IndexConsistencyCoordinator<K, V> consistencyCoordinator;
    private final BusyRetryPolicy walBackpressureRetryPolicy;
    private IndexWalCoordinatorDelegate<K, V> walCoordinator;

    StorageService(
            final RecoverySegmentDirectoryInspector<K> segmentDirectoryInspector,
            final OrphanedSegmentDirectoryRemover<K, V> orphanedSegmentDirectoryRemover,
            final IndexConsistencyCoordinator<K, V> consistencyCoordinator,
            final BusyRetryPolicy walBackpressureRetryPolicy) {
        this.segmentDirectoryInspector = Vldtn.requireNonNull(
                segmentDirectoryInspector, "segmentDirectoryInspector");
        this.orphanedSegmentDirectoryRemover = Vldtn.requireNonNull(
                orphanedSegmentDirectoryRemover,
                "orphanedSegmentDirectoryRemover");
        this.consistencyCoordinator = Vldtn.requireNonNull(
                consistencyCoordinator, "consistencyCoordinator");
        this.walBackpressureRetryPolicy = Vldtn.requireNonNull(
                walBackpressureRetryPolicy, "walBackpressureRetryPolicy");
        walCoordinator = new DisabledIndexWalCoordinator<>();
    }

    /**
     * Creates a builder for storage services.
     *
     * @param <M> key type
     * @param <N> value type
     * @return storage service builder
     */
    public static <M, N> StorageServiceBuilder<M, N> builder() {
        return new StorageServiceBuilder<>();
    }

    /**
     * Deletes physical segment directories that are no longer referenced by the
     * persisted route map.
     */
    public void cleanupOrphanedSegmentDirectories() {
        segmentDirectoryInspector.discoverOrphanedSegmentDirectories()
                .forEach(orphanedSegmentDirectoryRemover::remove);
    }

    /**
     * Checks whether a segment still has its physical lock file.
     *
     * @param segmentId segment id
     * @return true when the segment lock file exists
     */
    public boolean hasSegmentLockFile(final SegmentId segmentId) {
        return segmentDirectoryInspector.hasSegmentLockFile(
                Vldtn.requireNonNull(segmentId, "segmentId"));
    }

    /**
     * Validates and repairs persisted route-map-to-segment consistency.
     */
    public void checkAndRepairConsistency() {
        consistencyCoordinator.checkAndRepairConsistency();
    }

    /**
     * Runs startup consistency validation with recovery lock-file filtering.
     */
    public void runStartupConsistencyCheck() {
        consistencyCoordinator.runStartupConsistencyCheck();
    }

    /**
     * Initializes WAL coordination for this storage service. Disabled WAL
     * configuration installs a no-op coordinator.
     *
     * @param initialization named WAL runtime initialization collaborators
     */
    public void initializeWal(
            final WalRuntimeInitialization<K, V> initialization) {
        final WalRuntimeInitialization<K, V> walInitialization =
                Vldtn.requireNonNull(initialization, "initialization");
        if (!walInitialization.configuration().wal().isEnabled()) {
            walCoordinator = new DisabledIndexWalCoordinator<>();
            return;
        }
        walCoordinator = ActiveIndexWalCoordinator.create(walInitialization,
                walBackpressureRetryPolicy);
    }

    /**
     * Replays unapplied WAL entries into the runtime.
     *
     * @param replayConsumer replay consumer invoked for each recovered entry
     */
    public void recoverFromWal(
            final WalRuntime.ReplayConsumer<K, V> replayConsumer) {
        walCoordinator.recover(replayConsumer);
    }

    /**
     * Runs a WAL checkpoint.
     */
    public void checkpointWal() {
        walCoordinator.checkpoint();
    }

    /**
     * Appends one put entry to the WAL.
     *
     * @param key entry key
     * @param value entry value
     * @return appended WAL LSN or {@code 0} when WAL is disabled
     */
    public long appendWalPut(final K key, final V value) {
        return walCoordinator.appendPut(key, value);
    }

    /**
     * Appends one delete entry to the WAL.
     *
     * @param key deleted key
     * @return appended WAL LSN or {@code 0} when WAL is disabled
     */
    public long appendWalDelete(final K key) {
        return walCoordinator.appendDelete(key);
    }

    /**
     * Records the highest WAL LSN already applied to durable runtime state.
     *
     * @param walLsn applied WAL LSN
     */
    public void recordAppliedWalLsn(final long walLsn) {
        walCoordinator.recordAppliedLsn(walLsn);
    }

    /**
     * Closes WAL coordination resources.
     */
    public void closeWal() {
        walCoordinator.close();
    }
}
