package org.hestiastore.index.segmentindex.core.storage;

import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.core.SegmentIndexRuntimeState;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexMaintenanceConfiguration;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Storage-level operations that coordinate persisted routing metadata with
 * physical segment storage.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class StorageCoordinator<K, V> {

    private final StorageConsistencyCoordinator<K, V> consistencyCoordinator;
    private final BusyRetryPolicy walBackpressureRetryPolicy;
    private WalCoordinator<K, V> walCoordinator;

    StorageCoordinator(
            final StorageConsistencyCoordinator<K, V> consistencyCoordinator,
            final BusyRetryPolicy walBackpressureRetryPolicy) {
        this.consistencyCoordinator = Vldtn.requireNonNull(
                consistencyCoordinator, "consistencyCoordinator");
        this.walBackpressureRetryPolicy = Vldtn.requireNonNull(
                walBackpressureRetryPolicy, "walBackpressureRetryPolicy");
        walCoordinator = new NoopWalCoordinator<>();
    }

    /**
     * Creates storage service from initialized storage collaborators.
     *
     * @param <M> key type
     * @param <N> value type
     * @param directoryFacade root directory used for segment-directory discovery
     * @param keyToSegmentMap persisted route map
     * @param segmentRegistry physical segment registry
     * @param maintenance maintenance retry configuration
     * @return storage service
     */
    public static <M, N> StorageCoordinator<M, N> create(
            final Directory directoryFacade,
            final SegmentRouteMap<M> keyToSegmentMap,
            final SegmentRegistry<M, N> segmentRegistry,
            final EffectiveIndexMaintenanceConfiguration maintenance) {
        final Directory validatedDirectoryFacade = Vldtn.requireNonNull(
                directoryFacade, "directoryFacade");
        final SegmentRouteMap<M> validatedKeyToSegmentMap = Vldtn
                .requireNonNull(keyToSegmentMap, "keyToSegmentMap");
        final SegmentRegistry<M, N> validatedSegmentRegistry = Vldtn
                .requireNonNull(segmentRegistry, "segmentRegistry");
        final EffectiveIndexMaintenanceConfiguration validatedMaintenance = Vldtn
                .requireNonNull(maintenance, "maintenance");
        final BusyRetryPolicy storageCleanupRetryPolicy =
                new BusyRetryPolicy(validatedMaintenance.busyBackoffMillis(),
                        validatedMaintenance.busyTimeoutMillis(),
                        "Storage cleanup operation");
        final BusyRetryPolicy walBackpressureRetryPolicy =
                new BusyRetryPolicy(validatedMaintenance.busyBackoffMillis(),
                        validatedMaintenance.busyTimeoutMillis(),
                        "WAL backpressure operation");
        final SegmentDirectoryRecoveryScanner<M> segmentDirectoryInspector =
                new SegmentDirectoryRecoveryScanner<>(
                        validatedDirectoryFacade, validatedKeyToSegmentMap);
        final OrphanedSegmentCleaner<M, N> orphanedSegmentDirectoryRemover =
                new OrphanedSegmentCleaner<>(
                        validatedSegmentRegistry,
                        storageCleanupRetryPolicy);
        return new StorageCoordinator<>(
                new StorageConsistencyCoordinator<>(validatedKeyToSegmentMap,
                        validatedSegmentRegistry, segmentDirectoryInspector,
                        orphanedSegmentDirectoryRemover),
                walBackpressureRetryPolicy);
    }

    /**
     * Deletes physical segment directories that are no longer referenced by the
     * persisted route map.
     */
    public void cleanupOrphanedSegmentDirectories() {
        consistencyCoordinator.cleanupOrphanedSegmentDirectories();
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
     * @param configuration effective index configuration
     * @param walRuntime WAL runtime, required when WAL is enabled
     * @param checkpointMaintenance maintenance used before forced WAL
     *        checkpointing
     * @param runtimeState runtime state owner used for WAL failure transitions
     * @param lastAppliedWalLsn last durable WAL LSN tracker
     */
    public void initializeWal(
            final EffectiveIndexConfiguration<K, V> configuration,
            final WalRuntime<K, V> walRuntime,
            final WalCheckpointMaintenance checkpointMaintenance,
            final SegmentIndexRuntimeState runtimeState,
            final AtomicLong lastAppliedWalLsn) {
        if (!Vldtn.requireNonNull(configuration, "configuration").wal()
                .isEnabled()) {
            walCoordinator = new NoopWalCoordinator<>();
            return;
        }
        walCoordinator = new ActiveWalCoordinator<>(configuration,
                Vldtn.requireNonNull(walRuntime, "walRuntime"),
                walBackpressureRetryPolicy,
                Vldtn.requireNonNull(checkpointMaintenance,
                        "checkpointMaintenance"),
                Vldtn.requireNonNull(runtimeState, "runtimeState"),
                Vldtn.requireNonNull(lastAppliedWalLsn, "lastAppliedWalLsn"));
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
