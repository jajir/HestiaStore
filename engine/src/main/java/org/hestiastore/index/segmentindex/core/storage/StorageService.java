package org.hestiastore.index.segmentindex.core.storage;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.wal.WalRuntime;

/**
 * Storage-level operations that coordinate persisted routing metadata with
 * physical segment storage.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface StorageService<K, V> {

    /**
     * Creates a builder for storage services.
     *
     * @param <K> key type
     * @param <V> value type
     * @return storage service builder
     */
    static <K, V> StorageServiceBuilder<K, V> builder() {
        return new StorageServiceBuilder<>();
    }

    /**
     * Deletes physical segment directories that are no longer referenced by the
     * persisted route map.
     */
    void cleanupOrphanedSegmentDirectories();

    /**
     * Checks whether a segment still has its physical lock file.
     *
     * @param segmentId segment id
     * @return true when the segment lock file exists
     */
    boolean hasSegmentLockFile(SegmentId segmentId);

    /**
     * Validates and repairs persisted route-map-to-segment consistency.
     */
    void checkAndRepairConsistency();

    /**
     * Runs startup consistency validation with recovery lock-file filtering.
     */
    void runStartupConsistencyCheck();

    /**
     * Initializes WAL coordination for this storage service. Disabled WAL
     * configuration installs a no-op coordinator.
     *
     * @param conf effective index configuration
     * @param walRuntime WAL runtime, required when WAL is enabled
     * @param prepareDurableStateAction action run before checkpoint durability
     * @param flushDurableStateAction action that flushes durable state
     * @param stateSupplier runtime state supplier
     * @param failureHandler runtime failure handler
     * @param lastAppliedWalLsn last durable WAL LSN tracker
     */
    void initializeWal(EffectiveIndexConfiguration<K, V> conf,
            WalRuntime<K, V> walRuntime,
            Runnable prepareDurableStateAction,
            Runnable flushDurableStateAction,
            Supplier<SegmentIndexState> stateSupplier,
            Consumer<RuntimeException> failureHandler,
            AtomicLong lastAppliedWalLsn);

    /**
     * Replays unapplied WAL entries into the runtime.
     *
     * @param replayConsumer replay consumer invoked for each recovered entry
     */
    void recoverFromWal(WalRuntime.ReplayConsumer<K, V> replayConsumer);

    /**
     * Runs a WAL checkpoint.
     */
    void checkpointWal();

    /**
     * Appends one put entry to the WAL.
     *
     * @param key entry key
     * @param value entry value
     * @return appended WAL LSN or {@code 0} when WAL is disabled
     */
    long appendWalPut(K key, V value);

    /**
     * Appends one delete entry to the WAL.
     *
     * @param key deleted key
     * @return appended WAL LSN or {@code 0} when WAL is disabled
     */
    long appendWalDelete(K key);

    /**
     * Records the highest WAL LSN already applied to durable runtime state.
     *
     * @param walLsn applied WAL LSN
     */
    void recordAppliedWalLsn(long walLsn);

    /**
     * Closes WAL coordination resources.
     */
    void closeWal();
}
