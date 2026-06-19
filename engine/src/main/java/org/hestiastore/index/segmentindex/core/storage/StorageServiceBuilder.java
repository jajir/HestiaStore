package org.hestiastore.index.segmentindex.core.storage;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;

/**
 * Builder for {@link StorageService} instances.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class StorageServiceBuilder<K, V> {

    private Directory directoryFacade;
    private KeyToSegmentMap<K> keyToSegmentMap;
    private SegmentRegistry<K, V> segmentRegistry;
    private Integer storageCleanupBusyBackoffMillis;
    private Integer storageCleanupBusyTimeoutMillis;
    private Integer walBackpressureBusyBackoffMillis;
    private Integer walBackpressureBusyTimeoutMillis;

    StorageServiceBuilder() {
    }

    /**
     * Sets the root directory used to inspect physical segment directories.
     *
     * @param directoryFacade directory facade
     * @return this builder
     */
    public StorageServiceBuilder<K, V> withDirectoryFacade(
            final Directory directoryFacade) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        return this;
    }

    /**
     * Sets the persisted key-to-segment map used as the routing source of
     * truth.
     *
     * @param keyToSegmentMap key-to-segment map
     * @return this builder
     */
    public StorageServiceBuilder<K, V> withKeyToSegmentMap(
            final KeyToSegmentMap<K> keyToSegmentMap) {
        this.keyToSegmentMap = Vldtn.requireNonNull(keyToSegmentMap,
                "keyToSegmentMap");
        return this;
    }

    /**
     * Sets the registry used to remove physical segment directories.
     *
     * @param segmentRegistry segment registry
     * @return this builder
     */
    public StorageServiceBuilder<K, V> withSegmentRegistry(
            final SegmentRegistry<K, V> segmentRegistry) {
        this.segmentRegistry = Vldtn.requireNonNull(segmentRegistry,
                "segmentRegistry");
        return this;
    }

    /**
     * Sets the backoff value used to create the package-local storage cleanup
     * retry policy.
     *
     * @param busyBackoffMillis backoff in milliseconds
     * @return this builder
     */
    public StorageServiceBuilder<K, V> withStorageCleanupBusyBackoffMillis(
            final int busyBackoffMillis) {
        storageCleanupBusyBackoffMillis = busyBackoffMillis;
        return this;
    }

    /**
     * Sets the timeout value used to create the package-local storage cleanup
     * retry policy.
     *
     * @param busyTimeoutMillis timeout in milliseconds
     * @return this builder
     */
    public StorageServiceBuilder<K, V> withStorageCleanupBusyTimeoutMillis(
            final int busyTimeoutMillis) {
        storageCleanupBusyTimeoutMillis = busyTimeoutMillis;
        return this;
    }

    /**
     * Sets the backoff value used to create the package-local WAL
     * backpressure retry policy.
     *
     * @param busyBackoffMillis backoff in milliseconds
     * @return this builder
     */
    public StorageServiceBuilder<K, V> withWalBackpressureBusyBackoffMillis(
            final int busyBackoffMillis) {
        walBackpressureBusyBackoffMillis = busyBackoffMillis;
        return this;
    }

    /**
     * Sets the timeout value used to create the package-local WAL
     * backpressure retry policy.
     *
     * @param busyTimeoutMillis timeout in milliseconds
     * @return this builder
     */
    public StorageServiceBuilder<K, V> withWalBackpressureBusyTimeoutMillis(
            final int busyTimeoutMillis) {
        walBackpressureBusyTimeoutMillis = busyTimeoutMillis;
        return this;
    }

    /**
     * Builds the storage service.
     *
     * @return storage service
     */
    public StorageService<K, V> build() {
        final Directory validatedDirectoryFacade = Vldtn.requireNonNull(
                directoryFacade, "directoryFacade");
        final KeyToSegmentMap<K> validatedKeyToSegmentMap = Vldtn
                .requireNonNull(keyToSegmentMap, "keyToSegmentMap");
        final SegmentRegistry<K, V> validatedSegmentRegistry = Vldtn
                .requireNonNull(segmentRegistry, "segmentRegistry");
        final BusyRetryPolicy storageCleanupRetryPolicy =
                new BusyRetryPolicy(Vldtn.requireNonNull(
                        storageCleanupBusyBackoffMillis,
                        "storageCleanupBusyBackoffMillis"),
                        Vldtn.requireNonNull(storageCleanupBusyTimeoutMillis,
                                "storageCleanupBusyTimeoutMillis"),
                        "Storage cleanup operation");
        final BusyRetryPolicy walBackpressureRetryPolicy =
                new BusyRetryPolicy(Vldtn.requireNonNull(
                        walBackpressureBusyBackoffMillis,
                        "walBackpressureBusyBackoffMillis"),
                        Vldtn.requireNonNull(walBackpressureBusyTimeoutMillis,
                                "walBackpressureBusyTimeoutMillis"),
                        "WAL backpressure operation");
        final RecoverySegmentDirectoryInspector<K> segmentDirectoryInspector =
                new RecoverySegmentDirectoryInspector<>(
                        validatedDirectoryFacade, validatedKeyToSegmentMap);
        final OrphanedSegmentDirectoryRemover<K, V> orphanedSegmentDirectoryRemover =
                new OrphanedSegmentDirectoryRemover<>(
                        validatedSegmentRegistry,
                        storageCleanupRetryPolicy);
        return new StorageService<>(segmentDirectoryInspector,
                orphanedSegmentDirectoryRemover,
                new IndexConsistencyCoordinator<>(
                        validatedKeyToSegmentMap,
                        validatedSegmentRegistry,
                        segmentDirectoryInspector,
                        orphanedSegmentDirectoryRemover),
                walBackpressureRetryPolicy);
    }
}
