package org.hestiastore.index.segmentindex.core.storage;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
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
    private TypeDescriptor<K> keyTypeDescriptor;
    private IndexRetryPolicy retryPolicy;

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
     * Sets the key descriptor needed for segment consistency validation.
     *
     * @param keyTypeDescriptor key type descriptor
     * @return this builder
     */
    public StorageServiceBuilder<K, V> withKeyTypeDescriptor(
            final TypeDescriptor<K> keyTypeDescriptor) {
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        return this;
    }

    /**
     * Sets the retry policy used for transient busy cleanup states.
     *
     * @param retryPolicy retry policy
     * @return this builder
     */
    public StorageServiceBuilder<K, V> withRetryPolicy(
            final IndexRetryPolicy retryPolicy) {
        this.retryPolicy = Vldtn.requireNonNull(retryPolicy, "retryPolicy");
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
        final TypeDescriptor<K> validatedKeyTypeDescriptor = Vldtn
                .requireNonNull(keyTypeDescriptor, "keyTypeDescriptor");
        final IndexRetryPolicy validatedRetryPolicy = Vldtn
                .requireNonNull(retryPolicy, "retryPolicy");
        final RecoverySegmentDirectoryInspector<K> segmentDirectoryInspector =
                new RecoverySegmentDirectoryInspector<>(
                        validatedDirectoryFacade, validatedKeyToSegmentMap);
        final OrphanedSegmentDirectoryRemover<K, V> orphanedSegmentDirectoryRemover =
                new OrphanedSegmentDirectoryRemover<>(
                        validatedSegmentRegistry, validatedRetryPolicy);
        return new StorageServiceImpl<>(segmentDirectoryInspector,
                orphanedSegmentDirectoryRemover,
                new IndexConsistencyCoordinator<>(
                        validatedKeyToSegmentMap::validateUniqueSegmentIds,
                        segmentFilter -> new IndexConsistencyChecker<>(
                                validatedKeyToSegmentMap,
                                validatedSegmentRegistry,
                                validatedKeyTypeDescriptor, segmentFilter)
                                .checkAndRepairConsistency(),
                        () -> segmentDirectoryInspector
                                .discoverOrphanedSegmentDirectories()
                                .forEach(orphanedSegmentDirectoryRemover::remove),
                        segmentDirectoryInspector::hasSegmentLockFile),
                validatedRetryPolicy);
    }
}
