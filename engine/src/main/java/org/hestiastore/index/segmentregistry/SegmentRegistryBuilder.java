package org.hestiastore.index.segmentregistry;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstorecache.ChunkStoreCache;
import org.hestiastore.index.chunkstorecache.LruChunkStoreCache;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;

/**
 * Builder for {@link SegmentRegistry} instances.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentRegistryBuilder<K, V> {

    private static final int REGISTRY_CLOSE_TIMEOUT_MILLIS = (int) TimeUnit.MINUTES
            .toMillis(5);

    private Directory directoryFacade;
    private TypeDescriptor<K> keyTypeDescriptor;
    private TypeDescriptor<V> valueTypeDescriptor;
    private EffectiveIndexConfiguration<K, V> conf;
    private ExecutorService segmentMaintenanceExecutor;
    private ExecutorService registryMaintenanceExecutor;
    private SegmentIdAllocator segmentIdAllocator;
    private ChunkStoreCache<K, V> chunkStoreCache = new LruChunkStoreCache<>(0);

    SegmentRegistryBuilder() {
    }

    /**
     * Sets the base directory for segments.
     *
     * @param directoryFacade base directory
     * @return this builder
     */
    public SegmentRegistryBuilder<K, V> withDirectoryFacade(
            final Directory directoryFacade) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        return this;
    }

    /**
     * Sets the key type descriptor.
     *
     * @param keyTypeDescriptor key type descriptor
     * @return this builder
     */
    public SegmentRegistryBuilder<K, V> withKeyTypeDescriptor(
            final TypeDescriptor<K> keyTypeDescriptor) {
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        return this;
    }

    /**
     * Sets the value type descriptor.
     *
     * @param valueTypeDescriptor value type descriptor
     * @return this builder
     */
    public SegmentRegistryBuilder<K, V> withValueTypeDescriptor(
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        return this;
    }

    /**
     * Sets the index configuration.
     *
     * @param conf index configuration
     * @return this builder
     */
    public SegmentRegistryBuilder<K, V> withConfiguration(
            final EffectiveIndexConfiguration<K, V> conf) {
        this.conf = Vldtn.requireNonNull(conf, "conf");
        return this;
    }

    /**
     * Sets the segment maintenance executor.
     *
     * @param segmentMaintenanceExecutor segment maintenance executor
     * @return this builder
     */
    public SegmentRegistryBuilder<K, V> withSegmentMaintenanceExecutor(
            final ExecutorService segmentMaintenanceExecutor) {
        this.segmentMaintenanceExecutor = Vldtn
                .requireNonNull(segmentMaintenanceExecutor,
                        "segmentMaintenanceExecutor");
        return this;
    }

    /**
     * Sets the registry maintenance executor used for load/unload operations.
     *
     * @param registryMaintenanceExecutor registry maintenance executor
     * @return this builder
     */
    public SegmentRegistryBuilder<K, V> withRegistryMaintenanceExecutor(
            final ExecutorService registryMaintenanceExecutor) {
        this.registryMaintenanceExecutor = Vldtn
                .requireNonNull(registryMaintenanceExecutor,
                        "registryMaintenanceExecutor");
        return this;
    }

    /**
     * Sets the segment id allocator used for newly created segment ids.
     *
     * @param segmentIdAllocator allocator for segment ids
     * @return this builder
     */
    SegmentRegistryBuilder<K, V> withSegmentIdAllocator(
            final SegmentIdAllocator segmentIdAllocator) {
        this.segmentIdAllocator = Vldtn.requireNonNull(segmentIdAllocator,
                "segmentIdAllocator");
        return this;
    }

    /**
     * Sets the index-scoped parsed chunk page cache.
     *
     * @param chunkStoreCache parsed chunk page cache
     * @return this builder
     */
    public SegmentRegistryBuilder<K, V> withChunkStoreCache(
            final ChunkStoreCache<K, V> chunkStoreCache) {
        this.chunkStoreCache = Vldtn.requireNonNull(chunkStoreCache,
                "chunkStoreCache");
        return this;
    }

    /**
     * Builds a registry with the configured defaults and overrides.
     *
     * @return registry instance
     */
    public SegmentRegistry<K, V> build() {
        final Directory resolvedDirectory = Vldtn.requireNonNull(
                directoryFacade, "directoryFacade");
        final TypeDescriptor<K> resolvedKeyDescriptor = Vldtn.requireNonNull(
                keyTypeDescriptor, "keyTypeDescriptor");
        final TypeDescriptor<V> resolvedValueDescriptor = Vldtn.requireNonNull(
                valueTypeDescriptor, "valueTypeDescriptor");
        final EffectiveIndexConfiguration<K, V> resolvedConf = Vldtn
                .requireNonNull(conf, "conf");
        final ExecutorService resolvedSegmentMaintenanceExecutor = Vldtn
                .requireNonNull(segmentMaintenanceExecutor,
                        "segmentMaintenanceExecutor");
        final ExecutorService resolvedRegistryMaintenanceExecutor = Vldtn
                .requireNonNull(registryMaintenanceExecutor,
                        "registryMaintenanceExecutor");
        final int maxSegments = Vldtn
                .requireNonNull(resolvedConf.segment().cachedSegmentLimit(),
                        "maxNumberOfSegmentsInCache")
                .intValue();
        final int maxNumberOfSegmentsInCache = Vldtn.requireGreaterThanZero(
                maxSegments, "maxNumberOfSegmentsInCache");
        final int busyBackoffMillis = resolvedConf.maintenance()
                .busyBackoffMillis();
        final int busyTimeoutMillis = resolvedConf.maintenance()
                .busyTimeoutMillis();
        final boolean automaticMaintenanceEnabled = resolvedConf.maintenance()
                .backgroundAutoEnabled();
        final SegmentFactory<K, V> resolvedFactory = new SegmentFactory<>(
                resolvedDirectory, resolvedKeyDescriptor,
                resolvedValueDescriptor, resolvedConf,
                resolvedSegmentMaintenanceExecutor, chunkStoreCache);
        final SegmentIdAllocator resolvedAllocator = segmentIdAllocator == null
                ? new DirectorySegmentIdAllocator(resolvedDirectory)
                : segmentIdAllocator;
        final SegmentRegistryFileSystem resolvedFileSystem = new SegmentRegistryFileSystem(
                resolvedDirectory);
        // SegmentIndex key-map bootstraps the first logical segment with id 0.
        // Ensure its directory exists so registry loads do not fail on a fresh index.
        resolvedFileSystem.ensureSegmentDirectory(SegmentId.of(0));
        final BusyRetryPolicy resolvedCloseRetryPolicy = new BusyRetryPolicy(
                busyBackoffMillis, busyTimeoutMillis,
                "Maintenance operation");
        final BusyRetryPolicy resolvedBlockingRetryPolicy = new BusyRetryPolicy(
                busyBackoffMillis, busyTimeoutMillis,
                "Segment access operation");
        final BusyRetryPolicy resolvedRegistryCloseRetryPolicy = new BusyRetryPolicy(
                busyBackoffMillis, REGISTRY_CLOSE_TIMEOUT_MILLIS,
                "Maintenance operation");
        final SegmentRegistryStateMachine gate = new SegmentRegistryStateMachine();
        final SegmentLoadCloseOperations<K, V> segmentOperations = new SegmentLoadCloseOperations<>(
                resolvedFactory, resolvedFileSystem, resolvedCloseRetryPolicy,
                gate);
        final SegmentUnloadEligibility unloadEligibility = new SegmentUnloadEligibility(
                gate);
        final SegmentRegistryCache<K, V> cache = new SegmentRegistryCache<>(
                maxNumberOfSegmentsInCache, segmentOperations,
                unloadEligibility,
                resolvedRegistryMaintenanceExecutor);
        return new SegmentRegistryImpl<>(resolvedAllocator, resolvedFileSystem,
                cache, resolvedRegistryCloseRetryPolicy, gate, resolvedFactory,
                resolvedFactory, resolvedBlockingRetryPolicy,
                automaticMaintenanceEnabled);
    }

}
