package org.hestiastore.index.segmentregistry;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentBuildResult;
import org.hestiastore.index.segment.SegmentBuilder;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentMaintenancePolicy;
import org.hestiastore.index.segment.SegmentMaintenancePolicyThreshold;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRuntimeConfiguration;

/**
 * Builds segment instances and writer transactions with shared configuration.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentFactory<K, V> {

    private final Directory directoryFacade;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final IndexConfiguration<K, V> conf;
    private final IndexRuntimeConfiguration<K, V> runtimeConfiguration;
    private final ExecutorService stableSegmentMaintenanceExecutor;
    private volatile int runtimeMaxNumberOfKeysInSegmentCache;
    private volatile int runtimeMaxNumberOfKeysInSegmentWriteCache;
    private volatile int runtimeMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance;

    /**
     * Creates a factory for building segments with shared configuration.
     *
     * <p>
     * This overload resolves chunk filter suppliers from
     * {@link IndexConfiguration} using the built-in chunk filter provider
     * registry. When the configuration contains custom provider ids, use
     * {@link #withRuntimeConfiguration(Directory, TypeDescriptor, TypeDescriptor, IndexConfiguration, IndexRuntimeConfiguration, ExecutorService)}
     * to pass an explicitly resolved runtime configuration.
     * </p>
     *
     * @param directoryFacade    base directory for segment storage
     * @param keyTypeDescriptor  key type descriptor
     * @param valueTypeDescriptor value type descriptor
     * @param conf               index configuration
     * @param segmentMaintenanceExecutor executor for stable segment
     *                                 maintenance tasks
     */
    public SegmentFactory(final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf,
            final ExecutorService segmentMaintenanceExecutor) {
        this(directoryFacade, keyTypeDescriptor, valueTypeDescriptor, conf,
                conf.resolveRuntimeConfiguration(), segmentMaintenanceExecutor);
    }

    private SegmentFactory(final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf,
            final IndexRuntimeConfiguration<K, V> runtimeConfiguration,
            final ExecutorService segmentMaintenanceExecutor) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.runtimeConfiguration = Vldtn.requireNonNull(runtimeConfiguration,
                "runtimeConfiguration");
        this.stableSegmentMaintenanceExecutor = Vldtn
                .requireNonNull(segmentMaintenanceExecutor,
                        "segmentMaintenanceExecutor");
        this.runtimeMaxNumberOfKeysInSegmentCache = toIntOrZero(
                conf.getMaxNumberOfKeysInSegmentCache());
        this.runtimeMaxNumberOfKeysInSegmentWriteCache = toIntOrZero(
                conf.getMaxNumberOfKeysInActivePartition());
        this.runtimeMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance = toIntOrZero(
                conf.getMaxNumberOfKeysInPartitionBuffer());
    }

    /**
     * Creates a segment factory bound to an explicit resolved runtime
     * configuration.
     *
     * @param <K> key type
     * @param <V> value type
     * @param directoryFacade base directory for segment storage
     * @param keyTypeDescriptor key type descriptor
     * @param valueTypeDescriptor value type descriptor
     * @param conf persisted index configuration
     * @param runtimeConfiguration resolved runtime configuration
     * @param segmentMaintenanceExecutor executor for stable segment
     *                                  maintenance tasks
     * @return segment factory using the supplied runtime configuration
     */
    public static <K, V> SegmentFactory<K, V> withRuntimeConfiguration(
            final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf,
            final IndexRuntimeConfiguration<K, V> runtimeConfiguration,
            final ExecutorService segmentMaintenanceExecutor) {
        return new SegmentFactory<>(directoryFacade, keyTypeDescriptor,
                valueTypeDescriptor, conf, runtimeConfiguration,
                segmentMaintenanceExecutor);
    }

    /**
     * Builds a new segment instance for the given id.
     *
     * @param segmentId segment id
     * @return build result with the new segment or BUSY status
     * @throws RuntimeException when segment directory open/build fails
     */
    public SegmentBuildResult<Segment<K, V>> buildSegment(
            final SegmentId segmentId) {
        return newSegmentBuilder(segmentId).build();
    }

    /**
     * Creates a configured builder for the provided segment id.
     *
     * @param segmentId segment id
     * @return configured segment builder
     */
    public SegmentBuilder<K, V> newSegmentBuilder(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        refreshRuntimeLimitsFromConfigurationIfInvalid();
        final int segmentCacheKeyLimit = Vldtn.requireGreaterThanZero(
                runtimeMaxNumberOfKeysInSegmentCache,
                "maxNumberOfKeysInSegmentCache");
        final int segmentWriteCacheKeyLimit = Vldtn.requireGreaterThanZero(
                runtimeMaxNumberOfKeysInSegmentWriteCache,
                "maxNumberOfKeysInSegmentWriteCache");
        final int maintenanceWriteCacheKeyLimit = Vldtn.requireGreaterThanZero(
                runtimeMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance,
                "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance");
        if (maintenanceWriteCacheKeyLimit <= segmentWriteCacheKeyLimit) {
            throw new IllegalArgumentException(
                    "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance must be greater than maxNumberOfKeysInSegmentWriteCache");
        }
        final Directory segmentDirectory = openSegmentDirectory(segmentId);
        final boolean backgroundMaintenanceEnabled = Boolean.TRUE
                .equals(conf.isBackgroundMaintenanceAutoEnabled());
        final SegmentMaintenancePolicy<K, V> stableSegmentMaintenancePolicy = backgroundMaintenanceEnabled
                ? new SegmentMaintenancePolicyThreshold<>(
                        segmentCacheKeyLimit, segmentWriteCacheKeyLimit,
                        conf.getMaxNumberOfDeltaCacheFiles())
                        : SegmentMaintenancePolicy.none();
        final List<Supplier<? extends ChunkFilter>> encodingChunkFilters = resolveEncodingChunkFilterSuppliers();
        final List<Supplier<? extends ChunkFilter>> decodingChunkFilters = resolveDecodingChunkFilterSuppliers();
        return Segment.<K, V>builder(segmentDirectory)//
                .withId(segmentId)//
                .withDirectoryLockingEnabled(true)//
                .withKeyTypeDescriptor(keyTypeDescriptor)//
                .withMaintenanceExecutor(stableSegmentMaintenanceExecutor)//
                .withLoggingContextIndexName(
                        Boolean.TRUE.equals(conf.isContextLoggingEnabled())
                                ? conf.getIndexName()
                                : null)//
                .withMaintenancePolicy(stableSegmentMaintenancePolicy)//
                .withMaxNumberOfKeysInSegmentWriteCache(
                        segmentWriteCacheKeyLimit)//
                .withMaxNumberOfKeysInSegmentCache(
                        segmentCacheKeyLimit)//
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(
                        maintenanceWriteCacheKeyLimit)//
                .withMaxNumberOfKeysInSegmentChunk(
                        conf.getMaxNumberOfKeysInSegmentChunk())//
                .withMaxNumberOfDeltaCacheFiles(
                        conf.getMaxNumberOfDeltaCacheFiles())//
                .withValueTypeDescriptor(valueTypeDescriptor)//
                .withBloomFilterNumberOfHashFunctions(
                        conf.getBloomFilterNumberOfHashFunctions())//
                .withBloomFilterIndexSizeInBytes(
                        conf.getBloomFilterIndexSizeInBytes())//
                .withBloomFilterProbabilityOfFalsePositive(
                        conf.getBloomFilterProbabilityOfFalsePositive())//
                .withDiskIoBufferSize(conf.getDiskIoBufferSize())//
                .withEncodingChunkFilterSuppliers(encodingChunkFilters)//
                .withDecodingChunkFilterSuppliers(decodingChunkFilters);
    }

    /**
     * Updates runtime-only segment limits used for newly loaded segments.
     *
     * @param maxNumberOfKeysInSegmentCache segment-cache threshold
     * @param maxNumberOfKeysInSegmentWriteCache segment write-cache threshold
     * @param maxNumberOfKeysInSegmentWriteCacheDuringMaintenance widened
     *                                                           write-cache
     *                                                           threshold used
     *                                                           during
     *                                                           maintenance
     */
    public void updateRuntimeLimits(final int maxNumberOfKeysInSegmentCache,
            final int maxNumberOfKeysInSegmentWriteCache,
            final int maxNumberOfKeysInSegmentWriteCacheDuringMaintenance) {
        Vldtn.requireGreaterThanZero(maxNumberOfKeysInSegmentCache,
                "maxNumberOfKeysInSegmentCache");
        Vldtn.requireGreaterThanZero(maxNumberOfKeysInSegmentWriteCache,
                "maxNumberOfKeysInSegmentWriteCache");
        Vldtn.requireGreaterThanZero(
                maxNumberOfKeysInSegmentWriteCacheDuringMaintenance,
                "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance");
        if (maxNumberOfKeysInSegmentWriteCacheDuringMaintenance
                <= maxNumberOfKeysInSegmentWriteCache) {
            throw new IllegalArgumentException(
                    "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance must be greater than maxNumberOfKeysInSegmentWriteCache");
        }
        this.runtimeMaxNumberOfKeysInSegmentCache = maxNumberOfKeysInSegmentCache;
        this.runtimeMaxNumberOfKeysInSegmentWriteCache = maxNumberOfKeysInSegmentWriteCache;
        this.runtimeMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance = maxNumberOfKeysInSegmentWriteCacheDuringMaintenance;
    }

    private Directory openSegmentDirectory(final SegmentId segmentId) {
        return directoryFacade.openSubDirectory(segmentId.getName());
    }

    private void refreshRuntimeLimitsFromConfigurationIfInvalid() {
        if (runtimeMaxNumberOfKeysInSegmentCache > 0
                && runtimeMaxNumberOfKeysInSegmentWriteCache > 0
                && runtimeMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance > runtimeMaxNumberOfKeysInSegmentWriteCache) {
            return;
        }
        final int configuredCache = toIntOrZero(
                conf.getMaxNumberOfKeysInSegmentCache());
        final int configuredSegmentWriteCache = toIntOrZero(
                conf.getMaxNumberOfKeysInActivePartition());
        final int configuredMaintenanceWriteCache = toIntOrZero(
                conf.getMaxNumberOfKeysInPartitionBuffer());
        if (configuredCache > 0) {
            runtimeMaxNumberOfKeysInSegmentCache = configuredCache;
        }
        if (configuredSegmentWriteCache > 0) {
            runtimeMaxNumberOfKeysInSegmentWriteCache = configuredSegmentWriteCache;
        }
        if (configuredMaintenanceWriteCache > 0) {
            runtimeMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance = configuredMaintenanceWriteCache;
        }
    }

    private static int toIntOrZero(final Integer value) {
        if (value == null) {
            return 0;
        }
        return value.intValue();
    }

    private List<Supplier<? extends ChunkFilter>> resolveEncodingChunkFilterSuppliers() {
        return runtimeConfiguration.getEncodingChunkFilterSuppliers();
    }

    private List<Supplier<? extends ChunkFilter>> resolveDecodingChunkFilterSuppliers() {
        return runtimeConfiguration.getDecodingChunkFilterSuppliers();
    }
}
