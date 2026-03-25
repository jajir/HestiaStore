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
    private volatile int runtimeMaxNumberOfKeysInActivePartition;
    private volatile int runtimeMaxNumberOfKeysInPartitionBuffer;

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
        this.runtimeMaxNumberOfKeysInActivePartition = toIntOrZero(
                conf.getMaxNumberOfKeysInActivePartition());
        this.runtimeMaxNumberOfKeysInPartitionBuffer = toIntOrZero(
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
        final int activePartitionKeyLimit = Vldtn
                .requireGreaterThanZero(runtimeMaxNumberOfKeysInActivePartition,
                        "maxNumberOfKeysInActivePartition");
        final int partitionBufferKeyLimit = Vldtn
                .requireGreaterThanZero(runtimeMaxNumberOfKeysInPartitionBuffer,
                        "maxNumberOfKeysInPartitionBuffer");
        if (partitionBufferKeyLimit <= activePartitionKeyLimit) {
            throw new IllegalArgumentException(
                    "maxNumberOfKeysInPartitionBuffer must be greater than maxNumberOfKeysInActivePartition");
        }
        final Directory segmentDirectory = openSegmentDirectory(segmentId);
        final boolean backgroundMaintenanceEnabled = Boolean.TRUE
                .equals(conf.isBackgroundMaintenanceAutoEnabled());
        final SegmentMaintenancePolicy<K, V> stableSegmentMaintenancePolicy = backgroundMaintenanceEnabled
                ? new SegmentMaintenancePolicyThreshold<>(
                        segmentCacheKeyLimit, activePartitionKeyLimit,
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
                // Stable segment builder still exposes segment-local
                // write-buffer limit names.
                .withMaxNumberOfKeysInSegmentWriteCache(
                        mapActivePartitionKeyLimitToStableWriteBuffer(
                                activePartitionKeyLimit))//
                .withMaxNumberOfKeysInSegmentCache(
                        segmentCacheKeyLimit)//
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(
                        mapPartitionBufferKeyLimitToStableDrainWriteBuffer(
                                partitionBufferKeyLimit))//
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
     * @param maxNumberOfKeysInActivePartition active overlay threshold
     * @param maxNumberOfKeysInPartitionBuffer partition buffer threshold
     */
    public void updateRuntimeLimits(final int maxNumberOfKeysInSegmentCache,
            final int maxNumberOfKeysInActivePartition,
            final int maxNumberOfKeysInPartitionBuffer) {
        Vldtn.requireGreaterThanZero(maxNumberOfKeysInSegmentCache,
                "maxNumberOfKeysInSegmentCache");
        Vldtn.requireGreaterThanZero(maxNumberOfKeysInActivePartition,
                "maxNumberOfKeysInActivePartition");
        Vldtn.requireGreaterThanZero(maxNumberOfKeysInPartitionBuffer,
                "maxNumberOfKeysInPartitionBuffer");
        if (maxNumberOfKeysInPartitionBuffer
                <= maxNumberOfKeysInActivePartition) {
            throw new IllegalArgumentException(
                    "maxNumberOfKeysInPartitionBuffer must be greater than maxNumberOfKeysInActivePartition");
        }
        this.runtimeMaxNumberOfKeysInSegmentCache = maxNumberOfKeysInSegmentCache;
        this.runtimeMaxNumberOfKeysInActivePartition = maxNumberOfKeysInActivePartition;
        this.runtimeMaxNumberOfKeysInPartitionBuffer = maxNumberOfKeysInPartitionBuffer;
    }

    private Directory openSegmentDirectory(final SegmentId segmentId) {
        return directoryFacade.openSubDirectory(segmentId.getName());
    }

    private void refreshRuntimeLimitsFromConfigurationIfInvalid() {
        if (runtimeMaxNumberOfKeysInSegmentCache > 0
                && runtimeMaxNumberOfKeysInActivePartition > 0
                && runtimeMaxNumberOfKeysInPartitionBuffer > runtimeMaxNumberOfKeysInActivePartition) {
            return;
        }
        final int configuredCache = toIntOrZero(
                conf.getMaxNumberOfKeysInSegmentCache());
        final int configuredActivePartition = toIntOrZero(
                conf.getMaxNumberOfKeysInActivePartition());
        final int configuredPartitionBuffer = toIntOrZero(
                conf.getMaxNumberOfKeysInPartitionBuffer());
        if (configuredCache > 0) {
            runtimeMaxNumberOfKeysInSegmentCache = configuredCache;
        }
        if (configuredActivePartition > 0) {
            runtimeMaxNumberOfKeysInActivePartition = configuredActivePartition;
        }
        if (configuredPartitionBuffer > 0) {
            runtimeMaxNumberOfKeysInPartitionBuffer = configuredPartitionBuffer;
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

    private int mapActivePartitionKeyLimitToStableWriteBuffer(
            final int activePartitionKeyLimit) {
        return activePartitionKeyLimit;
    }

    private int mapPartitionBufferKeyLimitToStableDrainWriteBuffer(
            final int partitionBufferKeyLimit) {
        return partitionBufferKeyLimit;
    }
}
