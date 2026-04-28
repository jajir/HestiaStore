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
import org.hestiastore.index.segment.SegmentFullWriterTx;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentMaintenancePolicy;
import org.hestiastore.index.segment.SegmentMaintenancePolicyThreshold;
import org.hestiastore.index.segment.SegmentRuntimeLimits;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRuntimeConfiguration;

/**
 * Builds segment instances and writer transactions with shared configuration.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentFactory<K, V>
        implements SegmentBuildService<K, V>,
        PreparedSegmentWriterFactory<K, V>, SegmentRuntimeTuner {

    private final Directory directoryFacade;
    private final TypeDescriptor<K> keyTypeDescriptor;
    private final TypeDescriptor<V> valueTypeDescriptor;
    private final IndexConfiguration<K, V> conf;
    private final IndexRuntimeConfiguration<K, V> runtimeConfiguration;
    private final ExecutorService stableSegmentMaintenanceExecutor;
    private volatile SegmentRuntimeLimits runtimeLimits;

    /**
     * Creates a factory for building segments with shared configuration.
     *
     * @param directoryFacade            base directory for segment storage
     * @param keyTypeDescriptor          key type descriptor
     * @param valueTypeDescriptor        value type descriptor
     * @param conf                       persisted index configuration
     * @param runtimeConfiguration       resolved runtime configuration
     * @param segmentMaintenanceExecutor executor for stable segment
     *                                   maintenance tasks
     */
    SegmentFactory(final Directory directoryFacade,
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
        this.runtimeLimits = configuredRuntimeLimitsIfValid();
    }

    /**
     * Builds a new segment instance for the given id.
     *
     * @param segmentId segment id
     * @return build result with the new segment or BUSY status
     * @throws RuntimeException when segment directory open/build fails
     */
    @Override
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
    private SegmentBuilder<K, V> newSegmentBuilder(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        final SegmentRuntimeLimits limits = resolveRuntimeLimits();
        final Directory segmentDirectory = openSegmentDirectory(segmentId);
        final boolean backgroundMaintenanceEnabled = Boolean.TRUE
                .equals(conf.maintenance().backgroundAutoEnabled());
        final SegmentMaintenancePolicy<K, V> stableSegmentMaintenancePolicy = backgroundMaintenanceEnabled
                ? new SegmentMaintenancePolicyThreshold<>(
                        limits.maxNumberOfKeysInSegmentCache(),
                        limits.maxNumberOfKeysInSegmentWriteCache(),
                        conf.segment().deltaCacheFileLimit())
                : SegmentMaintenancePolicy.none();
        final List<Supplier<? extends ChunkFilter>> encodingChunkFilters = resolveEncodingChunkFilterSuppliers();
        final List<Supplier<? extends ChunkFilter>> decodingChunkFilters = resolveDecodingChunkFilterSuppliers();
        return Segment.<K, V>builder(segmentDirectory)//
                .withId(segmentId)//
                .withDirectoryLockingEnabled(true)//
                .withKeyTypeDescriptor(keyTypeDescriptor)//
                .withMaintenanceExecutor(stableSegmentMaintenanceExecutor)//
                .withLoggingContextIndexName(
                        Boolean.TRUE.equals(conf.logging().contextEnabled())
                                ? conf.identity().name()
                                : null)//
                .withMaintenancePolicy(stableSegmentMaintenancePolicy)//
                .withMaxNumberOfKeysInSegmentWriteCache(
                        limits.maxNumberOfKeysInSegmentWriteCache())//
                .withMaxNumberOfKeysInSegmentCache(
                        limits.maxNumberOfKeysInSegmentCache())//
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(
                        limits.maxNumberOfKeysInSegmentWriteCacheDuringMaintenance())//
                .withMaxNumberOfKeysInSegmentChunk(
                        conf.segment().chunkKeyLimit())//
                .withMaxNumberOfDeltaCacheFiles(
                        conf.segment().deltaCacheFileLimit())//
                .withValueTypeDescriptor(valueTypeDescriptor)//
                .withBloomFilterNumberOfHashFunctions(
                        conf.bloomFilter().hashFunctions())//
                .withBloomFilterIndexSizeInBytes(
                        conf.bloomFilter().indexSizeBytes())//
                .withBloomFilterProbabilityOfFalsePositive(
                        conf.bloomFilter().falsePositiveProbability())//
                .withDiskIoBufferSize(conf.io().diskBufferSizeBytes())//
                .withEncodingChunkFilterSuppliers(encodingChunkFilters)//
                .withDecodingChunkFilterSuppliers(decodingChunkFilters);
    }

    /**
     * Opens a synchronous bulk writer transaction for the provided segment id.
     *
     * @param segmentId segment id to materialize
     * @return full writer transaction for building the segment files
     */
    @Override
    public SegmentFullWriterTx<K, V> openWriterTx(final SegmentId segmentId) {
        return newSegmentBuilder(segmentId).openWriterTx();
    }

    /**
     * Updates runtime-only segment limits used for newly loaded segments.
     *
     * @param runtimeLimits validated runtime limits for future materialization
     */
    @Override
    public void updateRuntimeLimits(final SegmentRuntimeLimits runtimeLimits) {
        this.runtimeLimits = Vldtn.requireNonNull(runtimeLimits,
                "runtimeLimits");
    }

    private Directory openSegmentDirectory(final SegmentId segmentId) {
        return directoryFacade.openSubDirectory(segmentId.getName());
    }

    private SegmentRuntimeLimits resolveRuntimeLimits() {
        final SegmentRuntimeLimits currentLimits = runtimeLimits;
        if (currentLimits != null) {
            return currentLimits;
        }
        final SegmentRuntimeLimits configuredLimits = configuredRuntimeLimits();
        if (runtimeLimits == null) {
            runtimeLimits = configuredLimits;
        }
        return runtimeLimits;
    }

    private SegmentRuntimeLimits configuredRuntimeLimitsIfValid() {
        try {
            return configuredRuntimeLimits();
        } catch (final IllegalArgumentException ex) {
            return null;
        }
    }

    private SegmentRuntimeLimits configuredRuntimeLimits() {
        return new SegmentRuntimeLimits(
                toIntOrZero(conf.segment().cacheKeyLimit()),
                toIntOrZero(conf.writePath().segmentWriteCacheKeyLimit()),
                toIntOrZero(conf.writePath()
                        .segmentWriteCacheKeyLimitDuringMaintenance()));
    }

    private int toIntOrZero(final Integer value) {
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
