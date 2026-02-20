package org.hestiastore.index.segmentregistry;

import java.util.concurrent.ExecutorService;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentBuildResult;
import org.hestiastore.index.segment.SegmentBuilder;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentMaintenancePolicy;
import org.hestiastore.index.segment.SegmentMaintenancePolicyThreshold;
import org.hestiastore.index.segmentindex.IndexConfiguration;

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
    private final ExecutorService maintenanceExecutor;
    private final Runnable compactionRequestListener;
    private volatile int runtimeMaxNumberOfKeysInSegmentCache;
    private volatile int runtimeMaxNumberOfKeysInSegmentWriteCache;
    private volatile int runtimeMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance;

    /**
     * Creates a factory for building segments with shared configuration.
     *
     * @param directoryFacade    base directory for segment storage
     * @param keyTypeDescriptor  key type descriptor
     * @param valueTypeDescriptor value type descriptor
     * @param conf               index configuration
     * @param maintenanceExecutor executor for segment maintenance tasks
     */
    public SegmentFactory(final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf,
            final ExecutorService maintenanceExecutor) {
        this(directoryFacade, keyTypeDescriptor, valueTypeDescriptor, conf,
                maintenanceExecutor, () -> {});
    }

    /**
     * Creates a factory for building segments with shared configuration.
     *
     * @param directoryFacade    base directory for segment storage
     * @param keyTypeDescriptor  key type descriptor
     * @param valueTypeDescriptor value type descriptor
     * @param conf               index configuration
     * @param maintenanceExecutor executor for segment maintenance tasks
     * @param compactionRequestListener callback invoked when segment compaction
     *                                  is accepted
     */
    public SegmentFactory(final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> conf,
            final ExecutorService maintenanceExecutor,
            final Runnable compactionRequestListener) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.maintenanceExecutor = Vldtn.requireNonNull(maintenanceExecutor,
                "maintenanceExecutor");
        this.compactionRequestListener = Vldtn
                .requireNonNull(compactionRequestListener,
                        "compactionRequestListener");
        this.runtimeMaxNumberOfKeysInSegmentCache = toIntOrZero(
                conf.getMaxNumberOfKeysInSegmentCache());
        this.runtimeMaxNumberOfKeysInSegmentWriteCache = toIntOrZero(
                conf.getMaxNumberOfKeysInSegmentWriteCache());
        this.runtimeMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance = toIntOrZero(
                conf.getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance());
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
        final int maxNumberOfKeysInSegmentCache = Vldtn.requireGreaterThanZero(
                runtimeMaxNumberOfKeysInSegmentCache,
                "maxNumberOfKeysInSegmentCache");
        final int maxNumberOfKeysInSegmentWriteCache = Vldtn
                .requireGreaterThanZero(runtimeMaxNumberOfKeysInSegmentWriteCache,
                        "maxNumberOfKeysInSegmentWriteCache");
        final int maxNumberOfKeysInSegmentWriteCacheDuringMaintenance = Vldtn
                .requireGreaterThanZero(
                        runtimeMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance,
                        "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance");
        if (maxNumberOfKeysInSegmentWriteCacheDuringMaintenance
                <= maxNumberOfKeysInSegmentWriteCache) {
            throw new IllegalArgumentException(
                    "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance must be greater than maxNumberOfKeysInSegmentWriteCache");
        }
        final Directory segmentDirectory = openSegmentDirectory(segmentId);
        final SegmentMaintenancePolicy<K, V> maintenancePolicy = Boolean.TRUE
                .equals(conf.isSegmentMaintenanceAutoEnabled())
                        ? new SegmentMaintenancePolicyThreshold<>(
                                maxNumberOfKeysInSegmentCache,
                                maxNumberOfKeysInSegmentWriteCache,
                                conf.getMaxNumberOfDeltaCacheFiles())
                        : SegmentMaintenancePolicy.none();
        return Segment.<K, V>builder(segmentDirectory)//
                .withId(segmentId)//
                .withDirectoryLockingEnabled(true)//
                .withKeyTypeDescriptor(keyTypeDescriptor)//
                .withMaintenanceExecutor(maintenanceExecutor)//
                .withMaintenancePolicy(maintenancePolicy)//
                .withCompactionRequestListener(compactionRequestListener)//
                .withMaxNumberOfKeysInSegmentWriteCache(
                        maxNumberOfKeysInSegmentWriteCache)//
                .withMaxNumberOfKeysInSegmentCache(
                        maxNumberOfKeysInSegmentCache)//
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(
                        maxNumberOfKeysInSegmentWriteCacheDuringMaintenance)//
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
                .withEncodingChunkFilters(conf.getEncodingChunkFilters())//
                .withDecodingChunkFilters(conf.getDecodingChunkFilters());
    }

    /**
     * Updates runtime-only segment cache/write thresholds used for newly loaded
     * segments.
     *
     * @param maxNumberOfKeysInSegmentCache segment-cache threshold
     * @param maxNumberOfKeysInSegmentWriteCache write-cache threshold
     * @param maxNumberOfKeysInSegmentWriteCacheDuringMaintenance write-cache
     *        threshold while maintenance is running
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
        final int configuredWrite = toIntOrZero(
                conf.getMaxNumberOfKeysInSegmentWriteCache());
        final int configuredWriteDuringMaintenance = toIntOrZero(
                conf.getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance());
        if (configuredCache > 0) {
            runtimeMaxNumberOfKeysInSegmentCache = configuredCache;
        }
        if (configuredWrite > 0) {
            runtimeMaxNumberOfKeysInSegmentWriteCache = configuredWrite;
        }
        if (configuredWriteDuringMaintenance > 0) {
            runtimeMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance = configuredWriteDuringMaintenance;
        }
    }

    private static int toIntOrZero(final Integer value) {
        if (value == null) {
            return 0;
        }
        return value.intValue();
    }
}
