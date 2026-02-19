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
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.conf = Vldtn.requireNonNull(conf, "conf");
        this.maintenanceExecutor = Vldtn.requireNonNull(maintenanceExecutor,
                "maintenanceExecutor");
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
        final Directory segmentDirectory = openSegmentDirectory(segmentId);
        final SegmentMaintenancePolicy<K, V> maintenancePolicy = Boolean.TRUE
                .equals(conf.isSegmentMaintenanceAutoEnabled())
                        ? new SegmentMaintenancePolicyThreshold<>(
                                conf.getMaxNumberOfKeysInSegmentCache(),
                                conf.getMaxNumberOfKeysInSegmentWriteCache(),
                                conf.getMaxNumberOfDeltaCacheFiles())
                        : SegmentMaintenancePolicy.none();
        return Segment.<K, V>builder(segmentDirectory)//
                .withId(segmentId)//
                .withDirectoryLockingEnabled(true)//
                .withKeyTypeDescriptor(keyTypeDescriptor)//
                .withMaintenanceExecutor(maintenanceExecutor)//
                .withMaintenancePolicy(maintenancePolicy)//
                .withMaxNumberOfKeysInSegmentWriteCache(
                        conf.getMaxNumberOfKeysInSegmentWriteCache().intValue())//
                .withMaxNumberOfKeysInSegmentCache(
                        conf.getMaxNumberOfKeysInSegmentCache())//
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(conf
                        .getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance()
                        .intValue())//
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

    private Directory openSegmentDirectory(final SegmentId segmentId) {
        return directoryFacade.openSubDirectory(segmentId.getName());
    }
}
