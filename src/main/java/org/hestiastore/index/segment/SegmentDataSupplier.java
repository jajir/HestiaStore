package org.hestiastore.index.segment;

import java.util.Objects;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bloomfilter.BloomFilter;
import org.hestiastore.index.scarceindex.ScarceSegmentIndex;

/**
 * When any getter is called than new instance of object is created and
 * returned.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public final class SegmentDataSupplier<K, V> {

    private final SegmentFiles<K, V> segmentFiles;
    private final SegmentConf segmentConf;
    private final SegmentPropertiesManager segmentPropertiesManager;

    public SegmentDataSupplier(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf,
            final SegmentPropertiesManager segmentPropertiesManager) {
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        this.segmentConf = Vldtn.requireNonNull(segmentConf, "segmentConf");
        this.segmentPropertiesManager = Objects
                .requireNonNull(segmentPropertiesManager);
    }

    public SegmentDeltaCache<K, V> getSegmentDeltaCache() {
        return new SegmentDeltaCache<>(segmentFiles.getKeyTypeDescriptor(),
                segmentFiles, segmentPropertiesManager);
    }

    public BloomFilter<K> getBloomFilter() {
        return BloomFilter.<K>builder()
                .withBloomFilterFileName(segmentFiles.getBloomFilterFileName())
                .withConvertorToBytes(segmentFiles.getKeyTypeDescriptor()
                        .getConvertorToBytes())
                .withAsyncDirectory(segmentFiles.getAsyncDirectory())
                .withIndexSizeInBytes(
                        segmentConf.getBloomFilterIndexSizeInBytes())
                .withNumberOfHashFunctions(
                        segmentConf.getBloomFilterNumberOfHashFunctions())
                .withProbabilityOfFalsePositive(
                        segmentConf.getBloomFilterProbabilityOfFalsePositive())
                .withRelatedObjectName(segmentFiles.getSegmentIdName())
                .withDiskIoBufferSize(segmentConf.getDiskIoBufferSize())
                .build();
    }

    public ScarceSegmentIndex<K> getScarceIndex() {
        return ScarceSegmentIndex.<K>builder()//
                .withAsyncDirectory(segmentFiles.getAsyncDirectory())//
                .withFileName(segmentFiles.getScarceFileName())//
                .withKeyTypeDescriptor(segmentFiles.getKeyTypeDescriptor())//
                .withDiskIoBufferSize(segmentConf.getDiskIoBufferSize())//
                .build();
    }

}
