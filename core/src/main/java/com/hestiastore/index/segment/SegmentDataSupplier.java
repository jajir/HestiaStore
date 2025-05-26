package com.hestiastore.index.segment;

import java.util.Objects;

import com.hestiastore.index.bloomfilter.BloomFilter;
import com.hestiastore.index.scarceindex.ScarceIndex;

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
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        this.segmentConf = Objects.requireNonNull(segmentConf);
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
                .withDirectory(segmentFiles.getDirectory())
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

    public ScarceIndex<K> getScarceIndex() {
        return ScarceIndex.<K>builder()//
                .withDirectory(segmentFiles.getDirectory())//
                .withFileName(segmentFiles.getScarceFileName())//
                .withKeyTypeDescriptor(segmentFiles.getKeyTypeDescriptor())//
                .withDiskIoBufferSize(segmentConf.getDiskIoBufferSize())//
                .build();
    }

}
