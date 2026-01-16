package org.hestiastore.index.segment;

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
    /**
     * Creates a supplier for segment-related data structures.
     *
     * @param segmentFiles segment file access wrapper
     * @param segmentConf segment configuration
     */
    public SegmentDataSupplier(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf) {
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        this.segmentConf = Vldtn.requireNonNull(segmentConf, "segmentConf");
    }

    /**
     * Creates a new Bloom filter instance for the segment.
     *
     * @return Bloom filter instance
     */
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

    /**
     * Creates a new scarce index instance for the segment.
     *
     * @return scarce index instance
     */
    public ScarceSegmentIndex<K> getScarceIndex() {
        return ScarceSegmentIndex.<K>builder()//
                .withAsyncDirectory(segmentFiles.getAsyncDirectory())//
                .withFileName(segmentFiles.getScarceFileName())//
                .withKeyTypeDescriptor(segmentFiles.getKeyTypeDescriptor())//
                .withDiskIoBufferSize(segmentConf.getDiskIoBufferSize())//
                .build();
    }

}
