package com.coroptis.index.segment;

public class SegmentConf {

    private final long maxNumberOfKeysInSegmentDeltaCache;
    private final long maxNumberOfKeysInDeltaCacheDuringWriting;
    private final int maxNumberOfKeysInIndexPage;
    private final Integer bloomFilterNumberOfHashFunctions;
    private final Integer bloomFilterIndexSizeInBytes;
    private final Double bloomFilterProbabilityOfFalsePositive;
    private final Integer diskIoBufferSize;

    public SegmentConf(final long maxNumeberOfKeysInSegmentDeltaCache,
            final long maxNumberOfKeysInSegmentCacheDuringFlushing,
            final int maxNumberOfKeysInIndexPage,
            final Integer bloomFilterNumberOfHashFunctions,
            final Integer bloomFilterIndexSizeInBytes,
            final Double bloomFilterProbabilityOfFalsePositive,
            final Integer diskIoBufferSize) {
        this.maxNumberOfKeysInSegmentDeltaCache = maxNumeberOfKeysInSegmentDeltaCache;
        this.maxNumberOfKeysInDeltaCacheDuringWriting = maxNumberOfKeysInSegmentCacheDuringFlushing;
        this.maxNumberOfKeysInIndexPage = maxNumberOfKeysInIndexPage;
        this.bloomFilterNumberOfHashFunctions = bloomFilterNumberOfHashFunctions;
        this.bloomFilterIndexSizeInBytes = bloomFilterIndexSizeInBytes;
        this.bloomFilterProbabilityOfFalsePositive = bloomFilterProbabilityOfFalsePositive;
        this.diskIoBufferSize = diskIoBufferSize;
    }

    public SegmentConf(final SegmentConf segmentConf) {
        this.maxNumberOfKeysInSegmentDeltaCache = segmentConf.maxNumberOfKeysInSegmentDeltaCache;
        this.maxNumberOfKeysInDeltaCacheDuringWriting = segmentConf.maxNumberOfKeysInDeltaCacheDuringWriting;
        this.maxNumberOfKeysInIndexPage = segmentConf.maxNumberOfKeysInIndexPage;
        this.bloomFilterNumberOfHashFunctions = segmentConf.bloomFilterNumberOfHashFunctions;
        this.bloomFilterIndexSizeInBytes = segmentConf.bloomFilterIndexSizeInBytes;
        this.bloomFilterProbabilityOfFalsePositive = segmentConf.bloomFilterProbabilityOfFalsePositive;
        this.diskIoBufferSize = segmentConf.diskIoBufferSize;
    }

    /**
     * Provide number of keys in delta cache. Real number of keys in delta cache
     * is smaller or equal to this number.
     * 
     * @return return number of keys in delta cache
     */
    long getMaxNumberOfKeysInDeltaCache() {
        return maxNumberOfKeysInSegmentDeltaCache;
    }

    Integer getMaxNumberOfKeysInIndexPage() {
        return maxNumberOfKeysInIndexPage;
    }

    Integer getBloomFilterNumberOfHashFunctions() {
        return bloomFilterNumberOfHashFunctions;
    }

    Integer getBloomFilterIndexSizeInBytes() {
        return bloomFilterIndexSizeInBytes;
    }

    public Double getBloomFilterProbabilityOfFalsePositive() {
        return bloomFilterProbabilityOfFalsePositive;
    }

    /**
     * Provide number of keys in delta cache during writing. This value should
     * be at least 2 * maxNumberOfKeysInDeltaCache
     * 
     * @return
     */
    long getMaxNumberOfKeysInDeltaCacheDuringWriting() {
        return maxNumberOfKeysInDeltaCacheDuringWriting;
    }

    public Integer getDiskIoBufferSize() {
        return diskIoBufferSize;
    }
}
