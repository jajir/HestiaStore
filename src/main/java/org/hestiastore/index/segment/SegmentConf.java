package org.hestiastore.index.segment;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilter;

public class SegmentConf {

    private final long maxNumberOfKeysInSegmentDeltaCache;
    private final long maxNumberOfKeysInDeltaCacheDuringWriting;
    private final int maxNumberOfKeysInChunk;
    private final Integer bloomFilterNumberOfHashFunctions;
    private final Integer bloomFilterIndexSizeInBytes;
    private final Double bloomFilterProbabilityOfFalsePositive;
    private final Integer diskIoBufferSize;
    private final List<ChunkFilter> encodingChunkFilters;
    private final List<ChunkFilter> decodingChunkFilters;

    public SegmentConf(final long maxNumeberOfKeysInSegmentDeltaCache,
            final long maxNumberOfKeysInSegmentCacheDuringFlushing,
            final int maxNumberOfKeysInChunk,
            final Integer bloomFilterNumberOfHashFunctions,
            final Integer bloomFilterIndexSizeInBytes,
            final Double bloomFilterProbabilityOfFalsePositive,
            final Integer diskIoBufferSize,
            final List<ChunkFilter> encodingChunkFilters,
            final List<ChunkFilter> decodingChunkFilters) {
        this.maxNumberOfKeysInSegmentDeltaCache = maxNumeberOfKeysInSegmentDeltaCache;
        this.maxNumberOfKeysInDeltaCacheDuringWriting = maxNumberOfKeysInSegmentCacheDuringFlushing;
        this.maxNumberOfKeysInChunk = maxNumberOfKeysInChunk;
        this.bloomFilterNumberOfHashFunctions = bloomFilterNumberOfHashFunctions;
        this.bloomFilterIndexSizeInBytes = bloomFilterIndexSizeInBytes;
        this.bloomFilterProbabilityOfFalsePositive = bloomFilterProbabilityOfFalsePositive;
        this.diskIoBufferSize = diskIoBufferSize;
        this.encodingChunkFilters = List.copyOf(encodingChunkFilters);
        this.decodingChunkFilters = List.copyOf(decodingChunkFilters);
    }

    public SegmentConf(final SegmentConf segmentConf) {
        this.maxNumberOfKeysInSegmentDeltaCache = segmentConf.maxNumberOfKeysInSegmentDeltaCache;
        this.maxNumberOfKeysInDeltaCacheDuringWriting = segmentConf.maxNumberOfKeysInDeltaCacheDuringWriting;
        this.maxNumberOfKeysInChunk = segmentConf.maxNumberOfKeysInChunk;
        this.bloomFilterNumberOfHashFunctions = segmentConf.bloomFilterNumberOfHashFunctions;
        this.bloomFilterIndexSizeInBytes = segmentConf.bloomFilterIndexSizeInBytes;
        this.bloomFilterProbabilityOfFalsePositive = segmentConf.bloomFilterProbabilityOfFalsePositive;
        this.diskIoBufferSize = segmentConf.diskIoBufferSize;
        this.encodingChunkFilters = List.copyOf(segmentConf.encodingChunkFilters);
        this.decodingChunkFilters = List.copyOf(segmentConf.decodingChunkFilters);
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

    Integer getMaxNumberOfKeysInChunk() {
        return maxNumberOfKeysInChunk;
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

    public List<ChunkFilter> getEncodingChunkFilters() {
        return encodingChunkFilters;
    }

    public List<ChunkFilter> getDecodingChunkFilters() {
        return decodingChunkFilters;
    }
}
