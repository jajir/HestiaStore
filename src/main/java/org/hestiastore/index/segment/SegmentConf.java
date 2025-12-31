package org.hestiastore.index.segment;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilter;

public class SegmentConf {

    private final int maxNumberOfKeysInSegmentDeltaCache;
    private final int maxNumberOfKeysInSegmentWriteCache;
    private final int maxNumberOfKeysInChunk;
    private final Integer bloomFilterNumberOfHashFunctions;
    private final Integer bloomFilterIndexSizeInBytes;
    private final Double bloomFilterProbabilityOfFalsePositive;
    private final Integer diskIoBufferSize;
    private final List<ChunkFilter> encodingChunkFilters;
    private final List<ChunkFilter> decodingChunkFilters;

    public SegmentConf(final int maxNumeberOfKeysInSegmentDeltaCache,
            final int maxNumberOfKeysInSegmentWriteCache,
            final int maxNumberOfKeysInChunk,
            final Integer bloomFilterNumberOfHashFunctions,
            final Integer bloomFilterIndexSizeInBytes,
            final Double bloomFilterProbabilityOfFalsePositive,
            final Integer diskIoBufferSize,
            final List<ChunkFilter> encodingChunkFilters,
            final List<ChunkFilter> decodingChunkFilters) {
        this.maxNumberOfKeysInSegmentDeltaCache = maxNumeberOfKeysInSegmentDeltaCache;
        this.maxNumberOfKeysInSegmentWriteCache = maxNumberOfKeysInSegmentWriteCache;
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
        this.maxNumberOfKeysInSegmentWriteCache = segmentConf.maxNumberOfKeysInSegmentWriteCache;
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
    int getMaxNumberOfKeysInDeltaCache() {
        return maxNumberOfKeysInSegmentDeltaCache;
    }

    int getMaxNumberOfKeysInSegmentWriteCache() {
        return maxNumberOfKeysInSegmentWriteCache;
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
