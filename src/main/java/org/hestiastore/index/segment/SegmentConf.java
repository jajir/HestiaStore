package org.hestiastore.index.segment;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilter;

public class SegmentConf {

    private final int maxNumberOfKeysInSegmentWriteCache;
    private final int maxNumberOfKeysInSegmentWriteCacheDuringFlush;
    private final int maxNumberOfKeysInSegmentCache;
    private final int maxNumberOfKeysInChunk;
    private final Integer bloomFilterNumberOfHashFunctions;
    private final Integer bloomFilterIndexSizeInBytes;
    private final Double bloomFilterProbabilityOfFalsePositive;
    private final Integer diskIoBufferSize;
    private final List<ChunkFilter> encodingChunkFilters;
    private final List<ChunkFilter> decodingChunkFilters;

    public SegmentConf(final int maxNumberOfKeysInSegmentWriteCache,
            final int maxNumberOfKeysInSegmentWriteCacheDuringFlush,
            final int maxNumberOfKeysInSegmentCache,
            final int maxNumberOfKeysInChunk,
            final Integer bloomFilterNumberOfHashFunctions,
            final Integer bloomFilterIndexSizeInBytes,
            final Double bloomFilterProbabilityOfFalsePositive,
            final Integer diskIoBufferSize,
            final List<ChunkFilter> encodingChunkFilters,
            final List<ChunkFilter> decodingChunkFilters) {
        this.maxNumberOfKeysInSegmentWriteCache = maxNumberOfKeysInSegmentWriteCache;
        this.maxNumberOfKeysInSegmentWriteCacheDuringFlush = maxNumberOfKeysInSegmentWriteCacheDuringFlush;
        this.maxNumberOfKeysInSegmentCache = maxNumberOfKeysInSegmentCache;
        this.maxNumberOfKeysInChunk = maxNumberOfKeysInChunk;
        this.bloomFilterNumberOfHashFunctions = bloomFilterNumberOfHashFunctions;
        this.bloomFilterIndexSizeInBytes = bloomFilterIndexSizeInBytes;
        this.bloomFilterProbabilityOfFalsePositive = bloomFilterProbabilityOfFalsePositive;
        this.diskIoBufferSize = diskIoBufferSize;
        this.encodingChunkFilters = List.copyOf(encodingChunkFilters);
        this.decodingChunkFilters = List.copyOf(decodingChunkFilters);
    }

    public SegmentConf(final SegmentConf segmentConf) {
        this.maxNumberOfKeysInSegmentWriteCache = segmentConf.maxNumberOfKeysInSegmentWriteCache;
        this.maxNumberOfKeysInSegmentWriteCacheDuringFlush = segmentConf.maxNumberOfKeysInSegmentWriteCacheDuringFlush;
        this.maxNumberOfKeysInSegmentCache = segmentConf.maxNumberOfKeysInSegmentCache;
        this.maxNumberOfKeysInChunk = segmentConf.maxNumberOfKeysInChunk;
        this.bloomFilterNumberOfHashFunctions = segmentConf.bloomFilterNumberOfHashFunctions;
        this.bloomFilterIndexSizeInBytes = segmentConf.bloomFilterIndexSizeInBytes;
        this.bloomFilterProbabilityOfFalsePositive = segmentConf.bloomFilterProbabilityOfFalsePositive;
        this.diskIoBufferSize = segmentConf.diskIoBufferSize;
        this.encodingChunkFilters = List.copyOf(segmentConf.encodingChunkFilters);
        this.decodingChunkFilters = List.copyOf(segmentConf.decodingChunkFilters);
    }

    int getMaxNumberOfKeysInSegmentWriteCache() {
        return maxNumberOfKeysInSegmentWriteCache;
    }

    int getMaxNumberOfKeysInSegmentWriteCacheDuringFlush() {
        return maxNumberOfKeysInSegmentWriteCacheDuringFlush;
    }

    int getMaxNumberOfKeysInSegmentCache() {
        return maxNumberOfKeysInSegmentCache;
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
