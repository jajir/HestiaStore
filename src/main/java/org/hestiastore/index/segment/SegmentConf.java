package org.hestiastore.index.segment;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilter;

/**
 * Immutable configuration values for a segment instance.
 */
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

    /**
     * Creates a configuration with explicit values for all fields.
     *
     * @param maxNumberOfKeysInSegmentWriteCache max write-cache size
     * @param maxNumberOfKeysInSegmentWriteCacheDuringFlush write-cache size
     *        allowed during flush
     * @param maxNumberOfKeysInSegmentCache max segment cache size
     * @param maxNumberOfKeysInChunk max number of keys in a chunk
     * @param bloomFilterNumberOfHashFunctions Bloom filter hash count
     * @param bloomFilterIndexSizeInBytes Bloom filter index size in bytes
     * @param bloomFilterProbabilityOfFalsePositive Bloom filter false positive
     *        probability
     * @param diskIoBufferSize disk I/O buffer size in bytes
     * @param encodingChunkFilters chunk filters applied during encoding
     * @param decodingChunkFilters chunk filters applied during decoding
     */
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

    /**
     * Creates a copy of an existing configuration.
     *
     * @param segmentConf source configuration
     */
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

    /**
     * Returns the write-cache key limit for a segment.
     *
     * @return max number of keys in the write cache
     */
    int getMaxNumberOfKeysInSegmentWriteCache() {
        return maxNumberOfKeysInSegmentWriteCache;
    }

    /**
     * Returns the maximum write-cache size allowed during flush.
     *
     * @return max number of keys in write cache during flush
     */
    int getMaxNumberOfKeysInSegmentWriteCacheDuringFlush() {
        return maxNumberOfKeysInSegmentWriteCacheDuringFlush;
    }

    /**
     * Returns the maximum cached key count for the segment.
     *
     * @return max number of keys in segment cache
     */
    int getMaxNumberOfKeysInSegmentCache() {
        return maxNumberOfKeysInSegmentCache;
    }

    /**
     * Returns the maximum number of keys per chunk.
     *
     * @return max keys in a chunk
     */
    Integer getMaxNumberOfKeysInChunk() {
        return maxNumberOfKeysInChunk;
    }

    /**
     * Returns the Bloom filter hash function count.
     *
     * @return number of hash functions
     */
    Integer getBloomFilterNumberOfHashFunctions() {
        return bloomFilterNumberOfHashFunctions;
    }

    /**
     * Returns the Bloom filter index size in bytes.
     *
     * @return index size in bytes
     */
    Integer getBloomFilterIndexSizeInBytes() {
        return bloomFilterIndexSizeInBytes;
    }

    /**
     * Returns the configured Bloom filter false positive probability.
     *
     * @return false positive probability
     */
    public Double getBloomFilterProbabilityOfFalsePositive() {
        return bloomFilterProbabilityOfFalsePositive;
    }

    /**
     * Returns the disk I/O buffer size in bytes.
     *
     * @return buffer size in bytes
     */
    public Integer getDiskIoBufferSize() {
        return diskIoBufferSize;
    }

    /**
     * Returns the encoding chunk filter chain.
     *
     * @return encoding filters
     */
    public List<ChunkFilter> getEncodingChunkFilters() {
        return encodingChunkFilters;
    }

    /**
     * Returns the decoding chunk filter chain.
     *
     * @return decoding filters
     */
    public List<ChunkFilter> getDecodingChunkFilters() {
        return decodingChunkFilters;
    }
}
