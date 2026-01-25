package org.hestiastore.index.segment;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilter;

/**
 * Immutable configuration values for a segment instance.
 */
public class SegmentConf {

    /**
     * Sentinel value for unset Bloom filter hash function count.
     */
    public static final int UNSET_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS = -1;

    /**
     * Sentinel value for unset Bloom filter index size.
     */
    public static final int UNSET_BLOOM_FILTER_INDEX_SIZE_IN_BYTES = -1;

    /**
     * Sentinel value for unset Bloom filter false positive probability.
     */
    public static final double UNSET_BLOOM_FILTER_PROBABILITY = -1D;

    private final int maxNumberOfKeysInSegmentWriteCache;
    private final int maxNumberOfKeysInSegmentWriteCacheDuringMaintenance;
    private final int maxNumberOfKeysInSegmentCache;
    private final int maxNumberOfKeysInChunk;
    private final int maxNumberOfDeltaCacheFiles;
    private final int bloomFilterNumberOfHashFunctions;
    private final int bloomFilterIndexSizeInBytes;
    private final double bloomFilterProbabilityOfFalsePositive;
    private final int diskIoBufferSize;
    private final List<ChunkFilter> encodingChunkFilters;
    private final List<ChunkFilter> decodingChunkFilters;

    /**
     * Creates a configuration with explicit values for all fields.
     *
     * @param maxNumberOfKeysInSegmentWriteCache max write-cache size
     * @param maxNumberOfKeysInSegmentWriteCacheDuringMaintenance write-cache size
     *        allowed during maintenance
     * @param maxNumberOfKeysInSegmentCache max segment cache size
     * @param maxNumberOfKeysInChunk max number of keys in a chunk
     * @param maxNumberOfDeltaCacheFiles max delta cache file count
     * @param bloomFilterNumberOfHashFunctions Bloom filter hash count or
     *        {@link #UNSET_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS}
     * @param bloomFilterIndexSizeInBytes Bloom filter index size in bytes or
     *        {@link #UNSET_BLOOM_FILTER_INDEX_SIZE_IN_BYTES}
     * @param bloomFilterProbabilityOfFalsePositive Bloom filter false positive
     *        probability or {@link #UNSET_BLOOM_FILTER_PROBABILITY}
     * @param diskIoBufferSize disk I/O buffer size in bytes
     * @param encodingChunkFilters chunk filters applied during encoding
     * @param decodingChunkFilters chunk filters applied during decoding
     */
    public SegmentConf(final int maxNumberOfKeysInSegmentWriteCache,
            final int maxNumberOfKeysInSegmentWriteCacheDuringMaintenance,
            final int maxNumberOfKeysInSegmentCache,
            final int maxNumberOfKeysInChunk,
            final int maxNumberOfDeltaCacheFiles,
            final int bloomFilterNumberOfHashFunctions,
            final int bloomFilterIndexSizeInBytes,
            final double bloomFilterProbabilityOfFalsePositive,
            final int diskIoBufferSize,
            final List<ChunkFilter> encodingChunkFilters,
            final List<ChunkFilter> decodingChunkFilters) {
        this.maxNumberOfKeysInSegmentWriteCache = maxNumberOfKeysInSegmentWriteCache;
        this.maxNumberOfKeysInSegmentWriteCacheDuringMaintenance = maxNumberOfKeysInSegmentWriteCacheDuringMaintenance;
        this.maxNumberOfKeysInSegmentCache = maxNumberOfKeysInSegmentCache;
        this.maxNumberOfKeysInChunk = maxNumberOfKeysInChunk;
        this.maxNumberOfDeltaCacheFiles = maxNumberOfDeltaCacheFiles;
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
        this.maxNumberOfKeysInSegmentWriteCacheDuringMaintenance = segmentConf.maxNumberOfKeysInSegmentWriteCacheDuringMaintenance;
        this.maxNumberOfKeysInSegmentCache = segmentConf.maxNumberOfKeysInSegmentCache;
        this.maxNumberOfKeysInChunk = segmentConf.maxNumberOfKeysInChunk;
        this.maxNumberOfDeltaCacheFiles = segmentConf.maxNumberOfDeltaCacheFiles;
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
     * Returns the maximum write-cache size allowed during maintenance.
     *
     * @return max number of keys in write cache during maintenance
     */
    int getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance() {
        return maxNumberOfKeysInSegmentWriteCacheDuringMaintenance;
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
    int getMaxNumberOfKeysInChunk() {
        return maxNumberOfKeysInChunk;
    }

    /**
     * Returns the maximum number of delta cache files allowed per segment.
     *
     * @return max delta cache file count
     */
    int getMaxNumberOfDeltaCacheFiles() {
        return maxNumberOfDeltaCacheFiles;
    }

    /**
     * Returns the Bloom filter hash function count.
     *
     * @return number of hash functions or
     *         {@link #UNSET_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS}
     */
    int getBloomFilterNumberOfHashFunctions() {
        return bloomFilterNumberOfHashFunctions;
    }

    /**
     * Returns the Bloom filter index size in bytes.
     *
     * @return index size in bytes or
     *         {@link #UNSET_BLOOM_FILTER_INDEX_SIZE_IN_BYTES}
     */
    int getBloomFilterIndexSizeInBytes() {
        return bloomFilterIndexSizeInBytes;
    }

    /**
     * Returns the configured Bloom filter false positive probability.
     *
     * @return false positive probability or
     *         {@link #UNSET_BLOOM_FILTER_PROBABILITY}
     */
    public double getBloomFilterProbabilityOfFalsePositive() {
        return bloomFilterProbabilityOfFalsePositive;
    }

    /**
     * Returns the disk I/O buffer size in bytes.
     *
     * @return buffer size in bytes
     */
    public int getDiskIoBufferSize() {
        return diskIoBufferSize;
    }

    /**
     * Returns whether Bloom filter hash function count was configured.
     *
     * @return true when hash count is set
     */
    boolean hasBloomFilterNumberOfHashFunctions() {
        return bloomFilterNumberOfHashFunctions != UNSET_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS;
    }

    /**
     * Returns whether Bloom filter index size was configured.
     *
     * @return true when index size is set
     */
    boolean hasBloomFilterIndexSizeInBytes() {
        return bloomFilterIndexSizeInBytes != UNSET_BLOOM_FILTER_INDEX_SIZE_IN_BYTES;
    }

    /**
     * Returns whether Bloom filter false positive probability was configured.
     *
     * @return true when probability is set
     */
    boolean hasBloomFilterProbabilityOfFalsePositive() {
        return Double.compare(bloomFilterProbabilityOfFalsePositive,
                UNSET_BLOOM_FILTER_PROBABILITY) != 0;
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
