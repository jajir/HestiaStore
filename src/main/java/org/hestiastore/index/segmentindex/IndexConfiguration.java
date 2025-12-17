package org.hestiastore.index.segmentindex;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilter;

/**
 * Immutable configuration for the segment-index layer.
 * <p>
 * Encapsulates key/value types, index name, segment sizing and caching limits,
 * Bloom filter parameters, disk I/O buffer size, thread-safety and logging
 * switches, and the chunk encoding/decoding filter pipeline. Instances are
 * created via the fluent {@link IndexConfigurationBuilder}.
 *
 * @param <K> key type
 * @param <V> value type
 * @see IndexConfigurationBuilder
 */
public class IndexConfiguration<K, V> {

    /**
     * general Data configuration.
     */
    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private final String keyTypeDescriptor;
    private final String valueTypeDescriptor;

    /*
     * Segments configuration
     */
    private final Integer maxNumberOfKeysInSegmentCache;
    private final Integer maxNumberOfKeysInSegmentCacheDuringFlushing;
    private final Integer maxNumberOfKeysInSegmentChunk;

    /*
     * Segment index configuration
     */
    private final String indexName;
    private final Integer maxNumberOfKeysInCache;
    private final Integer maxNumberOfKeysInSegment;
    private final Integer maxNumberOfSegmentsInCache;

    private final Integer bloomFilterNumberOfHashFunctions;
    private final Integer bloomFilterIndexSizeInBytes;
    private final Double bloomFilterProbabilityOfFalsePositive;

    private final Integer diskIoBufferSize;
    private final Boolean threadSafe;
    private final Boolean contextLoggingEnabled;

    private final List<ChunkFilter> encodingChunkFilters;
    private final List<ChunkFilter> decodingChunkFilters;

    /**
     * Creates a new instance of IndexConfigurationBuilder.
     *
     * @param <M> the type of the key
     * @param <N> the type of the value
     * @return a new instance of IndexConfigurationBuilder
     */
    public static <M, N> IndexConfigurationBuilder<M, N> builder() {
        return new IndexConfigurationBuilder<>();
    }

    IndexConfiguration(final Class<K> keyClass, //
            final Class<V> valueClass, //
            final String keyTypeDescriptor, //
            final String valueTypeDescriptor, //
            final Integer maxNumberOfKeysInSegmentCache, //
            final Integer maxNumberOfKeysInSegmentCacheDuringFlushing, //
            final Integer maxNumberOfKeysInSegmentChunk, //
            final Integer maxNumberOfKeysInCache, //
            final Integer maxNumberOfKeysInSegment, //
            final Integer maxNumberOfSegmentsInCache, //
            final String indexName, //
            final Integer bloomFilterNumberOfHashFunctions, //
            final Integer bloomFilterIndexSizeInBytes, //
            final Double bloomFilterProbabilityOfFalsePositive, //
            final Integer diskIoBufferSize, final Boolean threadSafe,
            final Boolean contextLoggingEnabled,
            final List<ChunkFilter> encodingChunkFilters,
            final List<ChunkFilter> decodingChunkFilters) {
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.keyTypeDescriptor = keyTypeDescriptor;
        this.valueTypeDescriptor = valueTypeDescriptor;
        this.maxNumberOfKeysInSegmentCache = maxNumberOfKeysInSegmentCache;
        this.maxNumberOfKeysInSegmentCacheDuringFlushing = maxNumberOfKeysInSegmentCacheDuringFlushing;
        this.maxNumberOfKeysInSegmentChunk = maxNumberOfKeysInSegmentChunk;
        this.indexName = indexName;
        this.maxNumberOfKeysInCache = maxNumberOfKeysInCache;
        this.maxNumberOfKeysInSegment = maxNumberOfKeysInSegment;
        this.maxNumberOfSegmentsInCache = maxNumberOfSegmentsInCache;
        this.bloomFilterNumberOfHashFunctions = bloomFilterNumberOfHashFunctions;
        this.bloomFilterIndexSizeInBytes = bloomFilterIndexSizeInBytes;
        this.bloomFilterProbabilityOfFalsePositive = bloomFilterProbabilityOfFalsePositive;
        this.diskIoBufferSize = diskIoBufferSize;
        this.threadSafe = threadSafe;
        this.contextLoggingEnabled = contextLoggingEnabled;
        this.encodingChunkFilters = List.copyOf(encodingChunkFilters);
        this.decodingChunkFilters = List.copyOf(decodingChunkFilters);
    }

    public Integer getMaxNumberOfKeysInSegmentCache() {
        /**
         * Returns the maximum number of keys held in the in-memory segment
         * cache.
         *
         * @return max keys in segment cache
         */
        return maxNumberOfKeysInSegmentCache;
    }

    public Integer getMaxNumberOfKeysInSegmentChunk() {
        /**
         * Returns the maximum number of keys per segment chunk used for on-disk
         * layout and indexing.
         *
         * @return max keys per segment chunk
         */
        return maxNumberOfKeysInSegmentChunk;
    }

    public String getIndexName() {
        /**
         * Returns the logical name of the index.
         *
         * @return index name
         */
        return indexName;
    }

    public Integer getMaxNumberOfKeysInCache() {
        /**
         * Returns the maximum number of keys kept in the top-level index cache.
         *
         * @return max keys in index cache
         */
        return maxNumberOfKeysInCache;
    }

    public Integer getMaxNumberOfKeysInSegment() {
        /**
         * Returns the maximum number of keys allowed within a single segment.
         *
         * @return max keys per segment
         */
        return maxNumberOfKeysInSegment;
    }

    public Integer getBloomFilterNumberOfHashFunctions() {
        /**
         * Returns the number of hash functions used by the Bloom filter.
         *
         * @return Bloom filter hash function count
         */
        return bloomFilterNumberOfHashFunctions;
    }

    public Integer getBloomFilterIndexSizeInBytes() {
        /**
         * Returns the size of the Bloom filter index in bytes.
         *
         * @return Bloom filter size in bytes
         */
        return bloomFilterIndexSizeInBytes;
    }

    public Double getBloomFilterProbabilityOfFalsePositive() {
        /**
         * Returns the target false-positive probability for the Bloom filter
         * (0.0–1.0).
         *
         * @return Bloom filter false-positive probability
         */
        return bloomFilterProbabilityOfFalsePositive;
    }

    public Integer getMaxNumberOfSegmentsInCache() {
        /**
         * Returns the maximum number of segments retained in the in-memory
         * segment cache.
         *
         * @return max segments in cache
         */
        return maxNumberOfSegmentsInCache;
    }

    public Integer getMaxNumberOfKeysInSegmentCacheDuringFlushing() {
        /**
         * Returns the maximum number of keys allowed in segment cache during
         * flushing operations.
         *
         * @return max keys in segment cache during flushing
         */
        return maxNumberOfKeysInSegmentCacheDuringFlushing;
    }

    public Integer getDiskIoBufferSize() {
        /**
         * Returns the disk I/O buffer size in bytes.
         *
         * @return disk I/O buffer size in bytes
         */
        return diskIoBufferSize;
    }

    public Boolean isThreadSafe() {
        /**
         * Indicates whether a thread-safe (synchronized) index implementation
         * should be used.
         *
         * @return true if thread-safe variant is enabled; otherwise false
         */
        return threadSafe;
    }

    public Boolean isContextLoggingEnabled() {
        /**
         * Indicates whether logging context propagation via MDC is enabled.
         *
         * @return true if context logging is enabled; otherwise false
         */
        return contextLoggingEnabled;
    }

    public Class<K> getKeyClass() {
        /**
         * Returns the key class for this index.
         *
         * @return key class
         */
        return keyClass;
    }

    public Class<V> getValueClass() {
        /**
         * Returns the value class for this index.
         *
         * @return value class
         */
        return valueClass;
    }

    public String getKeyTypeDescriptor() {
        /**
         * Returns the fully qualified class name of the key type descriptor
         * used for serialization.
         *
         * @return key type descriptor class name
         */
        return keyTypeDescriptor;
    }

    public String getValueTypeDescriptor() {
        /**
         * Returns the fully qualified class name of the value type descriptor
         * used for serialization.
         *
         * @return value type descriptor class name
         */
        return valueTypeDescriptor;
    }

    public List<ChunkFilter> getEncodingChunkFilters() {
        /**
         * Returns the ordered list of chunk filters applied during encoding
         * (write path).
         *
         * @return encoding chunk filters
         */
        return encodingChunkFilters;
    }

    public List<ChunkFilter> getDecodingChunkFilters() {
        /**
         * Returns the ordered list of chunk filters applied during decoding
         * (read path).
         *
         * @return decoding chunk filters
         */
        return decodingChunkFilters;
    }
}
