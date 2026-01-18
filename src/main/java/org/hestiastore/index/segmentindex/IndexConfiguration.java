package org.hestiastore.index.segmentindex;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilter;

/**
 * Immutable configuration for the segment-index layer.
 * <p>
 * Encapsulates key/value types, index name, segment sizing and caching limits,
 * Bloom filter parameters, disk I/O buffer size, logging switches, and the
 * chunk encoding/decoding filter pipeline. Instances are created via the fluent
 * {@link IndexConfigurationBuilder}.
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
    private final Integer maxNumberOfKeysInSegmentWriteCache;
    private final Integer maxNumberOfKeysInSegmentWriteCacheDuringMaintenance;
    private final Integer maxNumberOfKeysInSegmentChunk;

    /*
     * Segment index configuration
     */
    private final String indexName;
    private final Integer maxNumberOfKeysInCache;
    private final Integer maxNumberOfKeysInSegment;
    private final Integer maxNumberOfSegmentsInCache;
    private final Integer numberOfThreads;
    private final Integer numberOfIoThreads;
    private final Integer numberOfSegmentIndexMaintenanceThreads;
    private final Integer numberOfIndexMaintenanceThreads;
    private final Integer indexBusyBackoffMillis;
    private final Integer indexBusyTimeoutMillis;
    private final Boolean segmentMaintenanceAutoEnabled;
    private final Boolean segmentRootDirectoryEnabled;

    private final Integer bloomFilterNumberOfHashFunctions;
    private final Integer bloomFilterIndexSizeInBytes;
    private final Double bloomFilterProbabilityOfFalsePositive;

    private final Integer diskIoBufferSize;
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
            final Integer maxNumberOfKeysInSegmentWriteCache, //
            final Integer maxNumberOfKeysInSegmentWriteCacheDuringMaintenance, //
            final Integer maxNumberOfKeysInSegmentChunk, //
            final Integer maxNumberOfKeysInCache, //
            final Integer maxNumberOfKeysInSegment, //
            final Integer maxNumberOfSegmentsInCache, //
            final String indexName, //
            final Integer bloomFilterNumberOfHashFunctions, //
            final Integer bloomFilterIndexSizeInBytes, //
            final Double bloomFilterProbabilityOfFalsePositive, //
            final Integer diskIoBufferSize, final Boolean contextLoggingEnabled,
            final Integer numberOfThreads, final Integer numberOfIoThreads,
            final Integer numberOfSegmentIndexMaintenanceThreads,
            final Integer numberOfIndexMaintenanceThreads,
            final Integer indexBusyBackoffMillis,
            final Integer indexBusyTimeoutMillis,
            final Boolean segmentMaintenanceAutoEnabled,
            final Boolean segmentRootDirectoryEnabled,
            final List<ChunkFilter> encodingChunkFilters,
            final List<ChunkFilter> decodingChunkFilters) {
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.keyTypeDescriptor = keyTypeDescriptor;
        this.valueTypeDescriptor = valueTypeDescriptor;
        this.maxNumberOfKeysInSegmentCache = maxNumberOfKeysInSegmentCache;
        this.maxNumberOfKeysInSegmentWriteCache = maxNumberOfKeysInSegmentWriteCache;
        this.maxNumberOfKeysInSegmentWriteCacheDuringMaintenance = maxNumberOfKeysInSegmentWriteCacheDuringMaintenance;
        this.maxNumberOfKeysInSegmentChunk = maxNumberOfKeysInSegmentChunk;
        this.indexName = indexName;
        this.maxNumberOfKeysInCache = maxNumberOfKeysInCache;
        this.maxNumberOfKeysInSegment = maxNumberOfKeysInSegment;
        this.maxNumberOfSegmentsInCache = maxNumberOfSegmentsInCache;
        this.numberOfThreads = numberOfThreads;
        this.numberOfIoThreads = numberOfIoThreads;
        this.numberOfSegmentIndexMaintenanceThreads = numberOfSegmentIndexMaintenanceThreads;
        this.numberOfIndexMaintenanceThreads = numberOfIndexMaintenanceThreads;
        this.indexBusyBackoffMillis = indexBusyBackoffMillis;
        this.indexBusyTimeoutMillis = indexBusyTimeoutMillis;
        this.segmentMaintenanceAutoEnabled = segmentMaintenanceAutoEnabled;
        this.segmentRootDirectoryEnabled = segmentRootDirectoryEnabled;
        this.bloomFilterNumberOfHashFunctions = bloomFilterNumberOfHashFunctions;
        this.bloomFilterIndexSizeInBytes = bloomFilterIndexSizeInBytes;
        this.bloomFilterProbabilityOfFalsePositive = bloomFilterProbabilityOfFalsePositive;
        this.diskIoBufferSize = diskIoBufferSize;
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

    public Integer getMaxNumberOfKeysInSegmentWriteCache() {
        /**
         * Returns the maximum number of keys held in the segment write cache
         * before flushing to disk.
         *
         * @return max keys in segment write cache
         */
        return maxNumberOfKeysInSegmentWriteCache;
    }

    public Integer getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance() {
        /**
         * Returns the maximum number of keys allowed while maintenance is in
         * flight before back-pressure is applied to writers.
         *
         * @return max buffered keys during maintenance
         */
        return maxNumberOfKeysInSegmentWriteCacheDuringMaintenance;
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

    public Integer getNumberOfThreads() {
        return numberOfThreads;
    }

    public Integer getNumberOfIoThreads() {
        return numberOfIoThreads;
    }

    /**
     * Returns the number of threads used for segment index maintenance.
     *
     * @return maintenance thread count
     */
    public Integer getNumberOfSegmentIndexMaintenanceThreads() {
        return numberOfSegmentIndexMaintenanceThreads;
    }

    /**
     * Returns the number of threads used for split maintenance.
     *
     * @return split maintenance thread count
     */
    public Integer getNumberOfIndexMaintenanceThreads() {
        return numberOfIndexMaintenanceThreads;
    }

    /**
     * Returns the busy backoff delay in milliseconds for index retries.
     *
     * @return busy backoff in milliseconds
     */
    public Integer getIndexBusyBackoffMillis() {
        return indexBusyBackoffMillis;
    }

    /**
     * Returns the busy retry timeout in milliseconds for index operations.
     *
     * @return busy retry timeout in milliseconds
     */
    public Integer getIndexBusyTimeoutMillis() {
        return indexBusyTimeoutMillis;
    }

    /**
     * Returns whether auto flush/compact is scheduled after writes.
     *
     * @return true if auto maintenance is enabled; otherwise false
     */
    public Boolean isSegmentMaintenanceAutoEnabled() {
        return segmentMaintenanceAutoEnabled;
    }

    /**
     * Returns whether segments use per-segment root directories.
     *
     * @return true when segment root directories are enabled
     */
    public Boolean isSegmentRootDirectoryEnabled() {
        return segmentRootDirectoryEnabled;
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

    public Integer getDiskIoBufferSize() {
        /**
         * Returns the disk I/O buffer size in bytes.
         *
         * @return disk I/O buffer size in bytes
         */
        return diskIoBufferSize;
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
