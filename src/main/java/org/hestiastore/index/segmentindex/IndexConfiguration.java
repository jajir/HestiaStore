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
        this.bloomFilterNumberOfHashFunctions = bloomFilterNumberOfHashFunctions;
        this.bloomFilterIndexSizeInBytes = bloomFilterIndexSizeInBytes;
        this.bloomFilterProbabilityOfFalsePositive = bloomFilterProbabilityOfFalsePositive;
        this.diskIoBufferSize = diskIoBufferSize;
        this.contextLoggingEnabled = contextLoggingEnabled;
        this.encodingChunkFilters = List.copyOf(encodingChunkFilters);
        this.decodingChunkFilters = List.copyOf(decodingChunkFilters);
    }

    /**
     * Returns the maximum number of keys held in the in-memory segment cache.
     *
     * @return max keys in segment cache
     */
    public Integer getMaxNumberOfKeysInSegmentCache() {
        return maxNumberOfKeysInSegmentCache;
    }

    /**
     * Returns the maximum number of keys held in the segment write cache before
     * flushing to disk.
     *
     * @return max keys in segment write cache
     */
    public Integer getMaxNumberOfKeysInSegmentWriteCache() {
        return maxNumberOfKeysInSegmentWriteCache;
    }

    /**
     * Returns the maximum number of keys allowed while maintenance is in flight
     * before back-pressure is applied to writers.
     *
     * @return max buffered keys during maintenance
     */
    public Integer getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance() {
        return maxNumberOfKeysInSegmentWriteCacheDuringMaintenance;
    }

    /**
     * Returns the maximum number of keys per segment chunk used for on-disk
     * layout and indexing.
     *
     * @return max keys per segment chunk
     */
    public Integer getMaxNumberOfKeysInSegmentChunk() {
        return maxNumberOfKeysInSegmentChunk;
    }

    /**
     * Returns the logical name of the index.
     *
     * @return index name
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     * Returns the maximum number of keys kept in the top-level index cache.
     *
     * @return max keys in index cache
     */
    public Integer getMaxNumberOfKeysInCache() {
        return maxNumberOfKeysInCache;
    }

    /**
     * Returns the maximum number of keys allowed within a single segment.
     *
     * @return max keys per segment
     */
    public Integer getMaxNumberOfKeysInSegment() {
        return maxNumberOfKeysInSegment;
    }

    /**
     * Returns the number of hash functions used by the Bloom filter.
     *
     * @return Bloom filter hash function count
     */
    public Integer getBloomFilterNumberOfHashFunctions() {
        return bloomFilterNumberOfHashFunctions;
    }

    /**
     * Returns the size of the Bloom filter index in bytes.
     *
     * @return Bloom filter size in bytes
     */
    public Integer getBloomFilterIndexSizeInBytes() {
        return bloomFilterIndexSizeInBytes;
    }

    /**
     * Returns the target false-positive probability for the Bloom filter
     * (0.0–1.0).
     *
     * @return Bloom filter false-positive probability
     */
    public Double getBloomFilterProbabilityOfFalsePositive() {
        return bloomFilterProbabilityOfFalsePositive;
    }

    /**
     * Returns the number of CPU threads used for index operations.
     *
     * @return CPU thread count
     */
    public Integer getNumberOfThreads() {
        return numberOfThreads;
    }

    /**
     * Returns the number of IO threads used by the async directory wrapper.
     *
     * @return IO thread count
     */
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
     * Returns the maximum number of segments retained in the in-memory segment
     * cache.
     *
     * @return max segments in cache
     */
    public Integer getMaxNumberOfSegmentsInCache() {
        return maxNumberOfSegmentsInCache;
    }

    /**
     * Returns the disk I/O buffer size in bytes.
     *
     * @return disk I/O buffer size in bytes
     */
    public Integer getDiskIoBufferSize() {
        return diskIoBufferSize;
    }

    /**
     * Indicates whether logging context propagation via MDC is enabled.
     *
     * @return true if context logging is enabled; otherwise false
     */
    public Boolean isContextLoggingEnabled() {
        return contextLoggingEnabled;
    }

    /**
     * Returns the key class for this index.
     *
     * @return key class
     */
    public Class<K> getKeyClass() {
        return keyClass;
    }

    /**
     * Returns the value class for this index.
     *
     * @return value class
     */
    public Class<V> getValueClass() {
        return valueClass;
    }

    /**
     * Returns the fully qualified class name of the key type descriptor used
     * for serialization.
     *
     * @return key type descriptor class name
     */
    public String getKeyTypeDescriptor() {
        return keyTypeDescriptor;
    }

    /**
     * Returns the fully qualified class name of the value type descriptor used
     * for serialization.
     *
     * @return value type descriptor class name
     */
    public String getValueTypeDescriptor() {
        return valueTypeDescriptor;
    }

    /**
     * Returns the ordered list of chunk filters applied during encoding (write
     * path).
     *
     * @return encoding chunk filters
     */
    public List<ChunkFilter> getEncodingChunkFilters() {
        return encodingChunkFilters;
    }

    /**
     * Returns the ordered list of chunk filters applied during decoding (read
     * path).
     *
     * @return decoding chunk filters
     */
    public List<ChunkFilter> getDecodingChunkFilters() {
        return decodingChunkFilters;
    }
}
