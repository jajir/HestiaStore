package org.hestiastore.index.segmentindex;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Validation;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Writing;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberValidation;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;

/**
 * Define contract, that define index configuration.
 * 
 * @author honza
 *
 */
public interface IndexConfigurationContract {
    int MAX_NUMBER_OF_KEYS_IN_CACHE = 4321;
    int MAX_NUMBER_OF_KEYS_IN_SEGMENT = 10_000_000;
    int MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE = 10_000;
    int MAX_NUMBER_OF_KEYS_IN_SEGMENT_CHUNK = 1_000;
    int MAX_NUMBER_OF_SEGMENTS_IN_CACHE = 10;
    int MAX_NUMBER_OF_DELTA_CACHE_FILES = 10;

    int BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS = 3;
    int BLOOM_FILTER_INDEX_SIZE_IN_BYTES = 5_000_000;
    double BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE = 0.01;

    int DISK_IO_BUFFER_SIZE_IN_BYTES = 1024 * 8;
    int NUMBER_OF_THREADS = 1;
    int NUMBER_OF_IO_THREADS = 1;
    int DEFAULT_SEGMENT_INDEX_MAINTENANCE_THREADS = 10;
    int DEFAULT_INDEX_MAINTENANCE_THREADS = 10;
    int DEFAULT_INDEX_BUSY_BACKOFF_MILLIS = 5;
    int DEFAULT_INDEX_BUSY_TIMEOUT_MILLIS = 30_000;
    boolean DEFAULT_SEGMENT_MAINTENANCE_AUTO_ENABLED = true;

    /**
     * Returns the default maximum number of keys in the in-memory segment
     * cache.
     *
     * @return default max keys in segment cache
     */
    default int getMaxNumberOfKeysInSegmentCache() {
        return MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE;
    }

    /**
     * Returns the default maximum number of keys buffered in the segment write
     * cache before flush.
     *
     * @return default max keys in segment write cache
     */
    default int getMaxNumberOfKeysInSegmentWriteCache() {
        return getMaxNumberOfKeysInSegmentCache() / 2;
    }

    /**
     * Returns the default maximum number of buffered keys allowed while
     * maintenance is in flight.
     *
     * @return default max buffered keys during maintenance
     */
    default int getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance() {
        return Math.max(getMaxNumberOfKeysInSegmentWriteCache() + 1,
                (int) Math.ceil(getMaxNumberOfKeysInSegmentWriteCache()
                        * 1.4));
    }

    /**
     * Returns the default maximum number of keys per segment chunk.
     *
     * @return default max keys per chunk
     */
    default int getMaxNumberOfKeysInSegmentChunk() {
        return MAX_NUMBER_OF_KEYS_IN_SEGMENT_CHUNK;
    }

    /**
     * Returns the default maximum number of delta cache files allowed for a
     * segment before maintenance should be triggered.
     *
     * @return default max delta cache files per segment
     */
    default int getMaxNumberOfDeltaCacheFiles() {
        return MAX_NUMBER_OF_DELTA_CACHE_FILES;
    }

    /**
     * Returns the default maximum number of keys stored in the top-level index
     * cache.
     *
     * @return default max keys in index cache
     */
    default int getMaxNumberOfKeysInCache() {
        return MAX_NUMBER_OF_KEYS_IN_CACHE;
    }

    /**
     * Returns the default maximum number of keys per segment.
     *
     * @return default max keys per segment
     */
    default int getMaxNumberOfKeysInSegment() {
        return MAX_NUMBER_OF_KEYS_IN_SEGMENT;
    }

    /**
     * Returns the default maximum number of segments kept in the in-memory
     * cache.
     *
     * @return default max segments in cache
     */
    default int getMaxNumberOfSegmentsInCache() {
        return MAX_NUMBER_OF_SEGMENTS_IN_CACHE;
    }

    /**
     * Returns the default disk I/O buffer size in bytes.
     *
     * @return default disk I/O buffer size in bytes
     */
    default int getDiskIoBufferSizeInBytes() {
        return DISK_IO_BUFFER_SIZE_IN_BYTES;
    }

    /**
     * Returns the default Bloom filter hash function count.
     *
     * @return default Bloom filter hash function count
     */
    default int getBloomFilterNumberOfHashFunctions() {
        return BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS;
    }

    /**
     * Returns the default Bloom filter index size in bytes.
     *
     * @return default Bloom filter size in bytes
     */
    default int getBloomFilterIndexSizeInBytes() {
        return BLOOM_FILTER_INDEX_SIZE_IN_BYTES;
    }

    /**
     * Returns the default Bloom filter false-positive probability.
     *
     * @return default false-positive probability
     */
    default double getBloomFilterProbabilityOfFalsePositive() {
        return BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE;
    }

    /**
     * Returns the default number of CPU threads used for index operations.
     *
     * @return default CPU thread count
     */
    default int getNumberOfThreads() {
        return NUMBER_OF_THREADS;
    }

    /**
     * Returns the default number of IO threads used by the async directory.
     *
     * @return default IO thread count
     */
    default int getNumberOfIoThreads() {
        return NUMBER_OF_IO_THREADS;
    }

    /**
     * Returns the default number of segment maintenance threads.
     *
     * @return default segment maintenance thread count
     */
    default int getNumberOfSegmentIndexMaintenanceThreads() {
        return DEFAULT_SEGMENT_INDEX_MAINTENANCE_THREADS;
    }

    /**
     * Returns the default number of split maintenance threads.
     *
     * @return default split maintenance thread count
     */
    default int getNumberOfIndexMaintenanceThreads() {
        return DEFAULT_INDEX_MAINTENANCE_THREADS;
    }

    /**
     * Returns the default busy backoff delay in milliseconds.
     *
     * @return default busy backoff in milliseconds
     */
    default int getIndexBusyBackoffMillis() {
        return DEFAULT_INDEX_BUSY_BACKOFF_MILLIS;
    }

    /**
     * Returns the default busy retry timeout in milliseconds.
     *
     * @return default busy retry timeout in milliseconds
     */
    default int getIndexBusyTimeoutMillis() {
        return DEFAULT_INDEX_BUSY_TIMEOUT_MILLIS;
    }

    /**
     * Returns whether auto maintenance is enabled by default.
     *
     * @return true when auto maintenance is enabled by default
     */
    default boolean isSegmentMaintenanceAutoEnabled() {
        return DEFAULT_SEGMENT_MAINTENANCE_AUTO_ENABLED;
    }

    /**
     * Returns whether MDC-based context logging is enabled by default.
     *
     * @return true when context logging is enabled by default
     */
    default boolean isContextLoggingEnabled() {
        return true;
    }

    /**
     * Returns the default encoding chunk filter chain.
     *
     * @return default encoding filters
     */
    default List<ChunkFilter> getEncodingChunkFilters() {
        return List.of(new ChunkFilterCrc32Writing(),
                new ChunkFilterMagicNumberWriting());
    }

    /**
     * Returns the default decoding chunk filter chain.
     *
     * @return default decoding filters
     */
    default List<ChunkFilter> getDecodingChunkFilters() {
        return List.of(new ChunkFilterMagicNumberValidation(),
                new ChunkFilterCrc32Validation());
    }

}
