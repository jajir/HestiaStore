package org.hestiastore.index.sst;

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
    /**
     * Default maximum number of keys kept in the read-side cache that stores
     * results of point lookups. Implementations may override this constant
     * through {@link #getMaxNumberOfKeysInReadCache()}.
     */
    int MAX_NUMBER_OF_KEYS_IN_READ_CACHE = 100_000;
    int MAX_NUMBER_OF_KEYS_IN_SEGMENT = 10_000_000;
    int MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE = 10_000;
    int MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING = 20_000;
    int MAX_NUMBER_OF_KEYS_IN_SEGMENT_CHUNK = 1_000;
    int MAX_NUMBER_OF_SEGMENTS_IN_CACHE = 10;

    int BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS = 3;
    int BLOOM_FILTER_INDEX_SIZE_IN_BYTES = 5_000_000;
    double BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE = 0.01;

    int DISK_IO_BUFFER_SIZE_IN_BYTES = 1024 * 8;

    default int getMaxNumberOfKeysInSegmentCache() {
        return MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE;
    }

    default int getMaxNumberOfKeysInSegmentCacheDuringFlushing() {
        return getMaxNumberOfKeysInSegmentCache() * 2;
    }

    default int getMaxNumberOfKeysInSegmentChunk() {
        return MAX_NUMBER_OF_KEYS_IN_SEGMENT_CHUNK;
    }

    default int getMaxNumberOfKeysInCache() {
        return MAX_NUMBER_OF_KEYS_IN_CACHE;
    }

    default int getMaxNumberOfKeysInReadCache() {
        return MAX_NUMBER_OF_KEYS_IN_READ_CACHE;
    }

    default int getMaxNumberOfKeysInSegment() {
        return MAX_NUMBER_OF_KEYS_IN_SEGMENT;
    }

    default int getMaxNumberOfSegmentsInCache() {
        return MAX_NUMBER_OF_SEGMENTS_IN_CACHE;
    }

    default int getDiskIoBufferSizeInBytes() {
        return DISK_IO_BUFFER_SIZE_IN_BYTES;
    }

    default int getBloomFilterNumberOfHashFunctions() {
        return BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS;
    }

    default int getBloomFilterIndexSizeInBytes() {
        return BLOOM_FILTER_INDEX_SIZE_IN_BYTES;
    }

    default double getBloomFilterProbabilityOfFalsePositive() {
        return BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE;
    }

    default boolean isThreadSafe() {
        return false;
    }

    default boolean isContextLoggingEnabled() {
        return true;
    }

    default List<ChunkFilter> getEncodingChunkFilters() {
        return List.of(new ChunkFilterCrc32Writing(),
                new ChunkFilterMagicNumberWriting());
    }

    default List<ChunkFilter> getDecodingChunkFilters() {
        return List.of(new ChunkFilterMagicNumberValidation(),
                new ChunkFilterCrc32Validation());
    }

}
