package com.coroptis.index.sst;

/**
 * Define contract, that define index configuration.
 * 
 * @author honza
 *
 */
public interface IndexConfigurationDefault {

    final static int MAX_NUMBER_OF_KEYS_IN_CACHE = 4321;
    final static int MAX_NUMBER_OF_KEYS_IN_SEGMENT = 10_000_000;
    final static long MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE = 10_000;
    final static long MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING = 20_000;
    final static int MAX_NUMBER_OF_KEYS_IN_SEGMENT_INDEX_PAGE = 1_000;
    final static int MAX_NUMBER_OF_SEGMENTS_IN_CACHE = 10;

    final static int BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS = 3;
    final static int BLOOM_FILTER_INDEX_SIZE_IN_BYTES = 5_000_000;
    final static double BLOOM_FILTER_PROBABILITY_OF_FALSE_POSITIVE = 0.01;

    final static int DISK_IO_BUFFER_SIZE_IN_BYTES = 1024 * 8;

    default long getMaxNumberOfKeysInSegmentCache() {
        return MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE;
    }

    default long getMaxNumberOfKeysInSegmentCacheDuringFlushing() {
        return MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING;
    }

    default int getMaxNumberOfKeysInSegmentIndexPage() {
        return MAX_NUMBER_OF_KEYS_IN_SEGMENT_INDEX_PAGE;
    }

    default int getMaxNumberOfKeysInCache() {
        return MAX_NUMBER_OF_KEYS_IN_CACHE;
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

    default boolean isLogEnabled() {
        return false;
    }

}
