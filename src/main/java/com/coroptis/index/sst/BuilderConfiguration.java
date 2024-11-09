package com.coroptis.index.sst;

/**
 * Define contract, that define index configuration.
 * 
 * @author honza
 *
 */
public interface BuilderConfiguration {

    final static double DEFAULT_PROBABILITY_OF_FALSE_POSITIVE = 0.01;

    long getMaxNumberOfKeysInSegmentCache();

    long getMaxNumberOfKeysInSegmentCacheDuringFlushing();

    int getMaxNumberOfKeysInSegmentIndexPage();

    int getMaxNumberOfKeysInCache();

    int getMaxNumberOfKeysInSegment();

    int getMaxNumberOfSegmentsInCache();

    int getIndexBufferSizeInBytes();

    int getBloomFilterNumberOfHashFunctions();

    default double getBloomFilterProbabilityOfFalsePositive() {
        return DEFAULT_PROBABILITY_OF_FALSE_POSITIVE;
    }

    int getBloomFilterIndexSizeInBytes();


}
