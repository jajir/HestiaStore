package com.coroptis.index.sst;

/**
 * Define contract, that define index configuration.
 * 
 * @author honza
 *
 */
public interface BuilderConfiguration {

    long getMaxNumberOfKeysInSegmentCache();

    long getMaxNumberOfKeysInSegmentCacheDuringFlushing();

    int getMaxNumberOfKeysInSegmentIndexPage();

    int getMaxNumberOfKeysInCache();

    int getMaxNumberOfKeysInSegment();

    int getMaxNumberOfSegmentsInCache();

    int getIndexBufferSizeInBytes();

    int getBloomFilterNumberOfHashFunctions();

    int getBloomFilterIndexSizeInBytes();

}
