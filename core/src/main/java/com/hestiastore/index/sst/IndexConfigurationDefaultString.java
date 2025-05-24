package com.hestiastore.index.sst;

/**
 * Define contract, that define index configuration.
 * 
 * @author honza
 *
 */
public class IndexConfigurationDefaultString
        implements IndexConfigurationDefault {

    @Override
    public long getMaxNumberOfKeysInSegmentCache() {
        return 500_000;
    }

    @Override
    public long getMaxNumberOfKeysInSegmentCacheDuringFlushing() {
        return getMaxNumberOfKeysInCache() * 2;
    }

    @Override
    public int getMaxNumberOfKeysInSegmentIndexPage() {
        return 10_000;
    }

    @Override
    public int getMaxNumberOfKeysInCache() {
        return 5_000_000;
    }

    @Override
    public int getMaxNumberOfKeysInSegment() {
        return 10_000_000;
    }

    @Override
    public int getMaxNumberOfSegmentsInCache() {
        return 10;
    }

    @Override
    public int getDiskIoBufferSizeInBytes() {
        return 1024 * 1024;
    }

    @Override
    public int getBloomFilterNumberOfHashFunctions() {
        return 2;
    }

    @Override
    public int getBloomFilterIndexSizeInBytes() {
        return 1_000_000;
    }

}
