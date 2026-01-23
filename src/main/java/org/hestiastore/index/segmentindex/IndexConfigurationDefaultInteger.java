package org.hestiastore.index.segmentindex;

/**
 * Define contract, that define index configuration.
 * 
 * @author honza
 *
 */
public class IndexConfigurationDefaultInteger
        implements IndexConfigurationContract {

    /** {@inheritDoc} */
    @Override
    public int getMaxNumberOfKeysInSegmentCache() {
        return 500_000;
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxNumberOfKeysInSegmentChunk() {
        return 1_000;
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxNumberOfKeysInCache() {
        return 5_000_000;
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxNumberOfKeysInSegment() {
        return 10_000_000;
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxNumberOfSegmentsInCache() {
        return 10;
    }

    /** {@inheritDoc} */
    @Override
    public int getDiskIoBufferSizeInBytes() {
        return 1024 * 1024;
    }

    /** {@inheritDoc} */
    @Override
    public int getBloomFilterNumberOfHashFunctions() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override
    public int getBloomFilterIndexSizeInBytes() {
        return 100_000;
    }

}
