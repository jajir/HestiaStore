package org.hestiastore.index.segmentindex.configuration.defaults;

import org.hestiastore.index.segmentindex.configuration.user.IndexConfigurationContract;
import org.hestiastore.index.segmentindex.configuration.user.IndexBloomFilterConfiguration;
import org.hestiastore.index.segmentindex.configuration.user.IndexIoConfiguration;
import org.hestiastore.index.segmentindex.configuration.user.IndexSegmentConfiguration;

/**
 * Default configuration values for String keys.
 */
public class IndexConfigurationDefaultString
        implements IndexConfigurationContract {

    /** {@inheritDoc} */
    @Override
    public IndexSegmentConfiguration segment() {
        return new IndexSegmentConfiguration(10_000_000, 10_000, 500_000, 10,
                DEFAULT_DELTA_CACHE_FILE_LIMIT);
    }

    /** {@inheritDoc} */
    @Override
    public IndexIoConfiguration io() {
        return new IndexIoConfiguration(1024 * 1024);
    }

    /** {@inheritDoc} */
    @Override
    public IndexBloomFilterConfiguration bloomFilter() {
        return new IndexBloomFilterConfiguration(2, 1_000_000,
                DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_PROBABILITY);
    }

}
