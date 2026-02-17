package org.hestiastore.index.segment;

import org.hestiastore.index.bloomfilter.BloomFilter;
import org.hestiastore.index.scarceindex.ScarceSegmentIndex;

/**
 * Provide access to main data object that are considerably large in memory.
 * Implementations use some sort of cache to minimize memory impact.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public interface SegmentResources<K, V> {

    /**
     * Returns the Bloom filter for this segment.
     *
     * @return Bloom filter instance
     */
    BloomFilter<K> getBloomFilter();

    /**
     * Returns the scarce index for this segment.
     *
     * @return scarce segment index
     */
    ScarceSegmentIndex<K> getScarceIndex();

    /**
     * Invalidates cached resources, closing any open handles.
     */
    void invalidate();
}
