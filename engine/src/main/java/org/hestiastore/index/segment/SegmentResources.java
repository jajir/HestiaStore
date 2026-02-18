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

    /**
     * Returns cumulative Bloom filter request count for this segment resource
     * lifecycle.
     *
     * @return cumulative request count
     */
    default long getBloomFilterRequestCount() {
        return 0L;
    }

    /**
     * Returns cumulative Bloom filter refused (negative) count.
     *
     * @return cumulative refused count
     */
    default long getBloomFilterRefusedCount() {
        return 0L;
    }

    /**
     * Returns cumulative Bloom filter positive response count.
     *
     * @return cumulative positive count
     */
    default long getBloomFilterPositiveCount() {
        return 0L;
    }

    /**
     * Returns cumulative Bloom filter false-positive count.
     *
     * @return cumulative false-positive count
     */
    default long getBloomFilterFalsePositiveCount() {
        return 0L;
    }
}
