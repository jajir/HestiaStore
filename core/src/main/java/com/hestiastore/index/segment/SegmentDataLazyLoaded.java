package com.hestiastore.index.segment;

import java.util.Objects;

import com.hestiastore.index.CloseableResource;
import com.hestiastore.index.bloomfilter.BloomFilter;
import com.hestiastore.index.scarceindex.ScarceIndex;

/**
 * Provide cached lazy loaded instances of segment data objects.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentDataLazyLoaded<K, V>
        implements SegmentData<K, V>, CloseableResource {

    private final SegmentDataSupplier<K, V> segmentDataSupplier;

    private SegmentDeltaCache<K, V> deltaCache;
    private BloomFilter<K> bloomFilter;
    private ScarceIndex<K> scarceIndex;

    public SegmentDataLazyLoaded(
            final SegmentDataSupplier<K, V> segmentDataSupplier) {
        this.segmentDataSupplier = Objects.requireNonNull(segmentDataSupplier);
    }

    @Override
    public SegmentDeltaCache<K, V> getSegmentDeltaCache() {
        if (deltaCache == null) {
            deltaCache = segmentDataSupplier.getSegmentDeltaCache();
        }
        return deltaCache;
    }

    @Override
    public BloomFilter<K> getBloomFilter() {
        if (bloomFilter == null) {
            bloomFilter = segmentDataSupplier.getBloomFilter();
        }
        return bloomFilter;
    }

    @Override
    public ScarceIndex<K> getScarceIndex() {
        if (scarceIndex == null) {
            scarceIndex = segmentDataSupplier.getScarceIndex();
        }
        return scarceIndex;
    }

    @Override
    public void close() {
        if (bloomFilter != null) {
            bloomFilter.close();
            bloomFilter = null;
        }
        if (deltaCache != null) {
            deltaCache.evictAll();
            deltaCache = null;
        }
        if (scarceIndex != null) {
            scarceIndex = null;
        }

    }

}
