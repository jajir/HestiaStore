package com.coroptis.index.segment;

import java.util.Objects;

import com.coroptis.index.bloomfilter.BloomFilter;
import com.coroptis.index.scarceindex.ScarceIndex;

/**
 * This provider, lazy load cached data objects.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentCacheDataProviderSimple<K, V>
        implements SegmentCacheDataProvider<K, V> {

    private final SegmentCacheDataDirectLoader<K, V> dataLoader;
    private SegmentDataLazyLoaded<K, V> dataImpl;

    SegmentCacheDataProviderSimple(
            final SegmentCacheDataDirectLoader<K, V> dataLoader) {
        this.dataLoader = Objects.requireNonNull(dataLoader);
        invalidate();
    }

    @Override
    public SegmentDeltaCache<K, V> getSegmentDeltaCache() {
        return dataImpl.getSegmentDeltaCache();
    }

    @Override
    public BloomFilter<K> getBloomFilter() {
        return dataImpl.getBloomFilter();
    }

    @Override
    public ScarceIndex<K> getScarceIndex() {
        return dataImpl.getScarceIndex();
    }

    @Override
    public void invalidate() {
        dataImpl = new SegmentDataLazyLoaded<>(dataLoader);
    }

    /**
     * It always return true. Even when cached data are not lazy loaded it's
     * fine to force to load them.
     */
    @Override
    public boolean isLoaded() {
        return true;
    }

}
