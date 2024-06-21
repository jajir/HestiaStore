package com.coroptis.index.sst;

import java.util.Objects;

import com.coroptis.index.bloomfilter.BloomFilter;
import com.coroptis.index.scarceindex.ScarceIndex;
import com.coroptis.index.segment.SegmentCacheDataProvider;
import com.coroptis.index.segment.SegmentData;
import com.coroptis.index.segment.SegmentDeltaCache;

/**
 * Provide cached lazy loaded instances of segment data objects.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentDataImpl<K, V> implements SegmentData<K, V> {

    private final SegmentCacheDataProvider<K, V> dataProvider;

    private SegmentDeltaCache<K, V> deltaCache;
    private BloomFilter<K> bloomFilter;
    private ScarceIndex<K> scarceIndex;

    SegmentDataImpl(final SegmentCacheDataProvider<K, V> dataProvider) {
        this.dataProvider = Objects.requireNonNull(dataProvider);
    }

    @Override
    public SegmentDeltaCache<K, V> getSegmentDeltaCache() {
        if (deltaCache == null) {
            deltaCache = dataProvider.getSegmentDeltaCache();
        }
        return deltaCache;
    }

    @Override
    public BloomFilter<K> getBloomFilter() {
        if (bloomFilter == null) {
            bloomFilter = dataProvider.getBloomFilter();
        }
        return bloomFilter;
    }

    @Override
    public ScarceIndex<K> getScarceIndex() {
        if (scarceIndex == null) {
            scarceIndex = dataProvider.getScarceIndex();
        }
        return scarceIndex;
    }

}
