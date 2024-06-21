package com.coroptis.index.sst;

import java.util.Objects;

import com.coroptis.index.bloomfilter.BloomFilter;
import com.coroptis.index.scarceindex.ScarceIndex;
import com.coroptis.index.segment.SegmentCacheDataProvider;
import com.coroptis.index.segment.SegmentData;
import com.coroptis.index.segment.SegmentDeltaCache;
import com.coroptis.index.segment.SegmentId;

public class SegmentCacheDataProviderImpl<K, V>
        implements SegmentCacheDataProvider<K, V> {

    private final SegmentId id;
    private final SegmentDataCache<K, V> cache;

    SegmentCacheDataProviderImpl(final SegmentId id,
            final SegmentDataCache<K, V> cache) {
        this.id = Objects.requireNonNull(id);
        this.cache = Objects.requireNonNull(cache);
    }

    private SegmentData<K, V> getSegmentData() {
        return cache.getSegmenData(id);
    }

    @Override
    public SegmentDeltaCache<K, V> getSegmentDeltaCache() {
        return getSegmentData().getSegmentDeltaCache();
    }

    @Override
    public BloomFilter<K> getBloomFilter() {
        return getSegmentData().getBloomFilter();
    }

    @Override
    public ScarceIndex<K> getScarceIndex() {
        return getSegmentData().getScarceIndex();
    }

    @Override
    public void invalidate() {
        // TODO Auto-generated method stub

    }

}
