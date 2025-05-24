package com.hestiastore.index.sst;

import java.util.Objects;
import java.util.Optional;

import com.hestiastore.index.bloomfilter.BloomFilter;
import com.hestiastore.index.scarceindex.ScarceIndex;
import com.hestiastore.index.segment.SegmentData;
import com.hestiastore.index.segment.SegmentDataFactory;
import com.hestiastore.index.segment.SegmentDataProvider;
import com.hestiastore.index.segment.SegmentDeltaCache;
import com.hestiastore.index.segment.SegmentId;

public class SegmentDataProviderFromMainCache<K, V>
        implements SegmentDataProvider<K, V> {

    private final SegmentId id;
    private final SegmentDataCache<K, V> cache;
    private final SegmentDataFactory<K, V> segmentDataFactory;

    SegmentDataProviderFromMainCache(final SegmentId id,
            final SegmentDataCache<K, V> cache,
            final SegmentDataFactory<K, V> segmentDataFactory) {
        this.id = Objects.requireNonNull(id);
        this.cache = Objects.requireNonNull(cache);
        this.segmentDataFactory = Objects.requireNonNull(segmentDataFactory);
    }

    private SegmentData<K, V> getSegmentData() {
        final Optional<SegmentData<K, V>> oData = cache.getSegmentData(id);
        if (oData.isEmpty()) {
            final SegmentData<K, V> out = segmentDataFactory.getSegmentData();
            cache.put(id, out);
            return out;
        } else {
            return oData.get();
        }
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
        cache.getSegmentData(id).ifPresent(SegmentData::close);
        cache.invalidate(id);
    }

    @Override
    public boolean isLoaded() {
        return cache.isPresent(id);
    }

}
