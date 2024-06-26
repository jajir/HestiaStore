package com.coroptis.index.sst;

import java.util.Objects;
import java.util.Optional;

import com.coroptis.index.bloomfilter.BloomFilter;
import com.coroptis.index.scarceindex.ScarceIndex;
import com.coroptis.index.segment.Segment;
import com.coroptis.index.segment.SegmentCacheDataProvider;
import com.coroptis.index.segment.SegmentData;
import com.coroptis.index.segment.SegmentDataLazyLoaded;
import com.coroptis.index.segment.SegmentDeltaCache;
import com.coroptis.index.segment.SegmentId;

public class SegmentCacheDataProviderFromMainCache<K, V>
        implements SegmentCacheDataProvider<K, V> {

    private final SegmentId id;
    private final SegmentManager<K, V> segmentManager;
    private final SegmentDataCache<K, V> cache;

    SegmentCacheDataProviderFromMainCache(final SegmentId id,
            final SegmentManager<K, V> segmentManager,
            final SegmentDataCache<K, V> cache) {
        this.id = Objects.requireNonNull(id);
        this.segmentManager = Objects.requireNonNull(segmentManager);
        this.cache = Objects.requireNonNull(cache);
    }

    private SegmentData<K, V> getSegmentData() {
        final Optional<SegmentData<K, V>> oData = cache.getSegmentData(id);
        if (oData.isEmpty()) {
            final Segment<K, V> segment = segmentManager.getSegment(id);
            final SegmentDataLazyLoaded<K, V> out = new SegmentDataLazyLoaded<>(
                    segment.getCacheDataProvider());
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
        cache.invalidate(id);
    }

    @Override
    public boolean isLoaded() {
        return cache.isPresent(id);
    }

}
