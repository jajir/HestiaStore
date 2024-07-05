package com.coroptis.index.sst;

import java.util.Objects;
import java.util.Optional;

import com.coroptis.index.bloomfilter.BloomFilter;
import com.coroptis.index.scarceindex.ScarceIndex;
import com.coroptis.index.segment.Segment;
import com.coroptis.index.segment.SegmentData;
import com.coroptis.index.segment.SegmentDataLazyLoader;
import com.coroptis.index.segment.SegmentDataProvider;
import com.coroptis.index.segment.SegmentDeltaCache;
import com.coroptis.index.segment.SegmentId;

public class SegmentDataProviderFromMainCache<K, V>
        implements SegmentDataProvider<K, V> {

    private final SegmentId id;
    private final SegmentManager<K, V> segmentManager;
    private final SegmentDataCache<K, V> cache;

    SegmentDataProviderFromMainCache(final SegmentId id,
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
            final SegmentDataLazyLoader<K, V> out = new SegmentDataLazyLoader<>(
                    segment.getDataBuilder());
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
