package org.hestiastore.index.sst;

import java.util.Optional;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bloomfilter.BloomFilter;
import org.hestiastore.index.scarceindex.ScarceIndex;
import org.hestiastore.index.segment.SegmentData;
import org.hestiastore.index.segment.SegmentDataFactory;
import org.hestiastore.index.segment.SegmentDataProvider;
import org.hestiastore.index.segment.SegmentDeltaCache;
import org.hestiastore.index.segment.SegmentId;

public class SegmentDataProviderFromMainCache<K, V>
        implements SegmentDataProvider<K, V> {

    private final SegmentId id;
    private final SegmentDataCache<K, V> cache;
    private final SegmentDataFactory<K, V> segmentDataFactory;

    SegmentDataProviderFromMainCache(final SegmentId id,
            final SegmentDataCache<K, V> cache,
            final SegmentDataFactory<K, V> segmentDataFactory) {
        this.id = Vldtn.requireNonNull(id, "segmentId");
        this.cache = Vldtn.requireNonNull(cache, "cache");
        this.segmentDataFactory = Vldtn.requireNonNull(segmentDataFactory,
                "segmentDataFactory");
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
        cache.invalidate(id);
    }

    @Override
    public boolean isLoaded() {
        return cache.isPresent(id);
    }

}
