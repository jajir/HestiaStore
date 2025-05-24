package com.hestiastore.index.segment;

import java.util.Objects;

import com.hestiastore.index.bloomfilter.BloomFilter;
import com.hestiastore.index.scarceindex.ScarceIndex;

/**
 * Provider of segment data. It's support invalidate segment data in memory.
 * 
 * @author honza
 *
 * @param <K> key type
 * @param <V> value type
 */
public class SegmentDataProviderSimple<K, V>
        implements SegmentDataProvider<K, V> {

    private final SegmentDataFactory<K, V> segmentDataFactory;
    private SegmentData<K, V> segmentData;

    SegmentDataProviderSimple(
            final SegmentDataFactory<K, V> segmentDataFactory) {
        this.segmentDataFactory = Objects.requireNonNull(segmentDataFactory,
                "segmentDataFactory cannot be null");
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
        segmentData = null;
    }

    private SegmentData<K, V> getSegmentData() {
        if (segmentData == null) {
            segmentData = segmentDataFactory.getSegmentData();
        }
        return segmentData;
    }

    /**
     * It always return true. Even when cached data are not lazy loaded it's
     * fine to force to load them.
     */
    @Override
    public boolean isLoaded() {
        return segmentData != null;
    }

}
