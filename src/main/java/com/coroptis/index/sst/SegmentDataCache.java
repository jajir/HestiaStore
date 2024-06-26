package com.coroptis.index.sst;

import java.util.Objects;
import java.util.Optional;

import com.coroptis.index.cache.Cache;
import com.coroptis.index.cache.CacheLru;
import com.coroptis.index.segment.SegmentData;
import com.coroptis.index.segment.SegmentId;

/**
 * Cache for segment searchers. It's a SegmentId, SegmentSearcher map.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentDataCache<K, V> {

    final Cache<SegmentId, SegmentData<K, V>> cache;

    SegmentDataCache(final SsstIndexConf conf) {
        cache = new CacheLru<>(conf.getMaxNumberOfSegmentsInCache(),
                (segmenId, segmentData) -> {
                    // intentionally do nothing
                    segmentData = null;
                });
    }

    public Optional<SegmentData<K, V>> getSegmentData(
            final SegmentId segmentId) {
        Objects.requireNonNull(segmentId);
        if (cache.get(segmentId).isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(cache.get(segmentId).get());
        }
    }

    public void put(final SegmentId segmentId, SegmentData<K, V> segmentData) {
        cache.put(segmentId, segmentData);
    }

    public void invalidate(final SegmentId id) {
        Objects.requireNonNull(id);
        cache.ivalidate(id);
    }

    public boolean isPresent(final SegmentId id) {
        return cache.get(id).isPresent();
    }

}
