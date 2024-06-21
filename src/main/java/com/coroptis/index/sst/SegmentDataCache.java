package com.coroptis.index.sst;

import java.util.Objects;

import com.coroptis.index.cache.Cache;
import com.coroptis.index.cache.CacheLru;
import com.coroptis.index.segment.Segment;
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

    private final SegmentManager<K, V> segmentManager;

    SegmentDataCache(final SsstIndexConf conf,
            final SegmentManager<K, V> segmentManager) {
        this.segmentManager = Objects.requireNonNull(segmentManager);
        cache = new CacheLru<>(conf.getMaxNumberOfSegmentsInCache(),
                (segmenId, segmentData) -> {
                    // intentionally do nothing
                    segmentData = null;
                });
    }

    public SegmentData<K, V> getSegmenData(final SegmentId segmentId) {
        Objects.requireNonNull(segmentId);
        SegmentData<K, V> out = null;
        if (cache.get(segmentId).isEmpty()) {
            final Segment<K, V> segment = segmentManager.getSegment(segmentId);
            out = new SegmentDataImpl<>(segment.getCacheDataProvider());
            cache.put(segmentId, out);
        } else {
            out = cache.get(segmentId).get();
        }
        return out;
    }

    public void invalidate(final SegmentId id) {
        Objects.requireNonNull(id);
        cache.ivalidate(id);
    }

}
