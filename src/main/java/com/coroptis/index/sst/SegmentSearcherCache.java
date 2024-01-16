package com.coroptis.index.sst;

import java.util.Objects;
import java.util.Optional;

import com.coroptis.index.cache.Cache;
import com.coroptis.index.cache.CacheLru;
import com.coroptis.index.segment.Segment;
import com.coroptis.index.segment.SegmentId;
import com.coroptis.index.segment.SegmentSearcher;

/**
 * Cache for segment searchers. It's a SegmentId, SegmentSearcher map.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentSearcherCache<K, V> {

    final Cache<SegmentId, SegmentSearcher<K, V>> cache;

    private final SegmentManager<K, V> segmentManager;

    SegmentSearcherCache(final SsstIndexConf conf,
            final SegmentManager<K, V> segmentManager) {
        this.segmentManager = Objects.requireNonNull(segmentManager);
        cache = new CacheLru<>(conf.getMaxNumberOfSegmentsInCache(),
                (segmenId, segment) -> {
                    segment.close();
                });
    }

    public SegmentSearcher<K, V> getSegmenSearcher(final SegmentId segmentId) {
        Objects.requireNonNull(segmentId);
        SegmentSearcher<K, V> out = null;
        if (cache.get(segmentId).isEmpty()) {
            final Segment<K, V> segment = segmentManager.getSegment(segmentId);
            out = segment.openSearcher();
            cache.put(segmentId, out);
        } else {
            out = cache.get(segmentId).get();
        }
        return out;
    }

    public Optional<SegmentSearcher<K, V>> getOptionalSegmenSearcher(
            final SegmentId segmentId) {
        Objects.requireNonNull(segmentId);
        return cache.get(segmentId);
    }

}
