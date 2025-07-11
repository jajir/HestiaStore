package org.hestiastore.index.sst;

import java.util.Optional;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.cache.Cache;
import org.hestiastore.index.cache.CacheLru;
import org.hestiastore.index.segment.SegmentData;
import org.hestiastore.index.segment.SegmentId;

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

    SegmentDataCache(final IndexConfiguration<K, V> conf) {
        cache = new CacheLru<>(conf.getMaxNumberOfSegmentsInCache(),
                (segmenId, segmentData) -> {
                    segmentData.close();
                    segmentData = null;
                });
    }

    public Optional<SegmentData<K, V>> getSegmentData(
            final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
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
        Vldtn.requireNonNull(id, "segmentId");
        cache.ivalidate(id);
    }

    public boolean isPresent(final SegmentId id) {
        return cache.get(id).isPresent();
    }

    public void invalidateAll() {
        cache.invalidateAll();
    }

}
