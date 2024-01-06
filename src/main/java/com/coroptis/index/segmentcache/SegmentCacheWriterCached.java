package com.coroptis.index.segmentcache;

import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.PairWriter;
import com.coroptis.index.segment.SegmentFiles;
import com.coroptis.index.segment.SegmentStatsManager;

/**
 * This implementation expect that segment cache is already loaded in memory.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentCacheWriterCached<K, V>
        implements SegmentCacheWriter<K, V> {

    private final SegmentCache<K, V> segmentCache;

    private final SegmentStatsManager segmentStatsManager;

    SegmentCacheWriterCached(final SegmentFiles<K, V> segmentFiles,
            final SegmentCache<K, V> segmentCache,
            final SegmentStatsManager segmentStatsManager) {
        this.segmentStatsManager = Objects.requireNonNull(segmentStatsManager);
        this.segmentCache = Objects.requireNonNull(segmentCache);
    }

    @Override
    public PairWriter<K, V> openWriter() {
        return new PairWriter<K, V>() {

            @Override
            public void close() {
                final int keysInCache = segmentCache.flushCache();
                segmentStatsManager.setNumberOfKeysInCache(keysInCache);
                segmentStatsManager.flush();
            }

            @Override
            public void put(final Pair<K, V> pair) {
                segmentCache.put(pair);
            }
        };
    }

}
