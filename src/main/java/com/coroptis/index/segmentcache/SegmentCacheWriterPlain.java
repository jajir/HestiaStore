package com.coroptis.index.segmentcache;

import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.PairWriter;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.segment.SegmentFiles;
import com.coroptis.index.segment.SegmentStatsManager;

/**
 * This implementation expect that segment cache is not loaded into memory.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentCacheWriterPlain<K, V> implements SegmentCacheWriter<K, V> {

    private final SegmentCache<K, V> segmentCache;

    private final SegmentStatsManager segmentStatsManager;

    SegmentCacheWriterPlain(final SegmentFiles<K, V> segmentFiles,
            final TypeDescriptor<K> keyTypeDescriptor,
            final SegmentStatsManager segmentStatsManager) {
        this.segmentStatsManager = Objects.requireNonNull(segmentStatsManager);
        this.segmentCache = new SegmentCache<>(keyTypeDescriptor, segmentFiles);
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
