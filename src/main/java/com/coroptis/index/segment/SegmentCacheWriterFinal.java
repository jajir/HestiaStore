package com.coroptis.index.segment;

import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.PairWriter;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.segmentcache.SegmentCache;

/**
 * This implementation expect that segment cache is not loaded into memory.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentCacheWriterFinal<K, V> {

    private final SegmentCache<K, V> segmentCache;
    private final SegmentStatsManager segmentStatsManager;
    private final VersionController versionController;
    private final SegmentCompacter<K, V> segmentCompacter;

    public SegmentCacheWriterFinal(final SegmentFiles<K, V> segmentFiles,
            final TypeDescriptor<K> keyTypeDescriptor,
            final SegmentStatsManager segmentStatsManager,
            final VersionController versionController,
            final SegmentCompacter<K, V> segmentCompacter) {
        this.segmentStatsManager = Objects.requireNonNull(segmentStatsManager);
        this.segmentCache = new SegmentCache<>(keyTypeDescriptor, segmentFiles);
        this.versionController = Objects.requireNonNull(versionController);
        this.segmentCompacter = Objects.requireNonNull(segmentCompacter);
    }

    public PairWriter<K, V> openWriter() {
        return new PairWriter<K, V>() {

            @Override
            public void close() {
                final int keysInCache = segmentCache.flushCache();
                segmentStatsManager.setNumberOfKeysInCache(keysInCache);
                segmentStatsManager.flush();
                segmentCompacter.optionallyCompact();
                versionController.changeVersion();
            }

            @Override
            public void put(final Pair<K, V> pair) {
                segmentCache.put(pair);
            }
        };
    }

}
