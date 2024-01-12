package com.coroptis.index.segment;

import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.PairWriter;
import com.coroptis.index.datatype.TypeDescriptor;

/**
 * Allows to add data to segment. When searcher is in memory and number of added
 * keys doesn't exceed limit than it could work without invalidating cache and
 * searcher object..
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentWriter<K, V> {

    private final SegmentCache<K, V> segmentCache;
    private final SegmentPropertiesManager segmenPropertiesManager;
    private final VersionController versionController;
    private final SegmentCompacter<K, V> segmentCompacter;

    public SegmentWriter(final SegmentFiles<K, V> segmentFiles,
            final TypeDescriptor<K> keyTypeDescriptor,
            final SegmentPropertiesManager segmentStatsManager,
            final VersionController versionController,
            final SegmentCompacter<K, V> segmentCompacter) {
        this.segmenPropertiesManager = Objects
                .requireNonNull(segmentStatsManager);
        this.segmentCache = new SegmentCache<>(keyTypeDescriptor, segmentFiles);
        this.versionController = Objects.requireNonNull(versionController);
        this.segmentCompacter = Objects.requireNonNull(segmentCompacter);
    }

    public PairWriter<K, V> openWriter() {
        return new PairWriter<K, V>() {

            @Override
            public void close() {
                final int keysInCache = segmentCache.flushCache();
                segmenPropertiesManager.setNumberOfKeysInCache(keysInCache);
                segmenPropertiesManager.flush();
                if (segmentCompacter.optionallyCompact()) {
                    versionController.changeVersion();
                }
            }

            @Override
            public void put(final Pair<K, V> pair) {
                segmentCache.put(pair);
            }
        };
    }

}
