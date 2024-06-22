package com.coroptis.index.segment;

import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.PairWriter;
import com.coroptis.index.cache.UniqueCache;
import com.coroptis.index.sstfile.SstFileWriter;

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

    private final UniqueCache<K, V> uniqueCache;
    private final SegmentPropertiesManager segmentPropertiesManager;
    private final SegmentCompacter<K, V> segmentCompacter;
    private final SegmentFiles<K, V> segmentFiles;
    private final SegmentCacheDataProvider<K, V> segmentCacheDataProvider;

    public SegmentWriter(final SegmentFiles<K, V> segmentFiles,
            final SegmentPropertiesManager segmentPropertiesManager,
            final SegmentCompacter<K, V> segmentCompacter,
            final SegmentCacheDataProvider<K, V> segmentCacheDataProvider) {
        this.segmentPropertiesManager = Objects
                .requireNonNull(segmentPropertiesManager);
        this.segmentFiles = Objects.requireNonNull(segmentFiles);

        this.uniqueCache = new UniqueCache<>(
                segmentFiles.getKeyTypeDescriptor().getComparator());

        this.segmentCompacter = Objects.requireNonNull(segmentCompacter);
        this.segmentCacheDataProvider = Objects.requireNonNull(
                segmentCacheDataProvider,
                "Segment cached data provider is required");
    }

    public PairWriter<K, V> openWriter() {
        return new PairWriter<K, V>() {

            private long cx = 0;

            @Override
            public void close() {
                closeWritingToCache();

                segmentCompacter.optionallyCompact();
            }

            @Override
            public void put(final Pair<K, V> pair) {
                uniqueCache.put(pair);
                cx++;
                if (segmentCacheDataProvider.isLoaded()) {
                    segmentCacheDataProvider.getSegmentDeltaCache().put(pair);
                }
                if (segmentCompacter.shouldBeCompactedDuringWriting(cx)) {
                    cx = 0;
                    segmentCacheDataProvider.invalidate();
                    closeWritingToCache();
                    segmentCompacter.forceCompact();
                }
            }

            private void closeWritingToCache() {
                // increase number of keys in cache
                final int keysInCache = uniqueCache.size();
                segmentPropertiesManager
                        .increaseNumberOfKeysInCache(keysInCache);
                segmentPropertiesManager.flush();

                // store cache
                try (final SstFileWriter<K, V> writer = segmentFiles
                        .getCacheSstFile(segmentPropertiesManager
                                .getAndIncreaseDeltaFileName())
                        .openWriter()) {
                    uniqueCache.getStream().forEach(pair -> {
                        writer.put(pair);
                    });
                }
                uniqueCache.clear();
            }
        };
    }

}
