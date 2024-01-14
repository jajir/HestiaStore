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
    private final VersionController versionController;
    private final SegmentCompacter<K, V> segmentCompacter;
    private final SegmentFiles<K, V> segmentFiles;

    private SegmentSearcher<K, V> segmentSearcher = null;

    public SegmentWriter(final SegmentFiles<K, V> segmentFiles,
            final SegmentPropertiesManager segmentPropertiesManager,
            final VersionController versionController,
            final SegmentCompacter<K, V> segmentCompacter) {
        this(segmentFiles, segmentPropertiesManager, versionController,
                segmentCompacter, null);
    }

    public SegmentWriter(final SegmentFiles<K, V> segmentFiles,
            final SegmentPropertiesManager segmentPropertiesManager,
            final VersionController versionController,
            final SegmentCompacter<K, V> segmentCompacter,
            SegmentSearcher<K, V> segmentSearcher) {
        this.segmentPropertiesManager = Objects
                .requireNonNull(segmentPropertiesManager);
        this.segmentFiles = Objects.requireNonNull(segmentFiles);

        this.uniqueCache = new UniqueCache<>(
                segmentFiles.getKeyTypeDescriptor().getComparator());

        this.versionController = Objects.requireNonNull(versionController);
        this.segmentCompacter = Objects.requireNonNull(segmentCompacter);
    }

    public PairWriter<K, V> openWriter() {
        return new PairWriter<K, V>() {

            private long cx = 0;

            @Override
            public void close() {
                if (segmentSearcher == null) {
                    versionController.changeVersion();
                }

                closeWritingToCache();

                if (segmentCompacter.optionallyCompact()) {
                    versionController.changeVersion();
                }

            }

            @Override
            public void put(final Pair<K, V> pair) {
                uniqueCache.put(pair);
                cx++;
                if (segmentSearcher != null) {
                    segmentSearcher.addPairIntoCache(pair);
                }
                if (segmentCompacter.shouldBeCompacted(cx)) {
                    cx = 0;
                    segmentSearcher = null;
                    closeWritingToCache();
                    versionController.changeVersion();
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
