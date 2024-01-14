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

    private final UniqueCache<K, V> segmentCache;
    private final SegmentPropertiesManager segmentPropertiesManager;
    private final VersionController versionController;
    private final SegmentCompacter<K, V> segmentCompacter;
    private final SegmentFiles<K, V> segmentFiles;

    public SegmentWriter(final SegmentFiles<K, V> segmentFiles,
            final SegmentPropertiesManager segmentPropertiesManager,
            final VersionController versionController,
            final SegmentCompacter<K, V> segmentCompacter) {
        this.segmentPropertiesManager = Objects
                .requireNonNull(segmentPropertiesManager);
        this.segmentFiles = Objects.requireNonNull(segmentFiles);

        this.segmentCache = new UniqueCache<>(
                segmentFiles.getKeyTypeDescriptor().getComparator());

        this.versionController = Objects.requireNonNull(versionController);
        this.segmentCompacter = Objects.requireNonNull(segmentCompacter);
    }

    public PairWriter<K, V> openWriter() {
        return new PairWriter<K, V>() {
            
            private long cx = 0;
            
            @Override
            public void close() {
                versionController.changeVersion();

                // increase number of keys in cache
                final int keysInCache = segmentCache.size();
                segmentPropertiesManager
                        .increaseNumberOfKeysInCache(keysInCache);
                segmentPropertiesManager.flush();

                // store cache
                try (final SstFileWriter<K, V> writer = segmentFiles
                        .getCacheSstFile(segmentPropertiesManager
                                .getAndIncreaseDeltaFileName())
                        .openWriter()) {
                    segmentCache.getStream().forEach(pair -> {
                        writer.put(pair);
                    });
                }

                if (segmentCompacter.optionallyCompact()) {
//                    versionController.changeVersion();
                }

            }

            @Override
            public void put(final Pair<K, V> pair) {
                segmentCache.put(pair);
                segmentCompacter.optionallyCompact(cx);
                cx++;
            }
        };
    }

}
