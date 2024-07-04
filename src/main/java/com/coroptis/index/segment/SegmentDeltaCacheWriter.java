package com.coroptis.index.segment;

import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.PairWriter;
import com.coroptis.index.cache.UniqueCache;
import com.coroptis.index.sstfile.SstFileWriter;

/**
 * Class collect unsorted data, sort them and finally write them into SST delta
 * file.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentDeltaCacheWriter<K, V> implements PairWriter<K, V> {

    /**
     * Cache will contains data written into this delta file.
     */
    private final UniqueCache<K, V> uniqueCache;

    private final SegmentPropertiesManager segmentPropertiesManager;
    private final SegmentFiles<K, V> segmentFiles;
    private final SegmentDataProvider<K, V> segmentCacheDataProvider;

    /**
     * How many keys was added to delta cache.
     * 
     * Consider using this number, it could be higher. Because of this delta
     * file will contains update command or tombstones.
     */
    private long cx = 0;

    public SegmentDeltaCacheWriter(final SegmentFiles<K, V> segmentFiles,
            final SegmentPropertiesManager segmentPropertiesManager,
            final SegmentDataProvider<K, V> segmentCacheDataProvider) {
        this.segmentPropertiesManager = Objects
                .requireNonNull(segmentPropertiesManager);
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        this.uniqueCache = new UniqueCache<>(
                segmentFiles.getKeyTypeDescriptor().getComparator());
        this.segmentCacheDataProvider = Objects.requireNonNull(
                segmentCacheDataProvider,
                "Segment cached data provider is required");
    }

    public long getNumberOfKeys() {
        return cx;
    }

    @Override
    public void close() {
        // increase number of keys in cache
        final int keysInCache = uniqueCache.size();
        segmentPropertiesManager.increaseNumberOfKeysInCache(keysInCache);
        segmentPropertiesManager.flush();

        // store cache
        try (final SstFileWriter<K, V> writer = segmentFiles
                .getCacheSstFile(
                        segmentPropertiesManager.getAndIncreaseDeltaFileName())
                .openWriter()) {
            uniqueCache.getStream().forEach(pair -> {
                writer.put(pair);
            });
        }
        uniqueCache.clear();
    }

    @Override
    public void put(Pair<K, V> pair) {
        uniqueCache.put(pair);
        cx++;
        if (segmentCacheDataProvider.isLoaded()) {
            segmentCacheDataProvider.getSegmentDeltaCache().put(pair);
        }
    }

}
