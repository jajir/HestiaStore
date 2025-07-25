package org.hestiastore.index.segment;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.cache.UniqueCache;
import org.hestiastore.index.sorteddatafile.SortedDataFileWriter;

/**
 * Class collect unsorted data, sort them and finally write them into SST delta
 * file.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public final class SegmentDeltaCacheWriter<K, V> implements PairWriter<K, V> {

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
        this.segmentPropertiesManager = Vldtn.requireNonNull(
                segmentPropertiesManager, "segmentPropertiesManager");
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        this.uniqueCache = new UniqueCache<>(
                segmentFiles.getKeyTypeDescriptor().getComparator());
        this.segmentCacheDataProvider = Vldtn.requireNonNull(
                segmentCacheDataProvider, "segmentCacheDataProvider");
    }

    public long getNumberOfKeys() {
        return cx;
    }

    /**
     * Finally writes data to segment delta file and update numbed of keys in
     * delta cache.
     */
    @Override
    public void close() {
        // increase number of keys in cache
        final int keysInCache = uniqueCache.size();
        segmentPropertiesManager.increaseNumberOfKeysInDeltaCache(keysInCache);
        segmentPropertiesManager.flush();

        // store cache
        try (SortedDataFileWriter<K, V> writer = segmentFiles
                .getCacheSstFile(
                        segmentPropertiesManager.getAndIncreaseDeltaFileName())
                .openWriter()) {
            uniqueCache.getStream().forEach(pair -> {
                writer.write(pair);
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
