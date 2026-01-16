package org.hestiastore.index.segment;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkentryfile.ChunkEntryFile;
import org.hestiastore.index.sorteddatafile.SortedDataFile;

/**
 * Loads on-disk delta cache entries into the in-memory segment cache.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentDeltaCacheLoader<K, V> {

    private final SegmentFiles<K, V> segmentFiles;
    private final SegmentPropertiesManager segmentPropertiesManager;

    /**
     * Creates a loader for delta cache files.
     *
     * @param segmentFiles segment file access wrapper
     * @param segmentPropertiesManager properties manager for delta file names
     */
    SegmentDeltaCacheLoader(final SegmentFiles<K, V> segmentFiles,
            final SegmentPropertiesManager segmentPropertiesManager) {
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        this.segmentPropertiesManager = Vldtn.requireNonNull(
                segmentPropertiesManager, "segmentPropertiesManager");
    }

    /**
     * Loads cache and delta files into the provided segment cache.
     *
     * @param segmentCache target in-memory cache
     */
    void loadInto(final SegmentCache<K, V> segmentCache) {
        Vldtn.requireNonNull(segmentCache, "segmentCache");
        loadFromCacheDataFile(segmentCache);
        segmentPropertiesManager.getCacheDeltaFileNames()
                .forEach(fileName -> loadFromDeltaFile(segmentCache, fileName));
    }

    private void loadFromCacheDataFile(final SegmentCache<K, V> segmentCache) {
        final SortedDataFile<K, V> cacheFile = segmentFiles.getCacheDataFile();
        try (EntryIterator<K, V> iterator = cacheFile.openIterator()) {
            while (iterator.hasNext()) {
                segmentCache.putToDeltaCache(iterator.next());
            }
        }
    }

    private void loadFromDeltaFile(final SegmentCache<K, V> segmentCache,
            final String segmentDeltaFileName) {
        final ChunkEntryFile<K, V> dataFile = segmentFiles
                .getDeltaCacheChunkEntryFile(segmentDeltaFileName);
        try (EntryIterator<K, V> iterator = dataFile.openIterator()) {
            while (iterator.hasNext()) {
                segmentCache.putToDeltaCache(iterator.next());
            }
        }
    }
}
