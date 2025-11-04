package org.hestiastore.index.segment;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;

/**
 * Provide ultimate access to delta cache and related operations
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public final class SegmentDeltaCacheController<K, V> {

    private final SegmentFiles<K, V> segmentFiles;
    private final SegmentPropertiesManager segmentPropertiesManager;
    private final SegmentDataProvider<K, V> segmentCacheDataProvider;

    public SegmentDeltaCacheController(final SegmentFiles<K, V> segmentFiles,
            final SegmentPropertiesManager segmentPropertiesManager,
            final SegmentDataProvider<K, V> segmentCacheDataProvider) {
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        this.segmentPropertiesManager = Vldtn.requireNonNull(
                segmentPropertiesManager, "segmentPropertiesManager");
        this.segmentCacheDataProvider = Vldtn.requireNonNull(
                segmentCacheDataProvider, "segmentCacheDataProvider");
    }

    public SegmentDeltaCache<K, V> getDeltaCache() {
        return segmentCacheDataProvider.getSegmentDeltaCache();
    }

    public int getDeltaCacheSizeWithoutTombstones() {
        return getDeltaCache().sizeWithoutTombstones();
    }

    public int getDeltaCacheSize() {
        return getDeltaCache().size();
    }

    public SegmentDeltaCacheWriter<K, V> openWriter() {
        return new SegmentDeltaCacheWriter<>(segmentFiles,
                segmentPropertiesManager, segmentCacheDataProvider);
    }

    public void clear() {
        if (segmentCacheDataProvider.isLoaded()) {
            getDeltaCache().evictAll();
        }
        segmentPropertiesManager.getCacheDeltaFileNames()
                .forEach(segmentCacheDeltaFile -> {
                    segmentFiles.deleteFile(segmentCacheDeltaFile);
                });
        segmentFiles.optionallyDeleteFile(segmentFiles.getCacheFileName());
        segmentPropertiesManager.clearCacheDeltaFileNamesCouter();
    }

    /**
     * Iterator will provide correct data event when some data are written to
     * delta cache.
     * 
     * @return
     */
    public EntryIterator<K, V> getSortedIterator() {
        return new SegmentDeltaCacheEntryIterator<>(
                getDeltaCache().getSortedKeys(), this);
    }
}
