package org.hestiastore.index.segment;

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
    private final int maxNumberOfKeysInSegmentDeltaCache;

    public SegmentDeltaCacheController(final SegmentFiles<K, V> segmentFiles,
            final SegmentPropertiesManager segmentPropertiesManager,
            final SegmentDataProvider<K, V> segmentCacheDataProvider,
            final int maxNumberOfKeysInSegmentDeltaCache) {
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        this.segmentPropertiesManager = Vldtn.requireNonNull(
                segmentPropertiesManager, "segmentPropertiesManager");
        this.segmentCacheDataProvider = Vldtn.requireNonNull(
                segmentCacheDataProvider, "segmentCacheDataProvider");
        this.maxNumberOfKeysInSegmentDeltaCache = maxNumberOfKeysInSegmentDeltaCache;
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
                segmentPropertiesManager, segmentCacheDataProvider,
                maxNumberOfKeysInSegmentDeltaCache);
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

}
