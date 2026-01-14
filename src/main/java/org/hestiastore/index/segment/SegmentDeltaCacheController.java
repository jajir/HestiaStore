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
final class SegmentDeltaCacheController<K, V> {

    private final SegmentFiles<K, V> segmentFiles;
    private final SegmentPropertiesManager segmentPropertiesManager;
    private final SegmentResources<K, V> segmentCacheDataProvider;
    private final int maxNumberOfKeysInSegmentWriteCache;
    private final int maxNumberOfKeysInChunk;
    private SegmentCache<K, V> segmentCache;

    public SegmentDeltaCacheController(final SegmentFiles<K, V> segmentFiles,
            final SegmentPropertiesManager segmentPropertiesManager,
            final SegmentResources<K, V> segmentCacheDataProvider,
            final int maxNumberOfKeysInSegmentWriteCache,
            final int maxNumberOfKeysInChunk) {
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        this.segmentPropertiesManager = Vldtn.requireNonNull(
                segmentPropertiesManager, "segmentPropertiesManager");
        this.segmentCacheDataProvider = Vldtn.requireNonNull(
                segmentCacheDataProvider, "segmentCacheDataProvider");
        this.maxNumberOfKeysInSegmentWriteCache = maxNumberOfKeysInSegmentWriteCache;
        this.maxNumberOfKeysInChunk = maxNumberOfKeysInChunk;
    }

    public SegmentDeltaCache<K, V> getDeltaCache() {
        return segmentCacheDataProvider.getSegmentDeltaCache();
    }

    public int getDeltaCacheSizeWithoutTombstones() {
        if (segmentCache != null) {
            return segmentCache.sizeWithoutTombstones();
        }
        return getDeltaCache().sizeWithoutTombstones();
    }

    public int getDeltaCacheSize() {
        if (segmentCache != null) {
            return segmentCache.size();
        }
        return getDeltaCache().size();
    }

    public SegmentDeltaCacheWriter<K, V> openWriter() {
        return new SegmentDeltaCacheWriter<>(segmentFiles,
                segmentPropertiesManager, segmentCacheDataProvider,
                maxNumberOfKeysInSegmentWriteCache, maxNumberOfKeysInChunk);
    }

    void setSegmentCache(final SegmentCache<K, V> segmentCache) {
        this.segmentCache = Vldtn.requireNonNull(segmentCache, "segmentCache");
    }

    public void clear() {
        // Clearing the delta cache means the on-disk view has changed
        // (compaction
        // or segment replacement). Any cached heavy-weight structures such as
        // the
        // Bloom filter and scarce index must be dropped as well to avoid stale
        // lookups returning false negatives.
        segmentCacheDataProvider.invalidate();
        if (segmentCache != null) {
            segmentCache.evictAll();
        }
        segmentPropertiesManager.getCacheDeltaFileNames()
                .forEach(segmentCacheDeltaFile -> {
                    segmentFiles.optionallyDeleteFile(segmentCacheDeltaFile);
                });
        segmentFiles.optionallyDeleteFile(segmentFiles.getCacheFileName());
        segmentPropertiesManager.clearCacheDeltaFileNamesCouter();
    }

    void clearPreservingWriteCache() {
        segmentCacheDataProvider.invalidate();
        if (segmentCache != null) {
            segmentCache.clearDeltaCachePreservingWriteCache();
        }
        segmentPropertiesManager.getCacheDeltaFileNames()
                .forEach(segmentCacheDeltaFile -> {
                    segmentFiles.optionallyDeleteFile(segmentCacheDeltaFile);
                });
        segmentFiles.optionallyDeleteFile(segmentFiles.getCacheFileName());
        segmentPropertiesManager.clearCacheDeltaFileNamesCouter();
    }

}
