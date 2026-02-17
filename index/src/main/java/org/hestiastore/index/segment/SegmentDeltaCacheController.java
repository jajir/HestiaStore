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

    /**
     * Creates a controller for delta cache operations.
     *
     * @param segmentFiles segment file access wrapper
     * @param segmentPropertiesManager properties manager for segment metadata
     * @param segmentCacheDataProvider data provider for cached resources
     * @param maxNumberOfKeysInSegmentWriteCache write-cache size limit
     * @param maxNumberOfKeysInChunk maximum keys per chunk
     */
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

    /**
     * Returns the number of delta cache entries excluding tombstones.
     *
     * @return number of non-tombstone entries
     */
    public int getDeltaCacheSizeWithoutTombstones() {
        return Vldtn.requireNonNull(segmentCache, "segmentCache")
                .sizeWithoutTombstones();
    }

    /**
     * Returns the total number of delta cache entries.
     *
     * @return number of cached entries
     */
    public int getDeltaCacheSize() {
        return Vldtn.requireNonNull(segmentCache, "segmentCache").size();
    }

    /**
     * Opens a writer for delta cache files.
     *
     * @return delta cache writer
     */
    public SegmentDeltaCacheWriter<K, V> openWriter() {
        return new SegmentDeltaCacheWriter<>(segmentFiles,
                segmentPropertiesManager,
                maxNumberOfKeysInSegmentWriteCache, maxNumberOfKeysInChunk);
    }

    /**
     * Sets the in-memory segment cache used for fast stats and clearing.
     *
     * @param segmentCache segment cache instance
     */
    void setSegmentCache(final SegmentCache<K, V> segmentCache) {
        this.segmentCache = Vldtn.requireNonNull(segmentCache, "segmentCache");
    }

    /**
     * Clears delta cache files and invalidates cached resources.
     */
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
        segmentPropertiesManager.clearCacheDeltaFileNamesCouter();
    }

    /**
     * Clears delta cache files while preserving the in-memory write cache.
     */
    void clearPreservingWriteCache() {
        segmentCacheDataProvider.invalidate();
        if (segmentCache != null) {
            segmentCache.clearDeltaCachePreservingWriteCache();
        }
        segmentPropertiesManager.getCacheDeltaFileNames()
                .forEach(segmentCacheDeltaFile -> {
                    segmentFiles.optionallyDeleteFile(segmentCacheDeltaFile);
                });
        segmentPropertiesManager.clearCacheDeltaFileNamesCouter();
    }

}
