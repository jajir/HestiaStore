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
    private final SegmentResources<K, V> segmentCacheDataProvider;
    private final int maxNumberOfKeysInSegmentDeltaCache;
    private final int maxNumberOfKeysInSegmentWriteCache;
    private final int maxNumberOfKeysInChunk;

    public SegmentDeltaCacheController(final SegmentFiles<K, V> segmentFiles,
            final SegmentPropertiesManager segmentPropertiesManager,
            final SegmentResources<K, V> segmentCacheDataProvider,
            final int maxNumberOfKeysInSegmentDeltaCache,
            final int maxNumberOfKeysInSegmentWriteCache,
            final int maxNumberOfKeysInChunk) {
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        this.segmentPropertiesManager = Vldtn.requireNonNull(
                segmentPropertiesManager, "segmentPropertiesManager");
        this.segmentCacheDataProvider = Vldtn.requireNonNull(
                segmentCacheDataProvider, "segmentCacheDataProvider");
        this.maxNumberOfKeysInSegmentDeltaCache = maxNumberOfKeysInSegmentDeltaCache;
        this.maxNumberOfKeysInSegmentWriteCache = maxNumberOfKeysInSegmentWriteCache;
        this.maxNumberOfKeysInChunk = maxNumberOfKeysInChunk;
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
                maxNumberOfKeysInSegmentWriteCache, maxNumberOfKeysInChunk);
    }

    public void clear() {
        // Clearing the delta cache means the on-disk view has changed (compaction
        // or segment replacement). Any cached heavy-weight structures such as the
        // Bloom filter and scarce index must be dropped as well to avoid stale
        // lookups returning false negatives.
        segmentCacheDataProvider.invalidate();
        segmentPropertiesManager.getCacheDeltaFileNames()
                .forEach(segmentCacheDeltaFile -> {
                    segmentFiles.deleteFile(segmentCacheDeltaFile);
                });
        segmentFiles.optionallyDeleteFile(segmentFiles.getCacheFileName());
        segmentPropertiesManager.clearCacheDeltaFileNamesCouter();
    }

}
