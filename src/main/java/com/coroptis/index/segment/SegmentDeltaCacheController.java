package com.coroptis.index.segment;

import java.util.Objects;

import com.coroptis.index.datatype.TypeDescriptor;

/**
 * Provide ultimate access to delta cache and relaret operations
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentDeltaCacheController<K, V> {

    private final SegmentFiles<K, V> segmentFiles;
    private final SegmentPropertiesManager segmentPropertiesManager;
    private final SegmentCacheDataProvider<K, V> segmentCacheDataProvider;

    public SegmentDeltaCacheController(
            final TypeDescriptor<K> keyTypeDescriptor,
            final SegmentFiles<K, V> segmentFiles,
            final SegmentPropertiesManager segmentPropertiesManager,
            final SegmentCacheDataProvider<K, V> segmentCacheDataProvider) {
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        this.segmentPropertiesManager = Objects
                .requireNonNull(segmentPropertiesManager);
        this.segmentCacheDataProvider = Objects.requireNonNull(
                segmentCacheDataProvider,
                "Segment cached data provider is required");
    }

    public SegmentDeltaCache<K, V> getDeltaCache() {
        return segmentCacheDataProvider.getSegmentDeltaCache();
    }

    public void clear() {
        if(segmentCacheDataProvider.isLoaded()) {
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
