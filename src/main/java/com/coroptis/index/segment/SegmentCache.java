package com.coroptis.index.segment;

import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.cache.UniqueCache;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.sstfile.SstFileStreamer;

/**
 * Represents segment cache containing changes in segment.
 * 
 * In constructor are data loaded from file system.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentCache<K, V> {

    private final UniqueCache<K, V> cache;

    private final SegmentFiles<K, V> segmentFiles;

    private final SegmentPropertiesManager segmentPropertiesManager;

    public SegmentCache(final TypeDescriptor<K> keyTypeDescriptor,
            final SegmentFiles<K, V> segmentFiles,
            final SegmentPropertiesManager segmentPropertiesManager) {
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        this.segmentPropertiesManager = Objects
                .requireNonNull(segmentPropertiesManager);
        this.cache = UniqueCache.<K, V>builder()
                .withKeyComparator(keyTypeDescriptor.getComparator())
                .withSstFile(segmentFiles.getCacheSstFile()).build();
        segmentPropertiesManager.getCacheDeltaFileNames()
                .forEach(segmentDeltaFileName -> {
                    try (SstFileStreamer<K, V> fileStreamer = segmentFiles
                            .getCacheSstFile(segmentDeltaFileName)
                            .openStreamer()) {
                        fileStreamer.stream().forEach(pair -> cache.put(pair));
                    }
                });
    }

    public void put(final Pair<K, V> pair) {
        Objects.requireNonNull(pair);
        cache.put(pair);
    }

    public int size() {
        return cache.size();
    }

    public void clear() {
        cache.clear();
        segmentPropertiesManager.getCacheDeltaFileNames()
                .forEach(segmentCacheDeltaFile -> {
                    segmentFiles.deleteFile(segmentCacheDeltaFile);
                });
        segmentFiles.optionallyDeleteFile(segmentFiles.getCacheFileName());
        segmentPropertiesManager.clearCacheDeltaFileNamesCouter();
    }

    public V get(final K key) {
        return cache.get(key);
    }

    public PairIterator<K, V> getSortedIterator() {
        return cache.getSortedIterator();
    }

}
