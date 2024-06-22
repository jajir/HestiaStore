package com.coroptis.index.segment;

import java.util.Objects;

import com.coroptis.index.OptimisticLock;
import com.coroptis.index.OptimisticLockObjectVersionProvider;
import com.coroptis.index.PairIterator;
import com.coroptis.index.PairIteratorWithLock;

/**
 * Perform full segment data read from data files. Class ignores possibility,
 * that index could be loaded into memory. It went through data from files.
 * 
 * 
 * Class should be combined with in memory fully loaded segments.
 *
 * 
 * It reads all data from cache than combine them with data from index file.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 *
 */
public class SegmentReader<K, V> {

    private final SegmentFiles<K, V> segmentFiles;
    private final SegmentCacheDataProvider<K, V> segmentCacheDataProvider;

    public SegmentReader(final SegmentFiles<K, V> segmentFiles,
            final SegmentCacheDataProvider<K, V> segmentCacheDataProvider) {
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        this.segmentCacheDataProvider = Objects.requireNonNull(
                segmentCacheDataProvider,
                "Segment cached data provider is required");
    }

    public PairIterator<K, V> openIterator(
            final OptimisticLockObjectVersionProvider versionProvider) {
        // Read segment cache into in memory list.
        final SegmentDeltaCache<K, V> segmentDeltaCache = segmentCacheDataProvider
                .getSegmentDeltaCache();

        // merge cache with main data
        return new PairIteratorWithLock<>(
                new MergeIterator<K, V>(
                        segmentFiles.getIndexSstFile().openIterator(),
                        segmentDeltaCache.getSortedIterator(),
                        segmentFiles.getKeyTypeDescriptor(),
                        segmentFiles.getValueTypeDescriptor()),
                new OptimisticLock(versionProvider));
    }

}
