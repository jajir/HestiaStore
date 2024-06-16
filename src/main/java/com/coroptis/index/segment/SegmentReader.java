package com.coroptis.index.segment;

import java.util.Objects;

import com.coroptis.index.OptimisticLockObjectVersionProvider;
import com.coroptis.index.PairIterator;

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
    private final SegmentPropertiesManager segmentPropertiesManager;

    public SegmentReader(final SegmentFiles<K, V> segmentFiles,
            final SegmentPropertiesManager segmentPropertiesManager) {
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        this.segmentPropertiesManager = Objects
                .requireNonNull(segmentPropertiesManager);
    }

    public PairIterator<K, V> openIterator(
            final OptimisticLockObjectVersionProvider versionProvider) {
        return openIterator(versionProvider, null);
    }

    public PairIterator<K, V> openIterator(
            final OptimisticLockObjectVersionProvider versionProvider,
            final SegmentSearcher<K, V> segmentSearcher) {
        // Read segment cache into in memory list.
        final SegmentDeltaCache<K, V> segmentDeltaCache = getSegmentCache(
                segmentSearcher);

        // merge cache with main data
        return new MergeIterator<K, V>(
                segmentFiles.getIndexSstFile().openIterator(versionProvider),
                segmentDeltaCache.getSortedIterator(),
                segmentFiles.getKeyTypeDescriptor(),
                segmentFiles.getValueTypeDescriptor());
    }

    private SegmentDeltaCache<K, V> getSegmentCache(
            final SegmentSearcher<K, V> segmentSearcher) {
        if (segmentSearcher == null) {
            return new SegmentDeltaCache<>(segmentFiles.getKeyTypeDescriptor(),
                    segmentFiles, segmentPropertiesManager);

        }
        return segmentSearcher.getSegmentCache()
                .orElse(new SegmentDeltaCache<>(segmentFiles.getKeyTypeDescriptor(),
                        segmentFiles, segmentPropertiesManager));
    }

}
