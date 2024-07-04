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
    private final SegmentDeltaCacheController<K, V> deltaCacheController;

    public SegmentReader(final SegmentFiles<K, V> segmentFiles,
            final SegmentDeltaCacheController<K, V> deltaCacheController) {
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        this.deltaCacheController = Objects.requireNonNull(deltaCacheController,
                "Segment delta cached controlle is required");
    }

    public PairIterator<K, V> openIterator(
            final OptimisticLockObjectVersionProvider versionProvider) {
        return new PairIteratorWithLock<>(
                new MergeWithCacheIterator<K, V>(
                        segmentFiles.getIndexSstFile().openIterator(),
                        segmentFiles.getKeyTypeDescriptor(),
                        segmentFiles.getValueTypeDescriptor(),
                        deltaCacheController.getDeltaCache().getSortedKeys(),
                        key -> deltaCacheController.getDeltaCache().get(key)),
                new OptimisticLock(versionProvider));
    }

}
