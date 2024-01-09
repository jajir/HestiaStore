package com.coroptis.index.segment;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.coroptis.index.OptimisticLockObjectVersionProvider;
import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.PairIteratorList;

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

    public SegmentReader(final SegmentFiles<K, V> segmentFiles) {
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
    }

    public PairIterator<K, V> openIterator(
            final OptimisticLockObjectVersionProvider versionProvider) {
        // Read segment cache into in memory list.
        final List<Pair<K, V>> pairs = new ArrayList<>();
        try (final PairIterator<K, V> iterator = segmentFiles.getCacheSstFile()
                .openIterator()) {
            while (iterator.hasNext()) {
                pairs.add(iterator.next());
            }
        }

        // merge cache with main data
        return new MergeIterator<K, V>(
                segmentFiles.getIndexSstFile().openIterator(versionProvider),
                new PairIteratorList<>(pairs),
                segmentFiles.getKeyTypeDescriptor(),
                segmentFiles.getValueTypeDescriptor());
    }

}
