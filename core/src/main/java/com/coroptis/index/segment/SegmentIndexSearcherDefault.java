package com.coroptis.index.segment;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.sorteddatafile.SortedDataFile;
import com.coroptis.index.CloseablePairReader;

/**
 * Searcher for each search open file for read and skip given number of bytes.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentIndexSearcherDefault<K, V>
        implements SegmentIndexSearcher<K, V> {

    private final SortedDataFile<K, V> segmentIndexFile;
    private final int maxNumberOfKeysInIndexPage;
    private final Comparator<K> keyTypeComparator;

    SegmentIndexSearcherDefault(final SortedDataFile<K, V> segmentIndexFile,
            final int maxNumberOfKeysInIndexPage,
            final Comparator<K> keyTypeComparator) {
        this.segmentIndexFile = Objects.requireNonNull(segmentIndexFile);
        this.maxNumberOfKeysInIndexPage = Objects
                .requireNonNull(maxNumberOfKeysInIndexPage);
        this.keyTypeComparator = Objects.requireNonNull(keyTypeComparator);
    }

    @Override
    public void close() {
        // do intentionally nothing
    }

    @Override
    public V search(final K key, long startPosition) {
        try (CloseablePairReader<K, V> fileReader = segmentIndexFile
                .openReader(startPosition)) {
            for (int i = 0; i < maxNumberOfKeysInIndexPage; i++) {
                final Pair<K, V> pair = fileReader.read();
                final int cmp = keyTypeComparator.compare(pair.getKey(), key);
                if (cmp == 0) {
                    return pair.getValue();
                }
                /**
                 * Keys are in ascending order. When searched key is smaller
                 * than key read from sorted data than key is not found.
                 */
                if (cmp > 0) {
                    return null;
                }
            }
        }
        return null;
    }

}
