package com.coroptis.index.segment;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.PairReader;
import com.coroptis.index.sstfile.SstFile;

public class SegmentIndexSearcherSeek<K, V>
        implements SegmentIndexSearcher<K, V> {

    private final SstFile<K, V> segmenIndexFile;
    private final int maxNumberOfKeysInIndexPage;
    private final Comparator<K> keyTypeComparator;

    SegmentIndexSearcherSeek(final SstFile<K, V> segmenIndexFile,
            final int maxNumberOfKeysInIndexPage,
            final Comparator<K> keyTypeComparator) {
        this.segmenIndexFile = Objects.requireNonNull(segmenIndexFile);
        this.maxNumberOfKeysInIndexPage = Objects
                .requireNonNull(maxNumberOfKeysInIndexPage);
        this.keyTypeComparator = Objects.requireNonNull(keyTypeComparator);
    }

    @Override
    public void close() {
    }

    @Override
    public V search(K key, long startPosition) {
        try (PairReader<K, V> fileReader = segmenIndexFile
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
