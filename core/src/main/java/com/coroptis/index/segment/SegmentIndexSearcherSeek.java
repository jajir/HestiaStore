package com.coroptis.index.segment;

import java.util.Comparator;
import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.PairSeekableReader;
import com.coroptis.index.sorteddatafile.SortedDataFile;

/**
 * This implementation keep open file and when it search it seek to given place
 * and read data. It use {@link java.util.RandomAccessFile#seek()}
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentIndexSearcherSeek<K, V>
        implements SegmentIndexSearcher<K, V> {

    private final int maxNumberOfKeysInIndexPage;
    private final Comparator<K> keyTypeComparator;
    private final PairSeekableReader<K, V> pairSeekableReader;

    SegmentIndexSearcherSeek(final SortedDataFile<K, V> segmenIndexFile,
            final int maxNumberOfKeysInIndexPage,
            final Comparator<K> keyTypeComparator) {
        Objects.requireNonNull(segmenIndexFile);
        this.maxNumberOfKeysInIndexPage = Objects
                .requireNonNull(maxNumberOfKeysInIndexPage);
        this.keyTypeComparator = Objects.requireNonNull(keyTypeComparator);
        this.pairSeekableReader = segmenIndexFile.openSeekableReader();
    }

    @Override
    public void close() {
        pairSeekableReader.close();
    }

    @Override
    public V search(final K key, final long startPosition) {
        pairSeekableReader.seek(startPosition);
        for (int i = 0; i < maxNumberOfKeysInIndexPage; i++) {
            final Pair<K, V> pair = pairSeekableReader.read();
            final int cmp = keyTypeComparator.compare(pair.getKey(), key);
            if (cmp == 0) {
                return pair.getValue();
            }
            /**
             * Keys are in ascending order. When searched key is smaller than
             * key read from sorted data than key is not found.
             */
            if (cmp > 0) {
                return null;
            }
        }
        return null;
    }

}
