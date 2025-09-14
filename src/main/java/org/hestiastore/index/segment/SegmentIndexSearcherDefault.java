package org.hestiastore.index.segment;

import java.util.Comparator;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkpairfile.ChunkPairFile;

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

    private final ChunkPairFile<K, V> chunkPairFile;
    private final int maxNumberOfKeysInIndexPage;
    private final Comparator<K> keyTypeComparator;

    SegmentIndexSearcherDefault(final ChunkPairFile<K, V> chunkPairFile,
            final int maxNumberOfKeysInIndexPage,
            final Comparator<K> keyTypeComparator) {
        this.chunkPairFile = Vldtn.requireNonNull(chunkPairFile,
                "segmentIndexFile");
        this.maxNumberOfKeysInIndexPage = Vldtn.requireNonNull(
                maxNumberOfKeysInIndexPage, "maxNumberOfKeysInIndexPage");
        this.keyTypeComparator = Vldtn.requireNonNull(keyTypeComparator,
                "keyTypeComparator");
    }

    @Override
    public void close() {
        // do intentionally nothing
    }

    @Override
    public V search(final K key, long startPosition) {
        try (PairIterator<K, V> iterator = chunkPairFile
                .search(startPosition)) {
            for (int i = 0; iterator.hasNext()
                    && i < maxNumberOfKeysInIndexPage; i++) {
                final Pair<K, V> pair = iterator.next();
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
