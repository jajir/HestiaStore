package org.hestiastore.index.chunkpairfile;

import java.util.Comparator;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentIndexSearcher;

public class ChunkPairFileSegmentIndexSearcher<K, V>
        implements SegmentIndexSearcher<K, V> {

    private final ChunkStoreSearcher<K, V> segmentIndexFile;
    private final int maxNumberOfKeysInIndexChunk;
    private final Comparator<K> keyTypeComparator;

    ChunkPairFileSegmentIndexSearcher(
            final ChunkStoreSearcher<K, V> chunkStoreSearcher,
            final int maxNumberOfKeysInIndexChunk,
            final Comparator<K> keyTypeComparator) {
        this.segmentIndexFile = Vldtn.requireNonNull(chunkStoreSearcher,
                "segmentIndexFile");
        this.maxNumberOfKeysInIndexChunk = maxNumberOfKeysInIndexChunk;
        this.keyTypeComparator = Vldtn.requireNonNull(keyTypeComparator,
                "keyTypeComparator");
    }

    @Override
    public void close() {
    }

    @Override
    public V search(final K key, final long startPosition) {
        try (PairIterator<K, V> fileReader = segmentIndexFile
                .openIteratorAtPosition(startPosition)) {
            for (int i = 0; i < maxNumberOfKeysInIndexChunk; i++) {
                final Pair<K, V> pair = fileReader.next();
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
