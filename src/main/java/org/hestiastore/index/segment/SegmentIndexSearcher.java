package org.hestiastore.index.segment;

import java.util.Comparator;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkentryfile.ChunkEntryFile;

/**
 * Searcher for each search open file for read and skip given number of bytes.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentIndexSearcher<K, V> extends AbstractCloseableResource {

    private final ChunkEntryFile<K, V> chunkPairFile;
    private final int maxNumberOfKeysInIndexPage;
    private final Comparator<K> keyTypeComparator;

    SegmentIndexSearcher(final ChunkEntryFile<K, V> chunkPairFile,
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
    protected void doClose() {
        // intentionally no-op
    }

    public V search(final K key, long startPosition) {
        try (EntryIterator<K, V> iterator = chunkPairFile
                .openIteratorAtPosition(startPosition)) {
            for (int i = 0; iterator.hasNext()
                    && i < maxNumberOfKeysInIndexPage; i++) {
                final Entry<K, V> entry = iterator.next();
                final int cmp = keyTypeComparator.compare(entry.getKey(), key);
                if (cmp == 0) {
                    return entry.getValue();
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
