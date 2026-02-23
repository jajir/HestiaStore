package org.hestiastore.index.segment;

import java.util.Comparator;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkentryfile.ChunkEntryFile;
import org.hestiastore.index.directory.FileReaderSeekable;

/**
 * Performs point lookups in the on-disk index file for a single segment.
 * <p>
 * Given a byte position from the scarce index, it opens an iterator positioned
 * at that block and scans forward (up to the configured page size) comparing
 * keys in order until it finds a match or determines absence.
 * <p>
 * The searcher uses a shared {@link FileReaderSeekable} to reuse an
 * underlying channel across lookups.
 *
 * @param <K> key type
 * @param <V> value type
 */
class SegmentIndexSearcher<K, V> extends AbstractCloseableResource {

    private final ChunkEntryFile<K, V> chunkPairFile;
    private final int maxNumberOfKeysInIndexPage;
    private final Comparator<K> keyTypeComparator;
    private final FileReaderSeekable seekableReader;

    /**
     * Creates a searcher bound to an index file and lookup limits.
     *
     * @param chunkPairFile              chunk-entry file representing the index
     * @param maxNumberOfKeysInIndexPage maximum number of entries to scan from
     *                                   the start position
     * @param keyTypeComparator          comparator used for key ordering in the
     *                                   index
     * @param seekableReader             shared reader reused across lookups
     */
    SegmentIndexSearcher(final ChunkEntryFile<K, V> chunkPairFile,
            final int maxNumberOfKeysInIndexPage,
            final Comparator<K> keyTypeComparator,
            final FileReaderSeekable seekableReader) {
        this.chunkPairFile = Vldtn.requireNonNull(chunkPairFile,
                "segmentIndexFile");
        this.maxNumberOfKeysInIndexPage = Vldtn.requireNonNull(
                maxNumberOfKeysInIndexPage, "maxNumberOfKeysInIndexPage");
        this.keyTypeComparator = Vldtn.requireNonNull(keyTypeComparator,
                "keyTypeComparator");
        this.seekableReader = Vldtn.requireNonNull(seekableReader,
                "seekableReader");
    }

    /**
     * Releases resources held by the searcher.
     */
    @Override
    protected void doClose() {
        seekableReader.close();
    }

    /**
     * Searches for a key starting at the provided index position.
     *
     * @param key           target key
     * @param startPosition byte offset provided by the scarce index
     * @return value when found, otherwise {@code null}
     */
    public V search(final K key, long startPosition) {
        try (EntryIterator<K, V> iterator = chunkPairFile
                .openIteratorAtPosition(startPosition, seekableReader)) {
            for (int i = 0; iterator.hasNext()
                    && i < maxNumberOfKeysInIndexPage; i++) {
                final Entry<K, V> entry = iterator.next();
                final int cmp = keyTypeComparator.compare(entry.getKey(), key);
                if (cmp == 0) {
                    return entry.getValue();
                }
                if (cmp > 0) {
                    return null;
                }
            }
        }
        return null;
    }

}
