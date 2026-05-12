package org.hestiastore.index.segment;

import java.util.Comparator;
import java.util.function.LongSupplier;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkentryfile.ChunkEntryFile;
import org.hestiastore.index.chunkstorecache.ChunkStoreCache;
import org.hestiastore.index.chunkstorecache.LruChunkStoreCache;
import org.hestiastore.index.chunkstorecache.ParsedChunkPage;
import org.hestiastore.index.directory.FileReaderSeekable;
import org.hestiastore.index.directory.FileReaderSeekableSupplier;

/**
 * Performs point lookups in the on-disk index file for a single segment.
 * <p>
 * Given a byte position from the scarce index, it opens an iterator positioned
 * at that block and scans forward (up to the configured page size) comparing
 * keys in order until it finds a match or determines absence.
 * <p>
 * The searcher obtains a dedicated seekable cursor for each lookup from a
 * factory. This avoids shared-position races between concurrent requests while
 * still allowing factory-level resource reuse.
 *
 * @param <K> key type
 * @param <V> value type
 */
class SegmentIndexSearcher<K, V> extends AbstractCloseableResource {

    private final String ownerId;
    private final LongSupplier activeVersionSupplier;
    private final ChunkEntryFile<K, V> chunkPairFile;
    private final int maxNumberOfKeysInIndexPage;
    private final Comparator<K> keyTypeComparator;
    private final FileReaderSeekableSupplier seekableReaderSupplier;
    private final ChunkStoreCache<K, V> chunkStoreCache;

    /**
     * Creates a searcher bound to an index file and lookup limits.
     *
     * @param chunkPairFile              chunk-entry file representing the index
     * @param maxNumberOfKeysInIndexPage maximum number of entries to scan from
     *                                   the start position
     * @param keyTypeComparator          comparator used for key ordering in the
     *                                   index
     * @param seekableReaderSupplier     supplier creating seekable cursors for
     *                                   each lookup
     */
    SegmentIndexSearcher(final ChunkEntryFile<K, V> chunkPairFile,
            final int maxNumberOfKeysInIndexPage,
            final Comparator<K> keyTypeComparator,
            final FileReaderSeekableSupplier seekableReaderSupplier) {
        this("segment", () -> 1L, chunkPairFile, maxNumberOfKeysInIndexPage,
                keyTypeComparator, seekableReaderSupplier,
                new LruChunkStoreCache<>(0));
    }

    /**
     * Creates a searcher bound to an index file, lookup limits, and parsed page
     * cache.
     *
     * @param ownerId cache owner id
     * @param activeVersionSupplier active segment version supplier
     * @param chunkPairFile chunk-entry file representing the index
     * @param maxNumberOfKeysInIndexPage maximum number of entries to scan from
     *                                   the start position
     * @param keyTypeComparator comparator used for key ordering in the index
     * @param seekableReaderSupplier supplier creating seekable cursors for each
     *                               lookup
     * @param chunkStoreCache parsed persisted page cache
     */
    SegmentIndexSearcher(final String ownerId,
            final LongSupplier activeVersionSupplier,
            final ChunkEntryFile<K, V> chunkPairFile,
            final int maxNumberOfKeysInIndexPage,
            final Comparator<K> keyTypeComparator,
            final FileReaderSeekableSupplier seekableReaderSupplier,
            final ChunkStoreCache<K, V> chunkStoreCache) {
        this.ownerId = Vldtn.requireNotBlank(ownerId, "ownerId");
        this.activeVersionSupplier = Vldtn.requireNonNull(
                activeVersionSupplier, "activeVersionSupplier");
        this.chunkPairFile = Vldtn.requireNonNull(chunkPairFile,
                "segmentIndexFile");
        this.maxNumberOfKeysInIndexPage = Vldtn.requireNonNull(
                maxNumberOfKeysInIndexPage, "maxNumberOfKeysInIndexPage");
        this.keyTypeComparator = Vldtn.requireNonNull(keyTypeComparator,
                "keyTypeComparator");
        this.seekableReaderSupplier = Vldtn.requireNonNull(
                seekableReaderSupplier, "seekableReaderSupplier");
        this.chunkStoreCache = Vldtn.requireNonNull(chunkStoreCache,
                "chunkStoreCache");
    }

    /**
     * Releases resources held by the searcher.
     */
    @Override
    protected void doClose() {
        seekableReaderSupplier.close();
    }

    /**
     * Searches for a key starting at the provided index position.
     *
     * @param key           target key
     * @param startPosition byte offset provided by the scarce index
     * @return value when found, otherwise {@code null}
     */
    public V search(final K key, long startPosition) {
        if (chunkStoreCache.isEnabled()) {
            return chunkStoreCache.find(ownerId,
                    activeVersionSupplier.getAsLong(), startPosition, key,
                    keyTypeComparator,
                    () -> loadParsedPage(startPosition));
        }
        try (FileReaderSeekable seekableReader = seekableReaderSupplier
                .get()) {
            return chunkPairFile.searchAtPosition(key, startPosition,
                    maxNumberOfKeysInIndexPage, keyTypeComparator,
                    seekableReader);
        }
    }

    private ParsedChunkPage<K, V> loadParsedPage(final long startPosition) {
        try (FileReaderSeekable seekableReader = seekableReaderSupplier
                .get()) {
            return chunkPairFile.loadParsedPageAtPosition(startPosition,
                    maxNumberOfKeysInIndexPage, seekableReader);
        }
    }

}
