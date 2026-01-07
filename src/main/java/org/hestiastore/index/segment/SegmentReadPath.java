package org.hestiastore.index.segment;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryIteratorWithLock;
import org.hestiastore.index.OptimisticLock;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileReaderSeekable;
import org.hestiastore.index.directory.async.AsyncFileReaderSeekableBlockingAdapter;

/**
 * Encapsulates segment read operations and read-time state.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentReadPath<K, V> {

    private final SegmentFiles<K, V> segmentFiles;
    private final SegmentConf segmentConf;
    private final SegmentResources<K, V> segmentResources;
    private final SegmentSearcher<K, V> segmentSearcher;
    private final SegmentCache<K, V> segmentCache;
    private final VersionController versionController;
    private SegmentIndexSearcher<K, V> segmentIndexSearcher;
    private FileReaderSeekable seekableReader;

    SegmentReadPath(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf,
            final SegmentResources<K, V> segmentResources,
            final SegmentSearcher<K, V> segmentSearcher,
            final SegmentCache<K, V> segmentCache,
            final VersionController versionController) {
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        this.segmentConf = Vldtn.requireNonNull(segmentConf, "segmentConf");
        this.segmentResources = Vldtn.requireNonNull(segmentResources,
                "segmentResources");
        this.segmentSearcher = Vldtn.requireNonNull(segmentSearcher,
                "segmentSearcher");
        this.segmentCache = Vldtn.requireNonNull(segmentCache, "segmentCache");
        this.versionController = Vldtn.requireNonNull(versionController,
                "versionController");
    }

    /**
     * Opens an iterator over the merged index + delta cache view.
     *
     * @param isolation iterator isolation mode
     * @return iterator over current entries
     */
    EntryIterator<K, V> openIterator(final SegmentIteratorIsolation isolation) {
        Vldtn.requireNonNull(isolation, "isolation");
        final EntryIterator<K, V> mergedEntryIterator = new MergeDeltaCacheWithIndexIterator<>(
                segmentFiles.getIndexFile().openIterator(),
                segmentFiles.getKeyTypeDescriptor(),
                segmentFiles.getValueTypeDescriptor(),
                segmentCache.getAsSortedList());
        if (isolation == SegmentIteratorIsolation.FULL_ISOLATION) {
            return mergedEntryIterator;
        }
        return new EntryIteratorWithLock<>(mergedEntryIterator,
                new OptimisticLock(versionController),
                segmentFiles.getId().toString());
    }

    /**
     * Retrieves a value using cache first and then falling back to disk search.
     *
     * @param key key to look up
     * @return found value or null if not present or tombstoned
     */
    V get(final K key) {
        final V cached = segmentCache.get(key);
        if (cached != null) {
            if (segmentFiles.getValueTypeDescriptor().isTombstone(cached)) {
                return null;
            }
            return cached;
        }
        return segmentSearcher.get(key, segmentResources,
                getSegmentIndexSearcher());
    }

    /**
     * Returns (and caches) the index searcher for point lookups.
     *
     * @return cached index searcher
     */
    SegmentIndexSearcher<K, V> getSegmentIndexSearcher() {
        if (segmentIndexSearcher == null) {
            segmentIndexSearcher = new SegmentIndexSearcher<>(
                    segmentFiles.getIndexFile(),
                    segmentConf.getMaxNumberOfKeysInChunk(),
                    segmentFiles.getKeyTypeDescriptor().getComparator(),
                    getSeekableReader());
        }
        return segmentIndexSearcher;
    }

    /**
     * Returns a cached seekable reader for the index file when available.
     *
     * @return cached reader or null when no index file exists
     */
    FileReaderSeekable getSeekableReader() {
        if (seekableReader == null) {
            final String indexFileName = segmentFiles.getIndexFileName();
            if (segmentFiles.getAsyncDirectory()
                    .isFileExistsAsync(indexFileName).toCompletableFuture()
                    .join()) {
                seekableReader = new AsyncFileReaderSeekableBlockingAdapter(
                        segmentFiles.getAsyncDirectory()
                                .getFileReaderSeekableAsync(indexFileName)
                                .toCompletableFuture().join());
            }
        }
        return seekableReader;
    }

    /**
     * Closes and clears the cached index searcher and seekable reader.
     */
    void resetSegmentIndexSearcher() {
        if (segmentIndexSearcher != null) {
            segmentIndexSearcher.close();
            segmentIndexSearcher = null;
        }
        resetSeekableReader();
    }

    /**
     * Closes and clears the cached seekable reader.
     */
    void resetSeekableReader() {
        if (seekableReader != null) {
            seekableReader.close();
            seekableReader = null;
        }
    }

    /**
     * Releases any cached read resources.
     */
    void close() {
        resetSegmentIndexSearcher();
    }
}
