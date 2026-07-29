package org.hestiastore.index.segment;

import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryIteratorWithLock;
import org.hestiastore.index.OptimisticLock;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstorecache.ChunkStoreCache;
import org.hestiastore.index.chunkstorecache.LruChunkStoreCache;
import org.hestiastore.index.sorteddatafile.EmptyEntryIteratorWithCurrent;

/**
 * Encapsulates segment read operations and read-time state.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentReadPath<K, V> {

    private final SegmentFiles<K, V> segmentFiles;
    private final SegmentConf segmentConf;
    private final SegmentResources<K> segmentResources;
    private final SegmentSearcher<K, V> segmentSearcher;
    private final SegmentCache<K, V> segmentCache;
    private final VersionController versionController;
    private final ChunkStoreCache<K, V> chunkStoreCache;
    private final AtomicReference<SegmentIndexSearcher<K, V>> segmentIndexSearcher = new AtomicReference<>();

    /**
     * Creates the read path components for a segment.
     *
     * @param segmentFiles segment file access wrapper
     * @param segmentConf segment configuration
     * @param segmentResources segment resource provider
     * @param segmentSearcher segment search pipeline
     * @param segmentCache in-memory cache
     * @param versionController version tracker for optimistic reads
     * @param chunkStoreCache parsed persisted page cache
     */
    SegmentReadPath(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf,
            final SegmentResources<K> segmentResources,
            final SegmentSearcher<K, V> segmentSearcher,
            final SegmentCache<K, V> segmentCache,
            final VersionController versionController) {
        this(segmentFiles, segmentConf, segmentResources, segmentSearcher,
                segmentCache, versionController, new LruChunkStoreCache<>(0));
    }

    /**
     * Creates the read path components for a segment.
     *
     * @param segmentFiles segment file access wrapper
     * @param segmentConf segment configuration
     * @param segmentResources segment resource provider
     * @param segmentSearcher segment search pipeline
     * @param segmentCache in-memory cache
     * @param versionController version tracker for optimistic reads
     * @param chunkStoreCache parsed persisted page cache
     */
    SegmentReadPath(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf,
            final SegmentResources<K> segmentResources,
            final SegmentSearcher<K, V> segmentSearcher,
            final SegmentCache<K, V> segmentCache,
            final VersionController versionController,
            final ChunkStoreCache<K, V> chunkStoreCache) {
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        this.segmentConf = Vldtn.requireNonNull(segmentConf, "segmentConf");
        this.segmentResources = Vldtn.requireNonNull(segmentResources,
                "segmentResources");
        this.segmentSearcher = Vldtn.requireNonNull(segmentSearcher,
                "segmentSearcher");
        this.segmentCache = Vldtn.requireNonNull(segmentCache, "segmentCache");
        this.versionController = Vldtn.requireNonNull(versionController,
                "versionController");
        this.chunkStoreCache = Vldtn.requireNonNull(chunkStoreCache,
                "chunkStoreCache");
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
                segmentCache.mergedIterator());
        return applyIsolation(mergedEntryIterator, isolation);
    }

    /**
     * Opens a merged iterator over a half-open key range. Persisted reads begin
     * at the scarce-index page selected for the lower bound.
     *
     * @param fromInclusive required inclusive lower key bound
     * @param toExclusive optional exclusive upper key bound
     * @param isolation iterator isolation mode
     * @return bounded merged iterator
     */
    EntryIterator<K, V> openIterator(final K fromInclusive,
            final K toExclusive,
            final SegmentIteratorIsolation isolation) {
        final K lowerBound = Vldtn.requireNonNull(fromInclusive,
                "fromInclusive");
        Vldtn.requireNonNull(isolation, "isolation");
        final EntryIterator<K, V> mergedEntryIterator = new MergeDeltaCacheWithIndexIterator<>(
                openPersistedIterator(lowerBound),
                segmentFiles.getKeyTypeDescriptor(),
                segmentFiles.getValueTypeDescriptor(),
                segmentCache.mergedIterator());
        final EntryIterator<K, V> rangeIterator = new KeyRangeEntryIterator<>(
                mergedEntryIterator,
                segmentFiles.getKeyTypeDescriptor().getComparator(), lowerBound,
                toExclusive);
        return applyIsolation(rangeIterator, isolation);
    }

    private EntryIterator<K, V> openPersistedIterator(
            final K fromInclusive) {
        final Integer position = segmentResources.getScarceIndex()
                .get(fromInclusive);
        if (position == null) {
            return new EmptyEntryIteratorWithCurrent<>();
        }
        return segmentFiles.getIndexFile()
                .openIteratorAtPosition(position.longValue());
    }

    private EntryIterator<K, V> applyIsolation(
            final EntryIterator<K, V> mergedEntryIterator,
            final SegmentIteratorIsolation isolation) {
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
     * Concurrent callers may construct redundant searchers; only the CAS
     * winner is cached and every losing instance is closed immediately.
     *
     * @return cached index searcher
     */
    SegmentIndexSearcher<K, V> getSegmentIndexSearcher() {
        SegmentIndexSearcher<K, V> current = segmentIndexSearcher.get();
        if (current != null) {
            return current;
        }
        final SegmentIndexSearcher<K, V> created = new SegmentIndexSearcher<>(
                cacheOwnerId(), segmentFiles::getActiveVersion,
                segmentFiles.getIndexFile(),
                segmentConf.getMaxNumberOfKeysInChunk(),
                segmentFiles.getKeyTypeDescriptor().getComparator(),
                segmentFiles.getDirectory().getFileReaderSeekableSupplier(
                        segmentFiles.getIndexFileName()),
                chunkStoreCache);
        if (segmentIndexSearcher.compareAndSet(null, created)) {
            return created;
        }
        created.close();
        return segmentIndexSearcher.get();
    }

    /**
     * Closes and clears the cached index searcher and seekable reader.
     */
    void resetSegmentIndexSearcher() {
        chunkStoreCache.invalidateOwner(cacheOwnerId());
        final SegmentIndexSearcher<K, V> current = segmentIndexSearcher
                .getAndSet(null);
        if (current != null) {
            current.close();
        }
    }

    /**
     * Releases any cached read resources.
     */
    void close() {
        resetSegmentIndexSearcher();
    }

    long getBloomFilterRequestCount() {
        return Math.max(0L, segmentResources.getBloomFilterRequestCount());
    }

    long getBloomFilterRefusedCount() {
        return Math.max(0L, segmentResources.getBloomFilterRefusedCount());
    }

    long getBloomFilterPositiveCount() {
        return Math.max(0L, segmentResources.getBloomFilterPositiveCount());
    }

    long getBloomFilterFalsePositiveCount() {
        return Math.max(0L, segmentResources.getBloomFilterFalsePositiveCount());
    }

    private String cacheOwnerId() {
        final String segmentIdName = segmentFiles.getSegmentIdName();
        if (segmentIdName != null && !segmentIdName.isBlank()) {
            return segmentIdName;
        }
        return Vldtn.requireNonNull(segmentFiles.getId(), "segmentId")
                .toString();
    }
}
