package org.hestiastore.index.segment;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryIteratorWithLock;
import org.hestiastore.index.OptimisticLock;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.FileReaderSeekable;
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
     */
    SegmentReadPath(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf,
            final SegmentResources<K> segmentResources,
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
                segmentCache.mergedIterator());
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
        SegmentIndexSearcher<K, V> current = segmentIndexSearcher.get();
        if (current != null) {
            return current;
        }
        final SegmentIndexSearcher<K, V> created = new SegmentIndexSearcher<>(
                segmentFiles.getIndexFile(),
                segmentConf.getMaxNumberOfKeysInChunk(),
                segmentFiles.getKeyTypeDescriptor().getComparator(),
                new LazySeekableReader(segmentFiles.getDirectory(),
                        segmentFiles.getIndexFileName()));
        if (segmentIndexSearcher.compareAndSet(null, created)) {
            return created;
        }
        return segmentIndexSearcher.get();
    }

    /**
     * Lazily opens a seekable file reader and tolerates missing files by
     * behaving as an empty reader.
     */
    private static final class LazySeekableReader extends AbstractCloseableResource
            implements FileReaderSeekable {

        private final Directory directory;
        private final String fileName;
        private final ThreadLocal<FileReaderSeekable> threadReader = new ThreadLocal<>();
        private final Set<FileReaderSeekable> openedReaders = ConcurrentHashMap
                .newKeySet();

        LazySeekableReader(final Directory directory, final String fileName) {
            this.directory = Vldtn.requireNonNull(directory, "directory");
            this.fileName = Vldtn.requireNonNull(fileName, "fileName");
        }

        private FileReaderSeekable openForCurrentThread() {
            final FileReaderSeekable previous = threadReader.get();
            if (previous != null) {
                threadReader.remove();
                if (openedReaders.remove(previous)) {
                    previous.close();
                }
            }
            if (!directory.isFileExists(fileName)) {
                return null;
            }
            final FileReaderSeekable current;
            try {
                current = directory.getFileReaderSeekable(fileName);
            } catch (final IllegalArgumentException e) {
                return null;
            }
            threadReader.set(current);
            openedReaders.add(current);
            return current;
        }

        @Override
        public int read() {
            final FileReaderSeekable current = threadReader.get();
            if (current == null) {
                return -1;
            }
            return current.read();
        }

        @Override
        public int read(final byte[] bytes) {
            final FileReaderSeekable current = threadReader.get();
            if (current == null) {
                return -1;
            }
            return current.read(bytes);
        }

        @Override
        public void skip(final long position) {
            final FileReaderSeekable current = threadReader.get();
            if (current != null) {
                current.skip(position);
            }
        }

        @Override
        public void seek(final long position) {
            final FileReaderSeekable current = openForCurrentThread();
            if (current != null) {
                current.seek(position);
            }
        }

        @Override
        protected void doClose() {
            threadReader.remove();
            for (final FileReaderSeekable reader : openedReaders) {
                reader.close();
            }
            openedReaders.clear();
        }
    }

    /**
     * Closes and clears the cached index searcher and seekable reader.
     */
    void resetSegmentIndexSearcher() {
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
}
