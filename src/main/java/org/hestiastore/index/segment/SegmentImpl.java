package org.hestiastore.index.segment;

import java.util.List;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryIteratorWithLock;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.OptimisticLock;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.WriteTransaction.WriterFunction;
import org.hestiastore.index.directory.FileReaderSeekable;
import org.hestiastore.index.directory.async.AsyncFileReaderSeekableBlockingAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A single on-disk index segment with delta-cache and compaction support.
 * <p>
 * Segment coordinates read and write operations for a bounded subset of the
 * index data. It encapsulates the underlying files, provides search and
 * iteration, accepts writes through a delta cache (with o̦ptional automatic
 * compaction), and exposes utilities for statistics and consistency checking.
 * Versioning is tracked via an optimistic lock to guard concurrent readers
 * while updates occur.
 *
 * @author honza
 *
 * @param <K> key type stored in this segment
 * @param <V> value type stored in this segment
 */
public class SegmentImpl<K, V> extends AbstractCloseableResource
        implements Segment<K, V> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final SegmentConf segmentConf;
    private final SegmentFiles<K, V> segmentFiles;
    private final VersionController versionController;
    private final SegmentPropertiesManager segmentPropertiesManager;
    private final SegmentCompacter<K, V> segmentCompacter;
    private final SegmentDeltaCacheController<K, V> deltaCacheController;
    private final SegmentSearcher<K, V> segmentSearcher;
    private final SegmentResources<K, V> segmentResources;
    private final SegmentCache<K, V> segmentCache;
    private SegmentIndexSearcher<K, V> segmentIndexSearcher;
    private FileReaderSeekable seekableReader;

    // Reduced constructors: keep only the most complex constructor below.

    /**
     * Full DI constructor allowing to inject both compacter and replacer.
     * Useful for testing and advanced wiring.
     */
    public SegmentImpl(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf,
            final VersionController versionController,
            final SegmentPropertiesManager segmentPropertiesManager,
            final SegmentResources<K, V> segmentResources,
            final SegmentDeltaCacheController<K, V> segmentDeltaCacheController,
            final SegmentSearcher<K, V> segmentSearcher,
            final SegmentCompacter<K, V> segmentCompacter) {
        this.segmentConf = Vldtn.requireNonNull(segmentConf, "segmentConf");
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        logger.debug("Initializing segment '{}'", segmentFiles.getId());
        this.versionController = Vldtn.requireNonNull(versionController,
                "versionController");
        this.segmentResources = Vldtn.requireNonNull(segmentResources,
                "segmentResources");
        this.deltaCacheController = Vldtn.requireNonNull(
                segmentDeltaCacheController, "segmentDeltaCacheController");
        final SegmentDeltaCache<K, V> deltaCache = segmentResources
                .getSegmentDeltaCache();
        this.segmentCache = new SegmentCache<>(
                segmentFiles.getKeyTypeDescriptor().getComparator(),
                segmentFiles.getValueTypeDescriptor(),
                deltaCache.getAsSortedList(),
                segmentConf.getMaxNumberOfKeysInSegmentWriteCache(),
                segmentConf.getMaxNumberOfKeysInSegmentWriteCacheDuringFlush(),
                segmentConf.getMaxNumberOfKeysInSegmentCache());
        this.deltaCacheController.setSegmentCache(segmentCache);
        this.segmentPropertiesManager = Vldtn.requireNonNull(
                segmentPropertiesManager, "segmentPropertiesManager");
        this.segmentCompacter = Vldtn.requireNonNull(segmentCompacter,
                "segmentCompacter");
        this.segmentSearcher = Vldtn.requireNonNull(segmentSearcher,
                "segmentSearcher");
    }

    @Override
    public SegmentStats getStats() {
        return segmentPropertiesManager.getSegmentStats();
    }

    @Override
    public long getNumberOfKeys() {
        return segmentPropertiesManager.getSegmentStats().getNumberOfKeys();
    }

    @Override
    public K checkAndRepairConsistency() {
        final SegmentConsistencyChecker<K, V> consistencyChecker = new SegmentConsistencyChecker<>(
                this, segmentFiles.getKeyTypeDescriptor().getComparator());
        return consistencyChecker.checkAndRepairConsistency();
    }

    @Override
    public void invalidateIterators() {
        versionController.changeVersion();
    }

    @Override
    public EntryIterator<K, V> openIterator() {
        return openIterator(SegmentIteratorIsolation.FAIL_FAST);
    }

    @Override
    public EntryIterator<K, V> openIterator(
            final SegmentIteratorIsolation isolation) {
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
                new OptimisticLock(versionController), getId().toString());
    }

    @Override
    public void compact() {
        segmentCompacter.forceCompact(this);
    }

    /**
     * Method should be called just from inside of this package. Method open
     * direct writer to scarce index and main sst file.
     * 
     * Writer should be opend and closed as one atomic operation.
     * 
     * @return return segment writer object
     */
    void executeFullWriteTx(final WriterFunction<K, V> writeFunction) {
        openFullWriteTx().execute(writeFunction);
    }

    WriteTransaction<K, V> openFullWriteTx() {
        return new SegmentFullWriterTx<>(segmentFiles, segmentPropertiesManager,
                segmentConf.getMaxNumberOfKeysInChunk(), segmentResources,
                deltaCacheController, segmentCache);
    }

    /**
     * Allows to open writer that will write to delta cache. When number of keys
     * in segment exceeds certain threshold, delta cache will be flushed to
     * disk.
     * 
     * It's not necesarry to run it in transaction because it's always new file.
     */
    private EntryWriter<K, V> openDeltaCacheWriter() {
        return new SegmentDeltaCacheCompactingWriter<>(deltaCacheController);
    }

    @Override
    public void put(final K key, final V value) {
        Vldtn.requireNonNull(key, "key");
        Vldtn.requireNonNull(value, "value");
        // TODO make sure that'scorrect behavior to kill all iterators on put
        versionController.changeVersion();
        segmentCache.putToWriteCache(Entry.of(key, value));
    }

    boolean tryPutWithoutWaiting(final K key, final V value) {
        Vldtn.requireNonNull(key, "key");
        Vldtn.requireNonNull(value, "value");
        if (segmentCache.tryPutToWriteCacheWithoutWaiting(
                Entry.of(key, value))) {
            versionController.changeVersion();
            return true;
        }
        return false;
    }

    void awaitWriteCapacity() {
        segmentCache.awaitWriteCapacity();
    }

    @Override
    public void flush() {
        final List<Entry<K, V>> entries = freezeWriteCacheForFlush();
        if (entries.isEmpty()) {
            return;
        }
        try {
            flushFrozenWriteCacheToDeltaFile(entries);
            applyFrozenWriteCacheAfterFlush();
        } catch (final RuntimeException e) {
            // Keep frozen cache for retry; caller sees the failure.
            throw e;
        }
    }

    @Override
    public int getNumberOfKeysInWriteCache() {
        return segmentCache.getNumberOfKeysInWriteCache();
    }

    @Override
    public long getNumberOfKeysInCache() {
        return segmentPropertiesManager.getSegmentStats()
                .getNumberOfKeysInSegment()
                + segmentCache.getNumbberOfKeysInCache();
    }

    @Override
    public V get(final K key) {
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

    @Override
    protected void doClose() {
        resetSegmentIndexSearcher();
        logger.debug("Closing segment '{}'", segmentFiles.getId());
    }

    @Override
    public SegmentId getId() {
        return segmentFiles.getId();
    }

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

    void resetSegmentIndexSearcher() {
        if (segmentIndexSearcher != null) {
            segmentIndexSearcher.close();
            segmentIndexSearcher = null;
        }
        resetSeekableReader();
    }

    void resetSeekableReader() {
        if (seekableReader != null) {
            seekableReader.close();
            seekableReader = null;
        }
    }

    List<Entry<K, V>> freezeWriteCacheForFlush() {
        final boolean hadFrozen = segmentCache.hasFrozenWriteCache();
        final List<Entry<K, V>> entries = segmentCache.freezeWriteCache();
        if (!entries.isEmpty() && !hadFrozen) {
            versionController.changeVersion();
        }
        return entries;
    }

    void flushFrozenWriteCacheToDeltaFile(final List<Entry<K, V>> entries) {
        if (entries == null || entries.isEmpty()) {
            return;
        }
        try (EntryWriter<K, V> writer = openDeltaCacheWriter()) {
            entries.forEach(writer::write);
        }
    }

    void applyFrozenWriteCacheAfterFlush() {
        segmentCache.mergeFrozenWriteCacheToDeltaCache();
    }

}
