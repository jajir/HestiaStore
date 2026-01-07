package org.hestiastore.index.segment;

import java.util.Comparator;
import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.WriteTransaction.WriterFunction;
import org.hestiastore.index.directory.FileReaderSeekable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single-threaded segment core holding the existing implementation logic.
 *
 * @param <K> key type stored in this segment
 * @param <V> value type stored in this segment
 */
final class SegmentCore<K, V> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final SegmentFiles<K, V> segmentFiles;
    private final VersionController versionController;
    private final SegmentPropertiesManager segmentPropertiesManager;
    private final SegmentCache<K, V> segmentCache;
    private final SegmentReadPath<K, V> readPath;
    private final SegmentWritePath<K, V> writePath;
    private final SegmentMaintenancePath<K, V> maintenancePath;

    SegmentCore(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf,
            final VersionController versionController,
            final SegmentPropertiesManager segmentPropertiesManager,
            final SegmentResources<K, V> segmentResources,
            final SegmentDeltaCacheController<K, V> segmentDeltaCacheController,
            final SegmentSearcher<K, V> segmentSearcher) {
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        logger.debug("Initializing segment '{}'", segmentFiles.getId());
        this.versionController = Vldtn.requireNonNull(versionController,
                "versionController");
        final SegmentConf conf = Vldtn.requireNonNull(segmentConf,
                "segmentConf");
        final SegmentResources<K, V> resources = Vldtn.requireNonNull(
                segmentResources, "segmentResources");
        final SegmentDeltaCacheController<K, V> deltaController = Vldtn
                .requireNonNull(segmentDeltaCacheController,
                        "segmentDeltaCacheController");
        final SegmentDeltaCache<K, V> deltaCache = segmentResources
                .getSegmentDeltaCache();
        this.segmentCache = new SegmentCache<>(
                segmentFiles.getKeyTypeDescriptor().getComparator(),
                segmentFiles.getValueTypeDescriptor(),
                deltaCache.getAsSortedList(),
                conf.getMaxNumberOfKeysInSegmentWriteCache(),
                conf.getMaxNumberOfKeysInSegmentWriteCacheDuringFlush(),
                conf.getMaxNumberOfKeysInSegmentCache());
        deltaController.setSegmentCache(segmentCache);
        this.segmentPropertiesManager = Vldtn.requireNonNull(
                segmentPropertiesManager, "segmentPropertiesManager");
        this.readPath = new SegmentReadPath<>(segmentFiles, conf, resources,
                Vldtn.requireNonNull(segmentSearcher, "segmentSearcher"),
                segmentCache, versionController);
        this.writePath = new SegmentWritePath<>(segmentCache,
                versionController);
        this.maintenancePath = new SegmentMaintenancePath<>(segmentFiles, conf,
                segmentPropertiesManager, resources, deltaController,
                segmentCache);
    }

    SegmentStats getStats() {
        return segmentPropertiesManager.getSegmentStats();
    }

    long getNumberOfKeys() {
        return segmentPropertiesManager.getSegmentStats().getNumberOfKeys();
    }

    void invalidateIterators() {
        versionController.changeVersion();
    }

    EntryIterator<K, V> openIterator() {
        return openIterator(SegmentIteratorIsolation.FAIL_FAST);
    }

    EntryIterator<K, V> openIterator(final SegmentIteratorIsolation isolation) {
        return readPath.openIterator(isolation);
    }

    WriteTransaction<K, V> openFullWriteTx() {
        return maintenancePath.openFullWriteTx();
    }

    void executeFullWriteTx(final WriterFunction<K, V> writeFunction) {
        maintenancePath.executeFullWriteTx(writeFunction);
    }

    void put(final K key, final V value) {
        writePath.put(key, value);
    }

    boolean tryPutWithoutWaiting(final K key, final V value) {
        return writePath.tryPutWithoutWaiting(key, value);
    }

    void awaitWriteCapacity() {
        writePath.awaitWriteCapacity();
    }

    void flush() {
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

    int getNumberOfKeysInWriteCache() {
        return writePath.getNumberOfKeysInWriteCache();
    }

    long getNumberOfKeysInCache() {
        return segmentPropertiesManager.getSegmentStats()
                .getNumberOfKeysInSegment()
                + segmentCache.getNumbberOfKeysInCache();
    }

    V get(final K key) {
        return readPath.get(key);
    }

    SegmentId getId() {
        return segmentFiles.getId();
    }

    Comparator<K> getKeyComparator() {
        return segmentFiles.getKeyTypeDescriptor().getComparator();
    }

    SegmentIndexSearcher<K, V> getSegmentIndexSearcher() {
        return readPath.getSegmentIndexSearcher();
    }

    FileReaderSeekable getSeekableReader() {
        return readPath.getSeekableReader();
    }

    void resetSegmentIndexSearcher() {
        readPath.resetSegmentIndexSearcher();
    }

    void resetSeekableReader() {
        readPath.resetSeekableReader();
    }

    List<Entry<K, V>> freezeWriteCacheForFlush() {
        return writePath.freezeWriteCacheForFlush();
    }

    void flushFrozenWriteCacheToDeltaFile(final List<Entry<K, V>> entries) {
        maintenancePath.flushFrozenWriteCacheToDeltaFile(entries);
    }

    void applyFrozenWriteCacheAfterFlush() {
        writePath.applyFrozenWriteCacheAfterFlush();
    }

    void close() {
        readPath.close();
        logger.debug("Closing segment '{}'", segmentFiles.getId());
    }
}
