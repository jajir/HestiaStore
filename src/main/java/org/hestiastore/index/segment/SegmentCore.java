package org.hestiastore.index.segment;

import java.util.Comparator;
import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.WriteTransaction.WriterFunction;
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
            final VersionController versionController,
            final SegmentPropertiesManager segmentPropertiesManager,
            final SegmentCache<K, V> segmentCache,
            final SegmentReadPath<K, V> readPath,
            final SegmentWritePath<K, V> writePath,
            final SegmentMaintenancePath<K, V> maintenancePath) {
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        logger.debug("Initializing segment '{}'", segmentFiles.getId());
        this.versionController = Vldtn.requireNonNull(versionController,
                "versionController");
        this.segmentPropertiesManager = Vldtn.requireNonNull(
                segmentPropertiesManager, "segmentPropertiesManager");
        this.segmentCache = Vldtn.requireNonNull(segmentCache, "segmentCache");
        this.readPath = Vldtn.requireNonNull(readPath, "readPath");
        this.writePath = Vldtn.requireNonNull(writePath, "writePath");
        this.maintenancePath = Vldtn.requireNonNull(maintenancePath,
                "maintenancePath");
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

    EntryIterator<K, V> openIterator(final SegmentIteratorIsolation isolation) {
        return readPath.openIterator(isolation);
    }

    /**
     * Opens an iterator over the index and a provided snapshot list.
     *
     * @param snapshotEntries sorted snapshot entries
     * @return iterator over the merged snapshot view
     */
    EntryIterator<K, V> openIteratorFromSnapshot(
            final List<Entry<K, V>> snapshotEntries) {
        Vldtn.requireNonNull(snapshotEntries, "snapshotEntries");
        return new MergeDeltaCacheWithIndexIterator<>(
                segmentFiles.getIndexFile().openIterator(),
                segmentFiles.getKeyTypeDescriptor(),
                segmentFiles.getValueTypeDescriptor(), snapshotEntries);
    }

    SegmentFullWriterTx<K, V> openFullWriteTx() {
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

    void flush() {
        final List<Entry<K, V>> entries = freezeWriteCacheForFlush();
        if (entries.isEmpty()) {
            return;
        }
        flushFrozenWriteCacheToDeltaFile(entries);
        applyFrozenWriteCacheAfterFlush();
    }

    int getNumberOfKeysInWriteCache() {
        return writePath.getNumberOfKeysInWriteCache();
    }

    /**
     * Captures a sorted snapshot of the current cache contents.
     *
     * @return sorted cache snapshot
     */
    List<Entry<K, V>> snapshotCacheEntries() {
        return segmentCache.getAsSortedList();
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

    void resetSegmentIndexSearcher() {
        readPath.resetSegmentIndexSearcher();
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
