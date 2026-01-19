package org.hestiastore.index.segment;

import java.util.Comparator;
import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.WriteTransaction.WriterFunction;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.properties.PropertyStore;
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

    /**
     * Creates the segment core with prewired components.
     *
     * @param segmentFiles segment file access wrapper
     * @param versionController version tracker for optimistic reads
     * @param segmentPropertiesManager properties manager for stats
     * @param segmentCache in-memory cache
     * @param readPath read path logic
     * @param writePath write path logic
     * @param maintenancePath maintenance path logic
     */
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

    /**
     * Returns current segment statistics.
     *
     * @return segment statistics snapshot
     */
    SegmentStats getStats() {
        return segmentPropertiesManager.getSegmentStats();
    }

    /**
     * Returns the total number of keys tracked by the segment.
     *
     * @return total key count
     */
    long getNumberOfKeys() {
        return segmentPropertiesManager.getSegmentStats().getNumberOfKeys();
    }

    /**
     * Invalidates optimistic iterators by bumping the version.
     */
    void invalidateIterators() {
        versionController.changeVersion();
    }

    /**
     * Opens a read iterator with the requested isolation level.
     *
     * @param isolation iterator isolation mode
     * @return entry iterator
     */
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

    /**
     * Opens a full write transaction for segment rebuild.
     *
     * @return full writer transaction
     */
    SegmentFullWriterTx<K, V> openFullWriteTx() {
        return maintenancePath.openFullWriteTx();
    }

    /**
     * Executes a full write transaction with the given writer function.
     *
     * @param writeFunction writer callback
     */
    void executeFullWriteTx(final WriterFunction<K, V> writeFunction) {
        maintenancePath.executeFullWriteTx(writeFunction);
    }

    /**
     * Writes a key/value pair into the write cache.
     *
     * @param key key to write
     * @param value value to write
     */
    void put(final K key, final V value) {
        writePath.put(key, value);
    }

    /**
     * Attempts to write without waiting for cache capacity.
     *
     * @param key key to write
     * @param value value to write
     * @return true when the write is accepted
     */
    boolean tryPutWithoutWaiting(final K key, final V value) {
        return writePath.tryPutWithoutWaiting(key, value);
    }

    /**
     * Flushes the write cache into the delta cache if there are entries.
     */
    void flush() {
        final List<Entry<K, V>> entries = freezeWriteCacheForFlush();
        if (entries.isEmpty()) {
            return;
        }
        flushFrozenWriteCacheToDeltaFile(entries);
        applyFrozenWriteCacheAfterFlush();
    }

    /**
     * Returns the number of keys currently buffered in the write cache.
     *
     * @return number of write-cache keys
     */
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

    /**
     * Returns the total number of keys across index and cached entries.
     *
     * @return total key count
     */
    long getNumberOfKeysInCache() {
        return segmentPropertiesManager.getSegmentStats()
                .getNumberOfKeysInSegment()
                + segmentCache.getNumbberOfKeysInCache();
    }

    /**
     * Returns the number of keys currently held in the in-memory segment cache.
     *
     * @return number of cached keys in memory
     */
    int getNumberOfKeysInSegmentCache() {
        return segmentCache.getNumbberOfKeysInCache();
    }

    /**
     * Returns the value for the given key, considering cache and disk.
     *
     * @param key key to look up
     * @return value or null
     */
    V get(final K key) {
        return readPath.get(key);
    }

    /**
     * Returns the segment identifier.
     *
     * @return segment id
     */
    SegmentId getId() {
        return segmentFiles.getId();
    }

    SegmentFiles<K, V> getSegmentFiles() {
        return segmentFiles;
    }

    SegmentPropertiesManager getSegmentPropertiesManager() {
        return segmentPropertiesManager;
    }

    SegmentConf getSegmentConf() {
        return maintenancePath.getSegmentConf();
    }

    SegmentDeltaCacheController<K, V> getDeltaCacheController() {
        return maintenancePath.getDeltaCacheController();
    }

    void switchActiveDirectory(final String directoryName,
            final AsyncDirectory directoryFacade) {
        segmentFiles.switchActiveDirectory(directoryName, directoryFacade);
        segmentPropertiesManager.switchDirectory(directoryFacade);
        readPath.resetSegmentIndexSearcher();
    }

    void switchActiveDirectory(final String directoryName,
            final AsyncDirectory directoryFacade,
            final PropertyStore propertyStore) {
        Vldtn.requireNonNull(propertyStore, "propertyStore");
        segmentFiles.switchActiveDirectory(directoryName, directoryFacade);
        segmentPropertiesManager.switchToStore(propertyStore);
        readPath.resetSegmentIndexSearcher();
    }

    /**
     * Returns the key comparator for this segment.
     *
     * @return key comparator
     */
    Comparator<K> getKeyComparator() {
        return segmentFiles.getKeyTypeDescriptor().getComparator();
    }

    /**
     * Clears cached index searcher resources.
     */
    void resetSegmentIndexSearcher() {
        readPath.resetSegmentIndexSearcher();
    }

    /**
     * Freezes the write cache into a flushable snapshot.
     *
     * @return frozen write cache entries
     */
    List<Entry<K, V>> freezeWriteCacheForFlush() {
        return writePath.freezeWriteCacheForFlush();
    }

    /**
     * Writes the frozen snapshot to delta cache files.
     *
     * @param entries frozen write cache entries
     */
    void flushFrozenWriteCacheToDeltaFile(final List<Entry<K, V>> entries) {
        maintenancePath.flushFrozenWriteCacheToDeltaFile(entries);
    }

    /**
     * Applies the frozen snapshot to the in-memory cache and updates version.
     */
    void applyFrozenWriteCacheAfterFlush() {
        writePath.applyFrozenWriteCacheAfterFlush();
    }

    /**
     * Closes read resources for this segment.
     */
    void close() {
        readPath.close();
        logger.debug("Closing segment '{}'", segmentFiles.getId());
    }
}
