package org.hestiastore.index.segment;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.OperationResult;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
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
        logger.debug("Opening segment '{}'", segmentFiles.getId());
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
     * Opens a read iterator over a half-open key range.
     *
     * @param fromInclusive required inclusive lower key bound
     * @param toExclusive optional exclusive upper key bound
     * @param isolation iterator isolation mode
     * @return bounded entry iterator
     */
    EntryIterator<K, V> openIterator(final K fromInclusive,
            final K toExclusive,
            final SegmentIteratorIsolation isolation) {
        return readPath.openIterator(fromInclusive, toExclusive, isolation);
    }

    /**
     * Opens an iterator over the index and stable compaction snapshot.
     *
     * @return iterator over the merged snapshot view
     */
    EntryIterator<K, V> openIteratorFromCompactionSnapshot() {
        return new MergeDeltaCacheWithIndexIterator<>(
                segmentFiles.getIndexFile().openIterator(),
                segmentFiles.getKeyTypeDescriptor(),
                segmentFiles.getValueTypeDescriptor(),
                segmentCache.compactionSnapshotIterator());
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
    void executeFullWriteTx(
            final Consumer<EntryWriter<K, V>> writeFunction) {
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
     * Inserts a value only when the key is logically absent.
     *
     * @param key key to write
     * @param value value to write
     * @return conditional mutation result
     */
    OperationResult<Boolean> tryPutIfAbsentWithoutWaiting(final K key,
            final V value) {
        final K nonNullKey = Vldtn.requireNonNull(key, "key");
        final V nonNullValue = requireConditionalValue(value, "value");
        return tryConditionalPutWithoutWaiting(nonNullKey, null, nonNullValue);
    }

    /**
     * Replaces a value only when the current logical value matches the expected
     * value according to the configured value comparator.
     *
     * @param key key to replace
     * @param expectedValue expected current value
     * @param newValue replacement value
     * @return conditional mutation result
     */
    OperationResult<Boolean> tryReplaceWithoutWaiting(final K key,
            final V expectedValue, final V newValue) {
        final K nonNullKey = Vldtn.requireNonNull(key, "key");
        final V nonNullExpected = requireConditionalValue(expectedValue,
                "expectedValue");
        final V nonNullValue = requireConditionalValue(newValue, "newValue");
        return tryConditionalPutWithoutWaiting(nonNullKey, nonNullExpected,
                nonNullValue);
    }

    private OperationResult<Boolean> tryConditionalPutWithoutWaiting(
            final K key, final V expectedValue, final V newValue) {
        final TypeDescriptor<V> valueDescriptor = segmentFiles
                .getValueTypeDescriptor();
        while (true) {
            final V observedValue = segmentCache.getFromWriteCache(key);
            if (observedValue != null) {
                final V logicalValue = valueDescriptor
                        .isTombstone(observedValue) ? null : observedValue;
                if (!matchesExpectedValue(valueDescriptor, logicalValue,
                        expectedValue)) {
                    return OperationResult.ok(false);
                }
                if (segmentCache.replaceInWriteCache(key, observedValue,
                        newValue)) {
                    return OperationResult.ok(true);
                }
                continue;
            }

            final V logicalValue = readPath.get(key);
            if (!matchesExpectedValue(valueDescriptor, logicalValue,
                    expectedValue)) {
                return OperationResult.ok(false);
            }
            if (segmentCache.tryPutIfAbsentToWriteCacheWithoutWaiting(
                    Entry.of(key, newValue))) {
                return OperationResult.ok(true);
            }
            if (segmentCache.getFromWriteCache(key) == null) {
                return OperationResult.writeCacheFull();
            }
        }
    }

    private boolean matchesExpectedValue(
            final TypeDescriptor<V> valueDescriptor, final V currentValue,
            final V expectedValue) {
        if (expectedValue == null) {
            return currentValue == null;
        }
        return currentValue != null && valueDescriptor.getComparator()
                .compare(currentValue, expectedValue) == 0;
    }

    private V requireConditionalValue(final V value,
            final String propertyName) {
        final V nonNullValue = Vldtn.requireNonNull(value, propertyName);
        if (segmentFiles.getValueTypeDescriptor()
                .isTombstone(nonNullValue)) {
            throw new IllegalArgumentException(String.format(
                    "Property '%s' must not be a tombstone.", propertyName));
        }
        return nonNullValue;
    }

    /**
     * Flushes the write cache into the delta cache if there are entries.
     */
    void flush() {
        freezeWriteCacheForFlush();
        if (!segmentCache.hasFrozenWriteCache()) {
            return;
        }
        flushFrozenWriteCacheToDeltaFile();
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
     * Returns the number of delta cache files recorded for this segment.
     *
     * @return number of delta cache files tracked in properties
     */
    int getDeltaCacheFileCount() {
        return segmentPropertiesManager.getDeltaFileCount();
    }

    long getBloomFilterRequestCount() {
        return readPath.getBloomFilterRequestCount();
    }

    long getBloomFilterRefusedCount() {
        return readPath.getBloomFilterRefusedCount();
    }

    long getBloomFilterPositiveCount() {
        return readPath.getBloomFilterPositiveCount();
    }

    long getBloomFilterFalsePositiveCount() {
        return readPath.getBloomFilterFalsePositiveCount();
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

    void applyRuntimeLimits(final SegmentRuntimeLimits limits) {
        Vldtn.requireNonNull(limits, "limits");
        segmentCache.updateWriteCacheLimits(
                limits.maxNumberOfKeysInSegmentWriteCache(),
                limits.maxNumberOfKeysInSegmentWriteCacheDuringMaintenance());
    }

    SegmentDeltaCacheController<K, V> getDeltaCacheController() {
        return maintenancePath.getDeltaCacheController();
    }

    void switchActiveVersion(final long version) {
        segmentFiles.switchActiveVersion(version);
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
     */
    void freezeWriteCacheForFlush() {
        writePath.freezeWriteCacheForFlush();
    }

    /**
     * Writes the frozen snapshot to delta cache files.
     */
    void flushFrozenWriteCacheToDeltaFile() {
        final Iterator<Entry<K, V>> entries = segmentCache
                .frozenWriteCacheIterator();
        maintenancePath.flushFrozenWriteCacheToDeltaFile(entries);
    }

    /**
     * Applies the frozen snapshot to the in-memory cache and updates version.
     */
    void applyFrozenWriteCacheAfterFlush() {
        writePath.applyFrozenWriteCacheAfterFlush();
    }

    /**
     * Clears persisted in-memory caches and closes read resources.
     */
    void close() {
        try {
            segmentCache.evictAll();
        } finally {
            closeReadResources();
        }
    }

    /**
     * Closes read resources without clearing write caches.
     */
    void closeReadResources() {
        readPath.close();
        logger.debug("Closing segment '{}'", segmentFiles.getId());
    }
}
