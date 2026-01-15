package org.hestiastore.index.segment;

import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.cache.UniqueCache;
import org.hestiastore.index.chunkentryfile.ChunkEntryFile;
import org.hestiastore.index.datatype.TypeDescriptor;

/**
 * Represents segment cache containing changes in segment.
 * 
 * In constructor are data loaded from file system.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public final class SegmentDeltaCache<K, V> {

    private final SegmentFiles<K, V> segmentFiles;

    private final UniqueCache<K, V> cache;

    /**
     * Loads delta cache entries from the segment cache files.
     *
     * @param keyTypeDescriptor descriptor for key ordering
     * @param segmentFiles segment file access wrapper
     * @param segmentPropertiesManager properties manager for delta file names
     */
    public SegmentDeltaCache(final TypeDescriptor<K> keyTypeDescriptor,
            final SegmentFiles<K, V> segmentFiles,
            final SegmentPropertiesManager segmentPropertiesManager) {
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        this.cache = UniqueCache.<K, V>builder()
                .withKeyComparator(keyTypeDescriptor.getComparator())
                .withThreadSafe(true)
                .buildEmpty();
        try (EntryIterator<K, V> iterator = segmentFiles.getCacheDataFile()
                .openIterator()) {
            while (iterator.hasNext()) {
                cache.put(iterator.next());
            }
        }
        segmentPropertiesManager.getCacheDeltaFileNames()
                .forEach(segmentDeltaFileName -> loadDataFromSegmentDeltaFile(
                        segmentFiles, segmentDeltaFileName));
    }

    /**
     * Loads entries from a single delta cache file into memory.
     *
     * @param segmentFiles segment file access wrapper
     * @param segmentDeltaFileName delta file name to load
     */
    private void loadDataFromSegmentDeltaFile(
            final SegmentFiles<K, V> segmentFiles,
            final String segmentDeltaFileName) {
        final ChunkEntryFile<K, V> dataFile = segmentFiles
                .getDeltaCacheChunkEntryFile(segmentDeltaFileName);
        try (EntryIterator<K, V> iterator = dataFile.openIterator()) {
            while (iterator.hasNext()) {
                cache.put(iterator.next());
            }
        }
    }

    /**
     * Adds or replaces an entry in the delta cache.
     *
     * @param entry entry to store
     */
    public void put(final Entry<K, V> entry) {
        Vldtn.requireNonNull(entry, "entry");
        cache.put(entry);
    }

    /**
     * Returns the total number of cached entries.
     *
     * @return number of entries in cache
     */
    public int size() {
        return cache.size();
    }

    /**
     * Returns the number of cached entries excluding tombstones.
     *
     * @return number of non-tombstone entries
     */
    public int sizeWithoutTombstones() {
        final TypeDescriptor<V> valueTypeDescriptor = segmentFiles
                .getValueTypeDescriptor();
        int count = 0;
        for (final Entry<K, V> entry : cache.getAsSortedList()) {
            if (!valueTypeDescriptor.isTombstone(entry.getValue())) {
                count++;
            }
        }
        return count;
    }

    /**
     * Clears all cached entries.
     */
    public void evictAll() {
        cache.clear();
    }

    /**
     * Returns the cached value for the given key.
     *
     * @param key key to look up
     * @return cached value or null if absent
     */
    public V get(final K key) {
        return cache.get(key);
    }

    /**
     * Returns cached entries as a sorted list.
     *
     * @return sorted cache entries
     */
    public List<Entry<K, V>> getAsSortedList() {
        return cache.getAsSortedList();
    }

}
