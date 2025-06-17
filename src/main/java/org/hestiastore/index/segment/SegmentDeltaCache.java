package org.hestiastore.index.segment;

import java.util.List;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.cache.UniqueCache;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.sorteddatafile.SortedDataFile;

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

    public SegmentDeltaCache(final TypeDescriptor<K> keyTypeDescriptor,
            final SegmentFiles<K, V> segmentFiles,
            final SegmentPropertiesManager segmentPropertiesManager) {
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        this.cache = UniqueCache.<K, V>builder()
                .withKeyComparator(keyTypeDescriptor.getComparator())
                .withSstFile(segmentFiles.getCacheSstFile()).build();
        segmentPropertiesManager.getCacheDeltaFileNames()
                .forEach(segmentDeltaFileName -> loadDataFromSegmentDeltaFile(
                        segmentFiles, segmentDeltaFileName));
    }

    private void loadDataFromSegmentDeltaFile(
            final SegmentFiles<K, V> segmentFiles,
            final String segmentDeltaFileName) {
        final SortedDataFile<K, V> dataFile = segmentFiles
                .getCacheSstFile(segmentDeltaFileName);
        try (PairIterator<K, V> iterator = dataFile.openIterator()) {
            while (iterator.hasNext()) {
                cache.put(iterator.next());
            }
        }
    }

    public void put(final Pair<K, V> pair) {
        Vldtn.requireNonNull(pair, "pair");
        cache.put(pair);
    }

    public int size() {
        return cache.size();
    }

    public int sizeWithoutTombstones() {
        final TypeDescriptor<V> valueTypeDescriptor = segmentFiles
                .getValueTypeDescriptor();
        return (int) cache.getStream().filter(
                pair -> !valueTypeDescriptor.isTombstone(pair.getValue()))
                .count();
    }

    public void evictAll() {
        cache.clear();
    }

    public V get(final K key) {
        return cache.get(key);
    }

    public List<K> getSortedKeys() {
        return cache.getSortedKeys();
    }

    public List<Pair<K, V>> getAsSortedList() {
        return cache.getAsSortedList();
    }

}
