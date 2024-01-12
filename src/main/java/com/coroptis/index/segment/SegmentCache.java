package com.coroptis.index.segment;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.cache.UniqueCache;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.sstfile.SstFileStreamer;
import com.coroptis.index.sstfile.SstFileWriter;

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
public class SegmentCache<K, V> {

    private final UniqueCache<K, V> cache;

    private final SegmentFiles<K, V> segmentFiles;

    public SegmentCache(final TypeDescriptor<K> keyTypeDescriptor,
            final SegmentFiles<K, V> segmentFiles,
            final SegmentPropertiesManager segmentPropertiesManager) {
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        this.cache = UniqueCache.<K, V>builder()
                .withKeyComparator(keyTypeDescriptor.getComparator())
                .withSstFile(segmentFiles.getCacheSstFile()).build();
        segmentPropertiesManager.getCacheDeltaFileNames()
                .forEach(segmentDeltaFileName -> {
                    try (final SstFileStreamer<K, V> fileStreamer = segmentFiles
                            .getCacheSstFile(segmentDeltaFileName)
                            .openStreamer()) {
                        fileStreamer.stream().forEach(pair -> cache.put(pair));
                    }
                });
    }

    public void put(final Pair<K, V> pair) {
        Objects.requireNonNull(pair);
        cache.put(pair);
    }

    public int size() {
        return cache.size();
    }

    public void clear() {
        cache.clear();
    }

    public V get(final K key) {
        return cache.get(key);
    }

    public PairIterator<K, V> getSortedIterator() {
        return cache.getSortedIterator();
    }

    /**
     * Store cache content to file system.
     * 
     * @return number of stored keys
     */
    public int flushCache() {
        final AtomicLong cx = new AtomicLong(0L);
        try (final SstFileWriter<K, V> writer = segmentFiles.getCacheSstFile()
                .openWriter()) {
            cache.getStream().forEach(pair -> {
                writer.put(pair);
                cx.incrementAndGet();
            });
        }
        return cx.intValue();
    }

}
