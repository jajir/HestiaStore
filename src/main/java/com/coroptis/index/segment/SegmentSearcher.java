package com.coroptis.index.segment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.CloseableResource;
import com.coroptis.index.Pair;
import com.coroptis.index.PairReader;
import com.coroptis.index.bloomfilter.BloomFilter;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.scarceindex.ScarceIndex;
import com.coroptis.index.segmentcache.SegmentCache;

/**
 * Object use in memory cache and bloom filter. Only one instance for one
 * segment should be in memory at the time.
 * 
 * This object can be cached in memory.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentSearcher<K, V> implements CloseableResource {

    private final Logger logger = LoggerFactory.getLogger(Segment.class);
    private final SegmentCache<K, V> cache;
    private final ScarceIndex<K> scarceIndex;
    private final int maxNumberOfKeysInIndexPage;
    private final BloomFilter<K> bloomFilter;
    private final SegmentFiles<K, V> segmentFiles;

    public SegmentSearcher(final Directory directory, final SegmentId id,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final int maxNumberOfKeysInIndexPage,
            final int bloomFilterNumberOfHashFunctions,
            final int bloomFilterIndexSizeInBytes) {
        this.segmentFiles = new SegmentFiles<>(directory, id, keyTypeDescriptor,
                valueTypeDescriptor);
        logger.debug("Initializing segment '{}'", segmentFiles.getId());
        this.cache = new SegmentCache<>(keyTypeDescriptor, segmentFiles);
        this.scarceIndex = ScarceIndex.<K>builder().withDirectory(directory)
                .withFileName(segmentFiles.getScarceFileName())
                .withKeyTypeDescriptor(keyTypeDescriptor).build();
        this.maxNumberOfKeysInIndexPage = maxNumberOfKeysInIndexPage;
        this.bloomFilter = BloomFilter.<K>builder()
                .withBloomFilterFileName(segmentFiles.getBloomFilterFileName())
                .withConvertorToBytes(keyTypeDescriptor.getConvertorToBytes())
                .withDirectory(directory)
                .withIndexSizeInBytes(bloomFilterIndexSizeInBytes)
                .withNumberOfHashFunctions(bloomFilterNumberOfHashFunctions)
                .build();
    }

    public K getMaxKey() {
        return scarceIndex.getMaxKey();
    }

    public K getMinKey() {
        return scarceIndex.getMinKey();
    }

    private int getMaxNumberOfKeysInIndexPage() {
        return maxNumberOfKeysInIndexPage;
    }

    public V get(final K key) {
        // look in cache
        final V out = cache.get(key);
        if (segmentFiles.getValueTypeDescriptor().isTombstone(out)) {
            return null;
        }

        // look in bloom filter
        if (out == null) {
            if (bloomFilter.isNotStored(key)) {
                /*
                 * It;s sure that key is not in index.
                 */
                return null;
            }
        }

        // look in index file
        if (out == null) {
            final Integer position = scarceIndex.get(key);
            if (position == null) {
                return null;
            }
            try (final PairReader<K, V> fileReader = segmentFiles
                    .getIndexSstFile().openReader(position)) {
                for (int i = 0; i < getMaxNumberOfKeysInIndexPage(); i++) {
                    final Pair<K, V> pair = fileReader.read();
                    final int cmp = segmentFiles.getKeyTypeDescriptor()
                            .getComparator().compare(pair.getKey(), key);
                    if (cmp == 0) {
                        return pair.getValue();
                    }
                    /**
                     * Keys are in ascending order. When searched key is smaller
                     * than key read from sorted data than key is not found.
                     */
                    if (cmp > 0) {
                        return null;
                    }
                }
            }
        }
        return out;
    }

    @Override
    public void close() {
        bloomFilter.logStats();
        logger.debug("Closing segment '{}'", segmentFiles.getId());
        // Do intentionally nothing.
    }

}
