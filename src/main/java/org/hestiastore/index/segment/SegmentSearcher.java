package org.hestiastore.index.segment;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bloomfilter.BloomFilter;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.scarceindex.ScarceIndex;

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

    private final TypeDescriptor<V> valueTypeDescriptor;
    private final SegmentIndexSearcher<K, V> segmentIndexSearcher;

    public SegmentSearcher(final TypeDescriptor<V> valueTypeDescriptor,
            final SegmentIndexSearcher<K, V> segmentIndexSearcher) {
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.segmentIndexSearcher = Vldtn.requireNonNull(segmentIndexSearcher,
                "segmentIndexSearcher");
    }

    public V get(final K key, final SegmentDeltaCache<K, V> deltaCache,
            final BloomFilter<K> bloomFilter,
            final ScarceIndex<K> scarceIndex) {
        Vldtn.requireNonNull(deltaCache, "deltaCache");
        Vldtn.requireNonNull(bloomFilter, "bloomFilter");
        Vldtn.requireNonNull(scarceIndex, "scarceIndex");
        // look in cache
        final V out = deltaCache.get(key);
        if (valueTypeDescriptor.isTombstone(out)) {
            return null;
        }

        if (out != null) {
            return out;
        }

        // look in bloom filter
        if (bloomFilter.isNotStored(key)) {
            /*
             * It's sure that key is not in index.
             */
            return null;
        }

        // look in index file
        final Integer position = scarceIndex.get(key);
        if (position == null) {
            return null;
        }
        final V value = segmentIndexSearcher.search(key, position);
        if (value == null) {
            bloomFilter.incrementFalsePositive();
            return null;
        }
        return value;
    }

    @Override
    public void close() {
        segmentIndexSearcher.close();
    }

}
