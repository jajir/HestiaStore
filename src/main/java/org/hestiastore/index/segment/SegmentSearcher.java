package org.hestiastore.index.segment;

import org.hestiastore.index.AbstractCloseableResource;
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
public class SegmentSearcher<K, V> extends AbstractCloseableResource {

    private final TypeDescriptor<V> valueTypeDescriptor;

    public SegmentSearcher(final TypeDescriptor<V> valueTypeDescriptor) {
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
    }

    public V get(final K key, final SegmentDeltaCache<K, V> deltaCache,
            final BloomFilter<K> bloomFilter,
            final ScarceIndex<K> scarceIndex,
            final SegmentIndexSearcher<K, V> segmentIndexSearcher) {
        Vldtn.requireNonNull(deltaCache, "deltaCache");
        Vldtn.requireNonNull(bloomFilter, "bloomFilter");
        Vldtn.requireNonNull(scarceIndex, "scarceIndex");
        Vldtn.requireNonNull(segmentIndexSearcher, "segmentIndexSearcher");
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
    protected void doClose() {
        // intentionally no-op
    }

}
