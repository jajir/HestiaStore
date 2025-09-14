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
    private final SegmentDataProvider<K, V> segmentCacheDataProvider;

    public SegmentSearcher(final TypeDescriptor<V> valueTypeDescriptor,
            final SegmentIndexSearcher<K, V> segmentIndexSearcher,
            final SegmentDataProvider<K, V> segmentDataProvider) {
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        this.segmentCacheDataProvider = Vldtn
                .requireNonNull(segmentDataProvider, "segmentDataProvider");
        this.segmentIndexSearcher = Vldtn.requireNonNull(segmentIndexSearcher,
                "segmentIndexSearcher");
    }

    private SegmentDeltaCache<K, V> getDeltaCache() {
        return segmentCacheDataProvider.getSegmentDeltaCache();
    }

    private ScarceIndex<K> getScarceIndex() {
        return segmentCacheDataProvider.getScarceIndex();
    }

    private BloomFilter<K> getBloomFilter() {
        return segmentCacheDataProvider.getBloomFilter();
    }

    public V get(final K key) {
        // look in cache
        final V out = getDeltaCache().get(key);
        if (valueTypeDescriptor.isTombstone(out)) {
            return null;
        }

        // TODO optimize ifs with junits

        // look in bloom filter
        if (out == null && getBloomFilter().isNotStored(key)) {
            /*
             * It's sure that key is not in index.
             */
            return null;
        }

        // look in index file
        if (out == null) {
            final Integer position = getScarceIndex().get(key);
            if (position == null) {
                return null;
            }
            final V value = segmentIndexSearcher.search(key, position);
            if (value == null) {
                getBloomFilter().incrementFalsePositive();
                return null;
            }
            return value;
        }
        return out;
    }

    @Override
    public void close() {
        segmentIndexSearcher.close();
    }

}
