package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.bloomfilter.BloomFilter;
import org.hestiastore.index.scarceindex.ScarceSegmentIndex;

/**
 * Immutable inputs for a single segment search operation.
 */
final class SegmentSearcherContext<K, V> {
    private final K key;
    private final SegmentResources<K, V> segmentDataProvider;
    private final SegmentIndexSearcher<K, V> segmentIndexSearcher;

    private SegmentSearcherContext(final K key,
            final SegmentResources<K, V> segmentDataProvider,
            final SegmentIndexSearcher<K, V> segmentIndexSearcher) {
        this.key = key;
        this.segmentDataProvider = segmentDataProvider;
        this.segmentIndexSearcher = segmentIndexSearcher;
    }

    static <K, V> SegmentSearcherContext<K, V> of(final K key,
            final SegmentResources<K, V> segmentDataProvider,
            final SegmentIndexSearcher<K, V> segmentIndexSearcher) {
        return new SegmentSearcherContext<>(Vldtn.requireNonNull(key, "key"),
                Vldtn.requireNonNull(segmentDataProvider,
                        "segmentDataProvider"),
                Vldtn.requireNonNull(segmentIndexSearcher,
                        "segmentIndexSearcher"));
    }

    K getKey() {
        return key;
    }

    SegmentDeltaCache<K, V> getDeltaCache() {
        return Vldtn.requireNonNull(segmentDataProvider.getSegmentDeltaCache(),
                "segmentDeltaCache");
    }

    BloomFilter<K> getBloomFilter() {
        return Vldtn.requireNonNull(segmentDataProvider.getBloomFilter(),
                "bloomFilter");
    }

    ScarceSegmentIndex<K> getScarceIndex() {
        return Vldtn.requireNonNull(segmentDataProvider.getScarceIndex(),
                "scarceIndex");
    }

    Integer getPositionFromScarceIndex() {
        return getScarceIndex().get(key);
    }

    SegmentIndexSearcher<K, V> getSegmentIndexSearcher() {
        return segmentIndexSearcher;
    }

    V searchInIndex(final int position) {
        return getSegmentIndexSearcher().search(key, position);
    }

    void incrementFalsePositive() {
        getBloomFilter().incrementFalsePositive();
    }

    boolean isNotStoredInBloomFilter() {
        return getBloomFilter().isNotStored(key);
    }
}
