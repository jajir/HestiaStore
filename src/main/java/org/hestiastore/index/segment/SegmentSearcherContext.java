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

    /**
     * Creates a search context for a single key lookup.
     *
     * @param key lookup key
     * @param segmentDataProvider segment resource provider
     * @param segmentIndexSearcher index searcher for point lookups
     */
    private SegmentSearcherContext(final K key,
            final SegmentResources<K, V> segmentDataProvider,
            final SegmentIndexSearcher<K, V> segmentIndexSearcher) {
        this.key = key;
        this.segmentDataProvider = segmentDataProvider;
        this.segmentIndexSearcher = segmentIndexSearcher;
    }

    /**
     * Builds a validated search context instance.
     *
     * @param key lookup key
     * @param segmentDataProvider segment resource provider
     * @param segmentIndexSearcher index searcher for point lookups
     * @param <K> key type
     * @param <V> value type
     * @return new search context
     */
    static <K, V> SegmentSearcherContext<K, V> of(final K key,
            final SegmentResources<K, V> segmentDataProvider,
            final SegmentIndexSearcher<K, V> segmentIndexSearcher) {
        return new SegmentSearcherContext<>(Vldtn.requireNonNull(key, "key"),
                Vldtn.requireNonNull(segmentDataProvider,
                        "segmentDataProvider"),
                Vldtn.requireNonNull(segmentIndexSearcher,
                        "segmentIndexSearcher"));
    }

    /**
     * Returns the lookup key.
     *
     * @return key
     */
    K getKey() {
        return key;
    }

    /**
     * Returns the Bloom filter for the segment.
     *
     * @return Bloom filter
     */
    BloomFilter<K> getBloomFilter() {
        return Vldtn.requireNonNull(segmentDataProvider.getBloomFilter(),
                "bloomFilter");
    }

    /**
     * Returns the scarce index for the segment.
     *
     * @return scarce index
     */
    ScarceSegmentIndex<K> getScarceIndex() {
        return Vldtn.requireNonNull(segmentDataProvider.getScarceIndex(),
                "scarceIndex");
    }

    /**
     * Looks up the position in the scarce index for the current key.
     *
     * @return position or null when absent
     */
    Integer getPositionFromScarceIndex() {
        return getScarceIndex().get(key);
    }

    /**
     * Returns the index searcher for point lookups.
     *
     * @return index searcher
     */
    SegmentIndexSearcher<K, V> getSegmentIndexSearcher() {
        return segmentIndexSearcher;
    }

    /**
     * Searches the index at the given position for the current key.
     *
     * @param position starting byte position
     * @return value if found, otherwise null
     */
    V searchInIndex(final int position) {
        return getSegmentIndexSearcher().search(key, position);
    }

    /**
     * Increments the Bloom filter false positive counter.
     */
    void incrementFalsePositive() {
        getBloomFilter().incrementFalsePositive();
    }

    /**
     * Returns true when the Bloom filter indicates the key is absent.
     *
     * @return true when key is not stored
     */
    boolean isNotStoredInBloomFilter() {
        return getBloomFilter().isNotStored(key);
    }
}
