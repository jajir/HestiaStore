package org.hestiastore.index.segment;

import java.util.List;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Filter;

/**
 * Uses Bloom filter and index lookups for cache-miss reads.
 * 
 * This object can be cached in memory.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
class SegmentSearcher<K, V> extends AbstractCloseableResource {

    private final List<Filter<SegmentSearcherContext<K, V>, SegmentSearcherResult<V>>> steps;

    /**
     * Creates a searcher pipeline for segment lookups.
     */
    public SegmentSearcher() {
        this.steps = List.of(new SegmentSearcherStepBloomFilter<>(),
                new SegmentSearcherStepIndexFile<>());
    }

    /**
     * Resolves a key by executing the lookup pipeline.
     *
     * @param key lookup key
     * @param segmentDataProvider segment resource provider
     * @param segmentIndexSearcher index searcher for point lookups
     * @return resolved value or null
     */
    public V get(final K key,
            final SegmentResources<K, V> segmentDataProvider,
            final SegmentIndexSearcher<K, V> segmentIndexSearcher) {
        final SegmentSearcherContext<K, V> ctx = SegmentSearcherContext.of(key,
                segmentDataProvider, segmentIndexSearcher);
        final SegmentSearcherResult<V> result = new SegmentSearcherResult<>();
        final SegmentSearcherPipeline<K, V> pipeline = new SegmentSearcherPipeline<>(
                steps);
        pipeline.run(ctx, result);
        return result.getValue();
    }

    /**
     * Releases resources held by the searcher. This implementation is a no-op.
     */
    @Override
    protected void doClose() {
        // intentionally no-op
    }

}
