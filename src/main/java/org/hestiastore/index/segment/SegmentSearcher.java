package org.hestiastore.index.segment;

import java.util.List;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Filter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;

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

    private final List<Filter<SegmentSearcherContext<K, V>, SegmentSearcherResult<V>>> steps;

    public SegmentSearcher(final TypeDescriptor<V> valueTypeDescriptor) {
        this.steps = List.of(//
                new SegmentSearcherStepDeltaCache<>(Vldtn.requireNonNull(
                        valueTypeDescriptor, "valueTypeDescriptor")), //
                new SegmentSearcherStepBloomFilter<>(), //
                new SegmentSearcherStepIndexFile<>()//
        );
    }

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

    @Override
    protected void doClose() {
        // intentionally no-op
    }

}
