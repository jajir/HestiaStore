package org.hestiastore.index.segment;

import java.util.List;

import org.hestiastore.index.AbstractChainOfFilters;
import org.hestiastore.index.Filter;

final class SegmentSearcherPipeline<K, V> extends
        AbstractChainOfFilters<SegmentSearcherContext<K, V>, SegmentSearcherResult<V>> {

    SegmentSearcherPipeline(
            final List<Filter<SegmentSearcherContext<K, V>, SegmentSearcherResult<V>>> steps) {
        super(steps);
    }

    void run(final SegmentSearcherContext<K, V> ctx,
            final SegmentSearcherResult<V> result) {
        filter(ctx, result);
    }
}
