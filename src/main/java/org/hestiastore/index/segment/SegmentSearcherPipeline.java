package org.hestiastore.index.segment;

import java.util.List;

import org.hestiastore.index.AbstractChainOfFilters;
import org.hestiastore.index.Filter;

/**
 * Pipeline of search steps for a segment lookup.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentSearcherPipeline<K, V> extends
        AbstractChainOfFilters<SegmentSearcherContext<K, V>, SegmentSearcherResult<V>> {

    /**
     * Creates a pipeline with the given ordered steps.
     *
     * @param steps filter steps to execute in order
     */
    SegmentSearcherPipeline(
            final List<Filter<SegmentSearcherContext<K, V>, SegmentSearcherResult<V>>> steps) {
        super(steps);
    }

    /**
     * Executes the pipeline against the given context and result container.
     *
     * @param ctx search context
     * @param result result holder to populate
     */
    void run(final SegmentSearcherContext<K, V> ctx,
            final SegmentSearcherResult<V> result) {
        filter(ctx, result);
    }
}
