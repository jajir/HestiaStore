package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.List;

import org.hestiastore.index.Filter;
import org.junit.jupiter.api.Test;

class SegmentSearcherPipelineTest {

    @Test
    void run_executesFiltersInOrder() {
        @SuppressWarnings("unchecked")
        final SegmentResources<Integer, String> resources = (SegmentResources<Integer, String>) mock(
                SegmentResources.class);
        @SuppressWarnings("unchecked")
        final SegmentIndexSearcher<Integer, String> searcher = (SegmentIndexSearcher<Integer, String>) mock(
                SegmentIndexSearcher.class);
        final SegmentSearcherContext<Integer, String> ctx = SegmentSearcherContext
                .of(1, resources, searcher);
        final SegmentSearcherResult<String> result = new SegmentSearcherResult<>();

        final Filter<SegmentSearcherContext<Integer, String>, SegmentSearcherResult<String>> first = (
                context, res) -> {
                    res.setValue("first");
                    return true;
                };
        final Filter<SegmentSearcherContext<Integer, String>, SegmentSearcherResult<String>> second = (
                context, res) -> {
                    res.setValue("second");
                    return false;
                };

        final SegmentSearcherPipeline<Integer, String> pipeline = new SegmentSearcherPipeline<>(
                List.of(first, second));
        pipeline.run(ctx, result);

        assertEquals("second", result.getValue());
    }
}
